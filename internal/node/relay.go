package node

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
	"github.com/sxfstudy-colorful/accel-proxy/internal/tunnel"
)

// ─────────────────────────────────────────────────────────────────────────────
// RelayNode
//
// 职责：接受入向隧道，按 TargetIDC 查路由表，转发到下一跳。
//
// 热重载：
//   Routes（next_hops）可在不中断存量连接的情况下更新。
//   实现方式：routeTable 字段通过 atomic.Pointer 持有，
//   Reload() 构造新表后原子替换，handleTunnel 每次取引用时拿到的
//   始终是完整的旧表或完整的新表，不存在中间状态。
// ─────────────────────────────────────────────────────────────────────────────

// routeTable maps serviceID → IDC → []nextHopURL.
// Treated as immutable once stored; hot-reload swaps the whole pointer.
type routeTable map[string]map[string][]config.HopAddr

// RelayNode forwards tunnels toward the target IDC.
type RelayNode struct {
	baseNode
	cfg       *config.Config
	frameSize int64

	// routes is accessed via atomic pointer for lock-free hot-reload.
	// Writers (Reload) swap the whole table; readers (handleTunnel) take
	// a snapshot at the start of each connection — they see either the
	// complete old table or the complete new table, never a partial update.
	routes atomic.Pointer[routeTable]

	// reloadMu serialises concurrent Reload() calls.
	// The atomic.Pointer alone guarantees readers always see a consistent
	// table, but the read-modify-write in Reload needs a mutex to prevent
	// two concurrent reloads from overwriting each other's changes.
	reloadMu sync.Mutex

	// dialers caches per-(serviceID, IDC) Dialer instances so Round-Robin
	// counters persist across connections. Keyed by dialerKey.
	// Must be a field (not a package-level var) so multiple RelayNode
	// instances in the same process don't share counters.
	dialersMu sync.RWMutex
	dialers   map[dialerKey]*tunnel.Dialer
}

var _ Node = (*RelayNode)(nil)
var _ CertReloader = (*RelayNode)(nil)

// NewRelayNode constructs a RelayNode.
func NewRelayNode(cfg *config.Config, logger *slog.Logger) (*RelayNode, error) {
	n := &RelayNode{
		baseNode:  baseNode{logger: logger},
		cfg:       cfg,
		frameSize: cfg.Tunnel.MaxFrameSize,
		dialers:   make(map[dialerKey]*tunnel.Dialer),
	}

	tbl, err := buildRouteTable(cfg)
	if err != nil {
		return nil, err
	}
	n.routes.Store(&tbl)

	n.server = tunnel.NewServer(&cfg.Tunnel, cfg.Node.ID, n.handleTunnel, logger)
	return n, nil
}

// Start begins accepting inbound tunnel connections (non-blocking).
func (n *RelayNode) Start() error {
	n.logger.Info("relay node starting",
		"id", n.cfg.Node.ID,
		"addr", n.cfg.Tunnel.ListenAddr,
	)
	return n.startServer("relay:" + n.cfg.Node.ID)
}

// Stop shuts down the relay node gracefully.
func (n *RelayNode) Stop() { n.stopServer() }

// ReloadCert triggers an immediate TLS certificate reload.
func (n *RelayNode) ReloadCert() error { return n.reloadCert() }

// Reload applies updated route configuration without dropping connections.
// Existing in-flight sessions continue using the old route table until they
// finish; new connections pick up the updated table immediately.
func (n *RelayNode) Reload(rc ReloadableConfig) error {
	if len(rc.Routes) == 0 {
		return nil
	}

	// Serialise concurrent reloads. The atomic.Pointer guarantees readers
	// always see a consistent table, but two concurrent Reload() calls doing
	// read-modify-write would overwrite each other's changes without this mutex.
	n.reloadMu.Lock()
	defer n.reloadMu.Unlock()

	current := *n.routes.Load()
	next := make(routeTable, len(current))
	for svcID, idcMap := range current {
		next[svcID] = idcMap
	}
	for svcID, idcMap := range rc.Routes {
		next[svcID] = idcMap
	}
	n.routes.Store(&next)

	n.logger.Info("relay: routes hot-reloaded", "updated_services", len(rc.Routes))
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-(service, IDC) Dialer cache
//
// Preserves Round-Robin counters across connections so load balancing works
// correctly. Stored as a RelayNode field (not a package-level var) so that
// multiple RelayNode instances in the same process don't share state.
//
// When Reload() changes next-hop URLs, dialerFor detects the content change
// and replaces the Dialer, resetting the counter for that bucket only.
// ─────────────────────────────────────────────────────────────────────────────

type dialerKey struct{ serviceID, idc string }

// dialerFor returns a stable Dialer for the (serviceID, idc) pair.
//
// If the next-hop URLs are unchanged from the existing Dialer's pool, the
// existing Dialer is reused — preserving the selector's Round-Robin counter
// and each endpoint's alive flag + latency history.
//
// If the addresses have changed (after a Reload()), UpdateEndpoints is called
// existing pool: unchanged endpoints keep their health/latency state; new
// endpoints start fresh; removed endpoints are discarded.
// A new Dialer wrapping the updated pool replaces the old one in the cache.
func (n *RelayNode) dialerFor(serviceID, idc string, nextHops []config.HopAddr) *tunnel.Dialer {
	key := dialerKey{serviceID, idc}

	n.dialersMu.RLock()
	d, ok := n.dialers[key]
	n.dialersMu.RUnlock()
	if ok && config.EqualHopAddrs(d.Pool().Addrs(), nextHops) {
		return d // URLs unchanged: reuse pool (counter + health intact)
	}

	n.dialersMu.Lock()
	defer n.dialersMu.Unlock()
	// Double-check under write lock.
	if d, ok = n.dialers[key]; ok {
		if config.EqualHopAddrs(d.Pool().Addrs(), nextHops) {
			return d
		}
		// URLs changed: hot-update the existing pool in-place so endpoints
		// that survive the change keep their alive flag and latency history.
		d.Pool().UpdateEndpoints(nextHops)
		return d
	}
	// No existing entry: create a new pool + dialer.
	pool := tunnel.NewHopPool(nextHops, nil, n.logger)
	d = tunnel.NewDialer(pool, n.frameSize, n.logger)
	n.dialers[key] = d
	return d
}

// handleTunnel is called for each accepted inbound tunnel.
func (n *RelayNode) handleTunnel(req *tunnel.HandshakeRequest, inbound *tunnel.Transport) {
	defer inbound.Close()

	nextHops := *n.routes.Load()
	nextServiceHop, ok := nextHops[req.ServiceID]
	if !ok {
		n.logger.Error("relay: unknown service", "service_id", req.ServiceID)
		return
	}

	hopAddress, ok := nextServiceHop[req.TargetIDC]
	if !ok {
		n.logger.Error("relay: unknown target", "target", req.TargetIDC)
		return
	}

	// Forward request with incremented hop count.
	forwardReq := *req
	forwardReq.HopCount++

	hopPool := tunnel.NewHopPool(hopAddress, &tunnel.LatencySelector{}, n.logger)

	dialer := tunnel.NewDialer(hopPool, n.cfg.Tunnel.MaxFrameSize, n.logger)
	outbound, err := dialer.DialWithFallback(&forwardReq)
	if err != nil {
		n.logger.Error("relay: next-hop dial failed",
			"service", req.ServiceID,
			"err", err,
		)
		return
	}

	n.logger.Info("relay: bridging tunnels",
		"service", req.ServiceID,
		"client_ip", req.ClientIP,
		"hop", forwardReq.HopCount,
	)

	// Bridge the two transports.
	tunnel.RelayTransports(inbound, outbound, n.logger)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func buildRouteTable(cfg *config.Config) (routeTable, error) {
	tbl := make(routeTable, len(cfg.Services))
	for i := range cfg.Services {
		svc := &cfg.Services[i]
		if len(svc.Routes) == 0 {
			return nil, fmt.Errorf("relay service %q has no routes", svc.ID)
		}
		tbl[svc.ID] = svc.Routes
	}
	return tbl, nil
}
