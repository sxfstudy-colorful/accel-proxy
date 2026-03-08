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
// 路由层级：Service → IDCRoute → RouteGroup → HopAddr
//   routeTable[serviceID] → []config.IDCRoute
//
// 热重载：
//   routeTable 通过 atomic.Pointer 持有，Reload() 原子替换。
//   handleTunnel 在连接建立时快照一次，整条连接生命周期使用同一张表。
// ─────────────────────────────────────────────────────────────────────────────

// routeTable maps serviceID → []IDCRoute (immutable once stored).
type routeTable map[string][]config.IDCRoute

// RelayNode forwards tunnels toward the target IDC.
type RelayNode struct {
	baseNode
	cfg       *config.Config
	frameSize int64

	routes   atomic.Pointer[routeTable]
	reloadMu sync.Mutex

	dialersMu sync.RWMutex
	dialers   map[dialerKey]*tunnel.Dialer
}

type dialerKey struct{ serviceID, idc string }

var _ Node         = (*RelayNode)(nil)
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

// Reload applies updated routing configuration without dropping connections.
// In-flight sessions keep their snapshot; new connections pick up the new table.
func (n *RelayNode) Reload(rc ReloadableConfig) error {
	if len(rc.Routes) == 0 {
		return nil
	}

	n.reloadMu.Lock()
	defer n.reloadMu.Unlock()

	current := *n.routes.Load()
	next := make(routeTable, len(current))
	for svcID, routes := range current {
		next[svcID] = routes
	}
	for svcID, routes := range rc.Routes {
		next[svcID] = routes
	}
	n.routes.Store(&next)

	n.logger.Info("relay: routes hot-reloaded", "updated_services", len(rc.Routes))
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// handleTunnel
// ─────────────────────────────────────────────────────────────────────────────

// handleTunnel is called by tunnel.Server for each accepted inbound connection.
func (n *RelayNode) handleTunnel(req *tunnel.HandshakeRequest, inbound *tunnel.Transport) {
	session := tunnel.NewTunnelSession(req, inbound, n.logger)
	defer session.Close()

	// ── 1. Route lookup ───────────────────────────────────────────────────
	// Snapshot the table once; this connection is immune to concurrent Reload().
	routes := *n.routes.Load()

	idcRoutes, ok := routes[req.ServiceID]
	if !ok {
		session.Logger().Warn("relay: unknown service")
		return
	}
	nextHops := hopsForIDC(idcRoutes, req.TargetIDC)
	if len(nextHops) == 0 {
		session.Logger().Warn("relay: no route for target IDC")
		return
	}

	// ── 2. Hop-count guard (loop detection) ───────────────────────────────
	forwardReq := *req
	forwardReq.HopCount++
	if forwardReq.HopCount > tunnel.MaxHopCount {
		session.Logger().Error("relay: hop count exceeded, dropping",
			"max", tunnel.MaxHopCount)
		return
	}

	// ── 3. Dial next hop ──────────────────────────────────────────────────
	dialer := n.dialerFor(req.ServiceID, req.TargetIDC, nextHops)
	outbound, err := dialer.DialWithFallback(&forwardReq)
	if err != nil {
		session.Logger().Error("relay: next-hop dial failed", "err", err)
		return
	}

	// ── 4. Bridge tunnels (blocks until both sides close) ─────────────────
	session.RunRelay(outbound)
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-(service, IDC) Dialer cache
// ─────────────────────────────────────────────────────────────────────────────

// dialerFor returns a stable Dialer for the (serviceID, idc) pair.
// If the hop list is unchanged, the existing Dialer (with its Round-Robin
// counter and endpoint health/latency) is reused. If changed, UpdateEndpoints
// is called in-place so surviving endpoints keep their state.
func (n *RelayNode) dialerFor(serviceID, idc string, nextHops []config.HopAddr) *tunnel.Dialer {
	key := dialerKey{serviceID, idc}

	n.dialersMu.RLock()
	d, ok := n.dialers[key]
	n.dialersMu.RUnlock()
	if ok && config.EqualHopAddrs(d.Pool().Addrs(), nextHops) {
		return d
	}

	n.dialersMu.Lock()
	defer n.dialersMu.Unlock()
	if d, ok = n.dialers[key]; ok {
		if config.EqualHopAddrs(d.Pool().Addrs(), nextHops) {
			return d
		}
		d.Pool().UpdateEndpoints(nextHops)
		return d
	}
	pool := tunnel.NewHopPool(nextHops, nil, n.logger)
	d = tunnel.NewDialer(pool, n.frameSize, n.logger)
	n.dialers[key] = d
	return d
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

// hopsForIDC returns priority-sorted hops for the given IDC from an IDCRoute slice.
func hopsForIDC(routes []config.IDCRoute, idc string) []config.HopAddr {
	for i := range routes {
		if routes[i].IDC == idc {
			return routes[i].SortedHops()
		}
	}
	return nil
}
