package node

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
	"github.com/sxfstudy-colorful/accel-proxy/internal/tunnel"
)

type routeTable map[string][]config.IDCRoute

type RelayNode struct {
	baseNode
	nodeID string
	psk    string

	routes   atomic.Pointer[routeTable]
	reloadMu sync.Mutex

	dialersMu sync.RWMutex
	dialers   map[dialerKey]*tunnel.Dialer
	frameSize int64

	// ctx/cancel control the lifecycle of background goroutines (probers).
	// cancel is called in Stop() to cleanly shut down all probers.
	ctx    context.Context
	cancel context.CancelFunc
}

type dialerKey struct{ serviceID, idc string }

var _ Node         = (*RelayNode)(nil)
var _ CertReloader = (*RelayNode)(nil)

func NewRelayNode(nodeID string, cfg *config.RelayConfig, logger *slog.Logger) (*RelayNode, error) {
	ctx, cancel := context.WithCancel(context.Background())
	n := &RelayNode{
		baseNode:  baseNode{logger: logger},
		nodeID:    nodeID,
		psk:       cfg.Tunnel.PSK,
		frameSize: cfg.Tunnel.MaxFrameSize,
		dialers:   make(map[dialerKey]*tunnel.Dialer),
		ctx:       ctx,
		cancel:    cancel,
	}

	tbl, err := buildRouteTable(cfg.Services)
	if err != nil {
		return nil, err
	}
	n.routes.Store(&tbl)

	n.server = tunnel.NewServer(&cfg.Tunnel, nodeID, n.handleTunnel, logger)
	return n, nil
}

func (n *RelayNode) Start() error {
	n.logger.Info("relay node starting", "id", n.nodeID,
		"addr", n.server.ListenAddr())
	return n.startServer("relay:" + n.nodeID)
}

func (n *RelayNode) Stop() {
	n.cancel() // stop all prober goroutines
	n.stopServer()
}

func (n *RelayNode) ReloadCert() error { return n.reloadCert() }

func (n *RelayNode) Reload(rc ReloadableConfig) error {
	if len(rc.Routes) == 0 {
		return nil
	}
	n.reloadMu.Lock()
	defer n.reloadMu.Unlock()

	current := *n.routes.Load()
	next := make(routeTable, len(current))
	for k, v := range current {
		next[k] = v
	}
	for svcID, routes := range rc.Routes {
		next[svcID] = routes
	}
	n.routes.Store(&next)

	n.cleanStaleDialers(next)

	n.logger.Info("relay: routes hot-reloaded", "updated_services", len(rc.Routes))
	return nil
}

func (n *RelayNode) cleanStaleDialers(routes routeTable) {
	valid := make(map[dialerKey]struct{})
	for svcID, idcRoutes := range routes {
		for _, rt := range idcRoutes {
			valid[dialerKey{svcID, rt.IDC}] = struct{}{}
		}
	}

	n.dialersMu.Lock()
	defer n.dialersMu.Unlock()
	for key := range n.dialers {
		if _, ok := valid[key]; !ok {
			n.logger.Info("relay: removing stale dialer",
				"service", key.serviceID, "idc", key.idc)
			delete(n.dialers, key)
		}
	}
}

func (n *RelayNode) handleTunnel(req *tunnel.HandshakeRequest, inbound *tunnel.Transport) {
	session := tunnel.NewTunnelSession(req, inbound, n.logger)
	defer session.Close()

	routes := *n.routes.Load()
	idcRoutes, ok := routes[req.ServiceID]
	if !ok {
		session.Logger().Warn("relay: unknown service")
		return
	}
	nextHops := config.HopsForIDC(idcRoutes, req.TargetIDC)
	if len(nextHops) == 0 {
		session.Logger().Warn("relay: no route for target IDC")
		return
	}

	forwardReq := *req
	forwardReq.HopCount++
	if forwardReq.HopCount > tunnel.MaxHopCount {
		session.Logger().Error("relay: hop count exceeded, dropping", "max", tunnel.MaxHopCount)
		return
	}

	dialer := n.dialerFor(req.ServiceID, req.TargetIDC, nextHops)
	outbound, err := dialer.DialWithFallback(&forwardReq)
	if err != nil {
		session.Logger().Error("relay: next-hop dial failed", "err", err)
		return
	}
	session.RunRelay(outbound)
}

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
	// FIX: use node's ctx so prober stops when RelayNode.Stop() is called.
	pool.StartProber(n.ctx, defaultProbeInterval)
	d = tunnel.NewDialer(pool, n.frameSize, n.psk, n.logger)
	n.dialers[key] = d
	return d
}

func buildRouteTable(services []config.ServiceConfig) (routeTable, error) {
	tbl := make(routeTable, len(services))
	for i := range services {
		svc := &services[i]
		if len(svc.Routes) == 0 {
			return nil, fmt.Errorf("relay service %q has no routes", svc.ID)
		}
		tbl[svc.ID] = svc.Routes
	}
	return tbl, nil
}
