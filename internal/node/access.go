package node

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
	"github.com/sxfstudy-colorful/accel-proxy/internal/proxy"
	"github.com/sxfstudy-colorful/accel-proxy/internal/tunnel"
)

// ─────────────────────────────────────────────────────────────────────────────
// AccessNode  (边缘机房，面向客户端)
//
// 职责：按监听端口识别业务，建立到下一跳的隧道，L4/L7 分发。
//
// 热重载：
//   Routes（next_hops）通过 atomic.Pointer 热更新，Dialer 随之替换。
//   已建立的连接不受影响；新连接立即使用新路由。
//   监听端口和协议类型不支持热更新（需重启）。
// ─────────────────────────────────────────────────────────────────────────────

// accessServiceState holds the hot-reloadable state for one service.
type accessServiceState struct {
	dialer    *tunnel.Dialer
	targetIDC string
}

// AccessNode is the client-facing edge of the proxy pipeline.
type AccessNode struct {
	cfg *config.Config

	// states: port → atomic pointer to service state (hot-reloadable)
	states     map[int]*atomic.Pointer[accessServiceState]
	l7handlers map[int]*proxy.HTTPConnHandler // port → L7 handler (immutable)

	listeners []net.Listener
	logger    *slog.Logger
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

var _ Node = (*AccessNode)(nil)

// NewAccessNode constructs an AccessNode from the given config.
func NewAccessNode(cfg *config.Config, logger *slog.Logger) (*AccessNode, error) {
	n := &AccessNode{
		cfg:        cfg,
		states:     make(map[int]*atomic.Pointer[accessServiceState]),
		l7handlers: make(map[int]*proxy.HTTPConnHandler),
		logger:     logger,
	}

	for i := range cfg.Services {
		svc := &cfg.Services[i]

		hops, ok := svc.NextHopsForIDC(svc.TargetIDC)
		if !ok {
			return nil, fmt.Errorf(
				"service %q: no routes for target_idc %q", svc.ID, svc.TargetIDC)
		}

		state := &accessServiceState{
			dialer:    tunnel.NewDialer(tunnel.NewHopPool(hops, nil, logger), cfg.Tunnel.MaxFrameSize, logger),
			targetIDC: svc.TargetIDC,
		}
		ptr := &atomic.Pointer[accessServiceState]{}
		ptr.Store(state)
		n.states[svc.Port] = ptr

		if svc.Protocol == config.ProtocolHTTP || svc.Protocol == config.ProtocolHTTPS {
			n.l7handlers[svc.Port] = proxy.NewHTTPConnHandler(*svc, logger)
		}
	}

	return n, nil
}

// Start binds all service ports and begins accepting connections.
func (n *AccessNode) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel

	for i := range n.cfg.Services {
		svc := &n.cfg.Services[i]

		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", svc.Port))
		if err != nil {
			n.Stop()
			return fmt.Errorf("listen port %d (service %q): %w", svc.Port, svc.ID, err)
		}
		n.listeners = append(n.listeners, ln)

		n.logger.Info("access node listening",
			"port", svc.Port,
			"service", svc.ID,
			"protocol", svc.Protocol,
			"target_idc", svc.TargetIDC,
		)

		n.wg.Add(1)
		go n.acceptLoop(ctx, ln, svc)
	}

	return nil
}

// Stop closes all listeners and waits for in-flight connections to finish.
func (n *AccessNode) Stop() {
	if n.cancel != nil {
		n.cancel()
	}
	for _, ln := range n.listeners {
		ln.Close()
	}
	n.wg.Wait()
}

// Reload applies updated routes without dropping existing connections.
func (n *AccessNode) Reload(rc ReloadableConfig) error {
	if len(rc.Routes) == 0 {
		return nil
	}

	for i := range n.cfg.Services {
		svc := &n.cfg.Services[i]
		idcRoutes, ok := rc.Routes[svc.ID]
		if !ok {
			continue
		}
		// Find the IDCRoute entry for this service's target IDC and extract
		// priority-sorted hops the same way the relay node does.
		var hops []config.HopAddr
		for j := range idcRoutes {
			if idcRoutes[j].IDC == svc.TargetIDC {
				hops = idcRoutes[j].SortedHops()
				break
			}
		}
		if len(hops) == 0 {
			n.logger.Warn("access reload: no hops for service, keeping old routes",
				"service", svc.ID, "target_idc", svc.TargetIDC)
			continue
		}

		newState := &accessServiceState{
			dialer:    tunnel.NewDialer(tunnel.NewHopPool(hops, nil, n.logger), n.cfg.Tunnel.MaxFrameSize, n.logger),
			targetIDC: svc.TargetIDC,
		}
		if ptr, ok := n.states[svc.Port]; ok {
			ptr.Store(newState)
			n.logger.Info("access: routes hot-reloaded",
				"service", svc.ID, "target_idc", svc.TargetIDC, "hops", len(hops))
		}
	}
	return nil
}

func (n *AccessNode) acceptLoop(ctx context.Context, ln net.Listener, svc *config.ServiceConfig) {
	defer n.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				n.logger.Warn("accept error", "port", svc.Port, "err", err)
				continue
			}
		}
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.handleConn(conn, svc)
		}()
	}
}

func (n *AccessNode) handleConn(conn net.Conn, svc *config.ServiceConfig) {
	defer conn.Close()

	clientAddr, _, _ := net.SplitHostPort(conn.RemoteAddr().String())

	// Snapshot the current state for this connection.
	// A concurrent Reload() may swap the pointer, but this connection
	// uses the state it observed at accept time for its full lifetime.
	state := n.states[svc.Port].Load()

	n.logger.Info("new connection",
		"service", svc.ID,
		"client", conn.RemoteAddr(),
		"protocol", svc.Protocol,
		"target_idc", state.targetIDC,
	)

	req := &tunnel.HandshakeRequest{
		Version:   tunnel.Version,
		ServiceID: svc.ID,
		TargetIDC: state.targetIDC,
		Protocol:  string(svc.Protocol),
		ClientIP:  clientAddr,
		HopCount:  0,
	}

	tun, err := state.dialer.DialWithFallback(req)
	if err != nil {
		n.logger.Error("tunnel dial failed",
			"service", svc.ID, "target_idc", state.targetIDC, "err", err)
		return
	}

	n.logger.Debug("tunnel established",
		"service", svc.ID, "target_idc", state.targetIDC)

	switch svc.Protocol {
	case config.ProtocolHTTP, config.ProtocolHTTPS:
		n.l7handlers[svc.Port].Handle(conn, tun)
	default:
		tunnel.Relay(conn, tun, n.logger)
	}
}
