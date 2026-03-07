// Package node implements the three proxy node types.
package node

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
	"github.com/sxfstudy-colorful/accel-proxy/internal/proxy"
	"github.com/sxfstudy-colorful/accel-proxy/internal/tunnel"
)

// ─────────────────────────────────────────────────────────────────────────────
// AccessNode
//
// Listens on N ports (one per service). When a client connects:
//  1. Identifies the service by the port number.
//  2. Dials the next-hop WebSocket tunnel.
//  3. Sends a HandshakeRequest carrying routing metadata.
//  4. Bridges the client TCP connection ↔ tunnel Transport.
// ─────────────────────────────────────────────────────────────────────────────

// AccessNode is the client-facing edge of the proxy pipeline.
type AccessNode struct {
	cfg       *config.Config
	dialers   map[int]*tunnel.Dialer // port → dialer
	l7proxies map[int]*proxy.L7Proxy // port → L7 proxy (http services)
	listeners []net.Listener
	logger    *slog.Logger
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewAccessNode constructs an AccessNode from the given config.
func NewAccessNode(cfg *config.Config, logger *slog.Logger) (*AccessNode, error) {
	n := &AccessNode{
		cfg:       cfg,
		dialers:   make(map[int]*tunnel.Dialer),
		l7proxies: make(map[int]*proxy.L7Proxy),
		logger:    logger,
	}

	for i := range cfg.Services {
		svc := &cfg.Services[i]

		n.dialers[svc.Port] = tunnel.NewDialerFromService(
			svc, cfg.Tunnel.MaxFrameSize, logger,
		)

		if svc.Protocol == config.ProtocolHTTP || svc.Protocol == config.ProtocolHTTPS {
			lp, err := proxy.NewL7Proxy(*svc, logger)
			if err != nil {
				return nil, fmt.Errorf("init L7 proxy for port %d: %w", svc.Port, err)
			}
			n.l7proxies[svc.Port] = lp
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
			"next_hops", svc.NextHops,
		)

		n.wg.Add(1)
		go n.acceptLoop(ctx, ln, svc)
	}

	return nil
}

// Stop shuts down all listeners.
func (n *AccessNode) Stop() {
	if n.cancel != nil {
		n.cancel()
	}
	for _, ln := range n.listeners {
		ln.Close()
	}
	n.wg.Wait()
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
			n.handleConn(ctx, conn, svc)
		}()
	}
}

func (n *AccessNode) handleConn(ctx context.Context, conn net.Conn, svc *config.ServiceConfig) {
	defer conn.Close()

	clientAddr, _, _ := net.SplitHostPort(conn.RemoteAddr().String())

	n.logger.Info("new connection",
		"service", svc.ID,
		"client", conn.RemoteAddr(),
		"protocol", svc.Protocol,
	)

	// Build the handshake that travels through all hops.
	req := &tunnel.HandshakeRequest{
		Version:    tunnel.Version,
		ServiceID:  svc.ID,
		TargetHost: svc.Origin.Host,
		TargetPort: svc.Origin.Port,
		Protocol:   string(svc.Protocol),
		ClientIP:   clientAddr,
		HopCount:   0,
	}

	dialer := n.dialers[svc.Port]
	tun, err := dialer.DialWithFallback(req)
	if err != nil {
		n.logger.Error("tunnel dial failed", "service", svc.ID, "err", err)
		return
	}

	// Bridge client ↔ tunnel.
	n.logger.Debug("tunnel established, relaying",
		"service", svc.ID,
		"client", conn.RemoteAddr(),
	)
	tunnel.Relay(conn, tun, n.logger)
}
