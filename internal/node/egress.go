package node

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
	"github.com/sxfstudy-colorful/accel-proxy/internal/tunnel"
)

const shutdownTimeout = 15 * time.Second

// ─────────────────────────────────────────────────────────────────────────────
// EgressNode
//
// The last proxy node before the real origin server.  It:
//  1. Runs a tunnel.Server to accept inbound WS tunnels (from access/relay).
//  2. Dials the real origin TCP socket (with optional TLS).
//  3. Bridges the tunnel Transport ↔ origin TCP connection.
// ─────────────────────────────────────────────────────────────────────────────

// EgressNode terminates tunnels and connects to real origin servers.
type EgressNode struct {
	cfg    *config.Config
	server *tunnel.Server
	logger *slog.Logger
}

// NewEgressNode constructs an EgressNode.
func NewEgressNode(cfg *config.Config, logger *slog.Logger) *EgressNode {
	n := &EgressNode{cfg: cfg, logger: logger}
	n.server = tunnel.NewServer(&cfg.Tunnel, cfg.Node.ID, n.handleTunnel, logger)
	return n
}

// Start begins accepting inbound tunnel connections (non-blocking).
func (n *EgressNode) Start() error {
	n.logger.Info("egress node starting", "id", n.cfg.Node.ID)
	go func() {
		if err := n.server.Start(); err != nil {
			n.logger.Error("egress tunnel server error", "err", err)
		}
	}()
	return nil
}

// Stop shuts down the egress node.
func (n *EgressNode) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	n.server.Stop(ctx) //nolint:errcheck
}

// ReloadCert triggers an immediate TLS certificate reload from disk.
// Satisfies the main.CertReloader interface.
func (n *EgressNode) ReloadCert() error {
	return n.server.ReloadCert()
}

// handleTunnel is called for each accepted inbound tunnel.
func (n *EgressNode) handleTunnel(req *tunnel.HandshakeRequest, tun *tunnel.Transport) {
	defer tun.Close()

	// Resolve origin from HandshakeRequest (set by access node from service config).
	originAddr := fmt.Sprintf("%s:%d", req.TargetHost, req.TargetPort)

	dialTimeout := n.originDialTimeout(req)
	origin, err := dialOrigin(originAddr, req.Protocol == "https", dialTimeout)
	if err != nil {
		n.logger.Error("egress: dial origin failed",
			"service", req.ServiceID,
			"origin", originAddr,
			"err", err,
		)
		return
	}
	defer origin.Close()

	n.logger.Info("egress: origin connected",
		"service", req.ServiceID,
		"origin", originAddr,
		"client_ip", req.ClientIP,
		"hops", req.HopCount,
	)

	// Bridge tunnel ↔ origin TCP.
	tunnel.Relay(origin, tun, n.logger)
}

// dialOrigin opens a TCP (or TLS) connection to the origin.
func dialOrigin(addr string, useTLS bool, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("tcp dial %q: %w", addr, err)
	}

	if useTLS {
		host, _, _ := net.SplitHostPort(addr)
		tlsConn := tls.Client(conn, &tls.Config{
			ServerName: host,
			// For internal services you may want to skip verification:
			// InsecureSkipVerify: true,
		})
		if err := tlsConn.HandshakeContext(context.Background()); err != nil {
			conn.Close()
			return nil, fmt.Errorf("tls handshake %q: %w", addr, err)
		}
		return tlsConn, nil
	}

	return conn, nil
}

// originDialTimeout returns the dial timeout, using a default if unconfigured.
func (n *EgressNode) originDialTimeout(req *tunnel.HandshakeRequest) time.Duration {
	// Look up service config for per-service overrides.
	for i := range n.cfg.Services {
		svc := &n.cfg.Services[i]
		if svc.ID == req.ServiceID && svc.Origin.DialTimeout > 0 {
			return svc.Origin.DialTimeout
		}
	}
	return 10 * time.Second
}
