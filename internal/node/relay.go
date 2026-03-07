package node

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
	"github.com/sxfstudy-colorful/accel-proxy/internal/tunnel"
)

// ─────────────────────────────────────────────────────────────────────────────
// RelayNode
//
// A middle proxy node.  It:
//  1. Runs a tunnel.Server to accept inbound WS tunnels from access/relay nodes.
//  2. For each accepted tunnel, dials the next-hop (another relay or egress).
//  3. Increments HopCount as a loop guard.
//  4. Bridges the two tunnel Transports together.
//
// The next-hop URL for a relay node is stored per-service in the incoming
// HandshakeRequest's ServiceID, which is looked up in the node's config.
// ─────────────────────────────────────────────────────────────────────────────

// RelayNode forwards tunnels one hop closer to the origin.
type RelayNode struct {
	cfg    *config.Config
	server *tunnel.Server
	// service registry built from config for fast O(1) lookup
	services map[string]*config.ServiceConfig
	logger   *slog.Logger
}

// NewRelayNode constructs a RelayNode.
func NewRelayNode(cfg *config.Config, logger *slog.Logger) (*RelayNode, error) {
	n := &RelayNode{
		cfg:      cfg,
		services: make(map[string]*config.ServiceConfig),
		logger:   logger,
	}

	for i := range cfg.Services {
		svc := &cfg.Services[i]
		n.services[svc.ID] = svc
		if len(svc.NextHops) == 0 {
			return nil, fmt.Errorf("relay service %q has no next_hops", svc.ID)
		}
	}

	n.server = tunnel.NewServer(&cfg.Tunnel, cfg.Node.ID, n.handleTunnel, logger)
	return n, nil
}

// Start begins accepting inbound tunnel connections (non-blocking).
func (n *RelayNode) Start() error {
	n.logger.Info("relay node starting", "id", n.cfg.Node.ID)
	go func() {
		if err := n.server.Start(); err != nil {
			n.logger.Error("relay tunnel server error", "err", err)
		}
	}()
	return nil
}

// Stop shuts down the relay node.
func (n *RelayNode) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	n.server.Stop(ctx) //nolint:errcheck
}

// ReloadCert triggers an immediate TLS certificate reload from disk.
// Satisfies the main.CertReloader interface.
func (n *RelayNode) ReloadCert() error {
	return n.server.ReloadCert()
}

// handleTunnel is called for each accepted inbound tunnel.
func (n *RelayNode) handleTunnel(req *tunnel.HandshakeRequest, inbound *tunnel.Transport) {
	defer inbound.Close()

	svc, ok := n.services[req.ServiceID]
	if !ok {
		n.logger.Warn("relay: unknown service", "service_id", req.ServiceID)
		return
	}

	// Forward request with incremented hop count.
	forwardReq := *req
	forwardReq.HopCount++

	dialer := tunnel.NewDialer(svc.NextHops, n.cfg.Tunnel.MaxFrameSize, n.logger)
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
