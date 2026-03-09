// Package node implements the three proxy node types and their shared base.
package node

import (
	"context"
	"log/slog"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
	"github.com/sxfstudy-colorful/accel-proxy/internal/tunnel"
)

const shutdownTimeout = 15 * time.Second

// ─────────────────────────────────────────────────────────────────────────────
// Public interfaces — defined here so the node package owns the contract.
// main.go uses these; nothing leaks into cmd/.
// ─────────────────────────────────────────────────────────────────────────────

// Node is the lifecycle interface for all proxy node types.
type Node interface {
	// Start begins accepting connections. Non-blocking for relay/egress;
	// blocks for access (caller should run in a goroutine if needed).
	Start() error
	// Stop performs a graceful shutdown, waiting for in-flight sessions to
	// finish or until the internal shutdownTimeout expires.
	Stop()
	// Reload applies a new configuration without interrupting existing
	// connections. Fields that cannot be changed at runtime (node type,
	// listen address, node ID) are ignored; a mismatch is logged as a warning.
	Reload(cfg ReloadableConfig) error
}

// CertReloader is optionally implemented by nodes that expose a tunnel server
// with TLS enabled. Triggering a reload is idempotent and safe to call from
// a signal handler.
type CertReloader interface {
	ReloadCert() error
}

// ReloadableConfig carries the subset of configuration that can be changed
// at runtime via SIGHUP without restarting the process or dropping connections.
//
// Fields that are intentionally excluded (cannot hot-reload):
//   - Node.Type, Node.ID, Node.IDC  — identity; changing requires restart
//   - Tunnel.ListenAddr             — rebinding a port requires restart
//   - Service.Port                  — rebinding a port requires restart
//   - Service.Protocol              — L4/L7 mode switch requires restart
type ReloadableConfig struct {
	// AccessServices is the full updated service list for access nodes.
	// Used to diff against running services: new entries are started,
	// removed entries are stopped, existing entries have their Groups hot-reloaded.
	// Immutable fields (port, protocol) are not changed at runtime.
	AccessServices []config.ServiceConfig

	// Groups is the updated route groups for access nodes.
	// Key: serviceID → updated []RouteGroup.
	Groups map[string][]config.RouteGroup

	// Routes is the updated IDC routing table for relay nodes.
	// Key: serviceID → updated []IDCRoute.
	Routes map[string][]config.IDCRoute

	// Origins is the updated origin address mapping for egress nodes.
	// Key: serviceID → updated OriginConfig.
	Origins map[string]OriginUpdate

	// LogLevel allows changing the log level at runtime.
	LogLevel string
}

// OriginUpdate carries the fields of OriginConfig that can be changed at runtime.
type OriginUpdate struct {
	Host        string
	Port        int
	TLS         bool
	DialTimeout time.Duration
}

// ─────────────────────────────────────────────────────────────────────────────
// baseNode — shared lifecycle logic for relay and egress nodes.
//
// Both RelayNode and EgressNode own a *tunnel.Server and have identical
// Start / Stop / ReloadCert implementations. baseNode factors those out.
// AccessNode does NOT embed baseNode because it manages its own listeners.
// ─────────────────────────────────────────────────────────────────────────────

type baseNode struct {
	server *tunnel.Server
	logger *slog.Logger
}

// startServer starts the tunnel server in a background goroutine and returns
// immediately. Errors from the server are logged.
func (b *baseNode) startServer(label string) error {
	go func() {
		if err := b.server.Start(); err != nil {
			b.logger.Error("tunnel server exited", "node", label, "err", err)
		}
	}()
	return nil
}

// stopServer gracefully shuts the tunnel server down.
func (b *baseNode) stopServer() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := b.server.Stop(ctx); err != nil {
		b.logger.Warn("tunnel server shutdown error", "err", err)
	}
}

// reloadCert triggers an immediate TLS certificate reload.
func (b *baseNode) reloadCert() error {
	return b.server.ReloadCert()
}
