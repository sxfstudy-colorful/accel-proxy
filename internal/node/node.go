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

// Node is the lifecycle interface for all proxy node types.
type Node interface {
	Start() error
	Stop()
	Reload(cfg ReloadableConfig) error
}

// CertReloader is optionally implemented by nodes that expose a tunnel server
// with TLS enabled.
type CertReloader interface {
	ReloadCert() error
}

// ReloadableConfig carries the subset of configuration that can be changed
// at runtime via SIGHUP.
type ReloadableConfig struct {
	AccessServices []config.ServiceConfig
	Groups         map[string][]config.RouteGroup
	Routes         map[string][]config.IDCRoute
	Origins        map[string]OriginUpdate
	LogLevel       string
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
// ─────────────────────────────────────────────────────────────────────────────

type baseNode struct {
	server *tunnel.Server
	logger *slog.Logger
}

func (b *baseNode) startServer(label string) error {
	go func() {
		if err := b.server.Start(); err != nil {
			b.logger.Error("tunnel server exited", "node", label, "err", err)
		}
	}()
	return nil
}

func (b *baseNode) stopServer() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := b.server.Stop(ctx); err != nil {
		b.logger.Warn("tunnel server shutdown error", "err", err)
	}
}

func (b *baseNode) reloadCert() error {
	return b.server.ReloadCert()
}
