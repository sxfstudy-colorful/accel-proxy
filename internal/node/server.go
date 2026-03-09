package node

import (
	"fmt"
	"log/slog"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
)

// ─────────────────────────────────────────────────────────────────────────────
// ProxyServer — multi-role process entry point
//
// A single process may run any combination of the three roles:
//
//   access  — accepts client connections, dials tunnels outward
//   relay   — accepts inbound tunnels, forwards to next hop
//   egress  — accepts inbound tunnels, connects to origin servers
//
// Each active role is an independent node instance with its own listener,
// goroutines, and hot-reload state. Roles absent from the config (nil section)
// are simply not started.
//
// Lifecycle:
//   NewProxyServer → Start() → [running] → Stop()
//   SIGHUP → Reload(rc) + ReloadCert()
// ─────────────────────────────────────────────────────────────────────────────

// ProxyServer holds the active role instances for this process.
type ProxyServer struct {
	access *AccessNode
	relay  *RelayNode
	egress *EgressNode
	logger *slog.Logger
}

// NewProxyServer constructs a ProxyServer from a Config.
// Only roles with a non-nil section in cfg are instantiated.
func NewProxyServer(cfg *config.Config, logger *slog.Logger) (*ProxyServer, error) {
	srv := &ProxyServer{logger: logger}

	if cfg.Access != nil {
		n, err := NewAccessNode(cfg.Access, logger)
		if err != nil {
			return nil, fmt.Errorf("access role: %w", err)
		}
		srv.access = n
	}

	if cfg.Relay != nil {
		n, err := NewRelayNode(cfg.Node.ID, cfg.Relay, logger)
		if err != nil {
			return nil, fmt.Errorf("relay role: %w", err)
		}
		srv.relay = n
	}

	if cfg.Egress != nil {
		srv.egress = NewEgressNode(cfg.Node.ID, cfg.Node.IDC, cfg.Egress, logger)
	}

	return srv, nil
}

// Start starts all active roles.
// If any role fails to start, already-started roles are stopped before returning.
func (s *ProxyServer) Start() error {
	var started []func()

	rollback := func() {
		for _, stop := range started {
			stop()
		}
	}

	if s.access != nil {
		if err := s.access.Start(); err != nil {
			rollback()
			return fmt.Errorf("start access: %w", err)
		}
		started = append(started, s.access.Stop)
	}
	if s.relay != nil {
		if err := s.relay.Start(); err != nil {
			rollback()
			return fmt.Errorf("start relay: %w", err)
		}
		started = append(started, s.relay.Stop)
	}
	if s.egress != nil {
		if err := s.egress.Start(); err != nil {
			rollback()
			return fmt.Errorf("start egress: %w", err)
		}
	}
	return nil
}

// Stop gracefully shuts down all active roles concurrently and waits for all
// in-flight connections to drain.
func (s *ProxyServer) Stop() {
	type stopper interface{ Stop() }
	var roles []stopper
	if s.egress != nil {
		roles = append(roles, s.egress)
	}
	if s.relay != nil {
		roles = append(roles, s.relay)
	}
	if s.access != nil {
		roles = append(roles, s.access)
	}

	done := make(chan struct{}, len(roles))
	for _, r := range roles {
		r := r
		go func() { r.Stop(); done <- struct{}{} }()
	}
	for range roles {
		<-done
	}
}

// Reload dispatches hot-reload to each active role.
// Each role only processes the ReloadableConfig fields relevant to it.
func (s *ProxyServer) Reload(rc ReloadableConfig) error {
	if s.access != nil {
		if err := s.access.Reload(rc); err != nil {
			s.logger.Error("reload: access role failed", "err", err)
		}
	}
	if s.relay != nil {
		if err := s.relay.Reload(rc); err != nil {
			s.logger.Error("reload: relay role failed", "err", err)
		}
	}
	if s.egress != nil {
		if err := s.egress.Reload(rc); err != nil {
			s.logger.Error("reload: egress role failed", "err", err)
		}
	}
	return nil
}

// ReloadCert triggers TLS certificate reload on roles that own a tunnel listener.
func (s *ProxyServer) ReloadCert() error {
	if s.relay != nil {
		if err := s.relay.ReloadCert(); err != nil {
			s.logger.Error("reload cert: relay failed", "err", err)
		}
	}
	if s.egress != nil {
		if err := s.egress.ReloadCert(); err != nil {
			s.logger.Error("reload cert: egress failed", "err", err)
		}
	}
	return nil
}
