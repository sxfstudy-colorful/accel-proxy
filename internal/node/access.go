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
// serviceInstance — per-service lifecycle owner
//
// Each service gets its own instance that owns:
//   - the TCP listener bound to the service port
//   - the accept loop goroutine (and all connection goroutines it spawns)
//   - the hot-reloadable routing state (dialer + targetIDC)
//   - the L7 handler (nil for TCP services)
//
// Lifecycle:
//   newServiceInstance → start() → [running] → stop()
//
// Hot-reload updates only the state pointer; the listener and l7handler
// are immutable for the lifetime of the instance (port/protocol change
// requires stop + new instance).
// ─────────────────────────────────────────────────────────────────────────────

// serviceState is the hot-reloadable portion of a service instance.
// Swapped atomically on Reload; in-flight connections keep their snapshot.
type serviceState struct {
	dialer    *tunnel.Dialer
	targetIDC string
}

// serviceInstance owns one service's complete runtime lifecycle.
type serviceInstance struct {
	// cfg holds the immutable fields: ID, Port, Protocol.
	// Mutable routing (Groups) lives in state.
	cfg config.ServiceConfig

	// state is replaced atomically on Reload.
	// Each connection snapshots it once at accept time.
	state atomic.Pointer[serviceState]

	// l7handler is nil for TCP services; non-nil for HTTP/HTTPS.
	// Immutable: protocol cannot change without restarting the instance.
	l7handler *proxy.HTTPConnHandler

	frameSize int64
	logger    *slog.Logger

	listener net.Listener
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// newServiceInstance constructs a serviceInstance from a ServiceConfig.
// It does not bind the port yet; call start() for that.
func newServiceInstance(svc config.ServiceConfig, frameSize int64, logger *slog.Logger) (*serviceInstance, error) {
	hops := svc.SortedHops()
	if len(hops) == 0 {
		return nil, fmt.Errorf("service %q: no hops configured", svc.ID)
	}

	inst := &serviceInstance{
		cfg:       svc,
		frameSize: frameSize,
		logger:    logger.With("service", svc.ID, "port", svc.Port),
	}

	inst.state.Store(&serviceState{
		dialer:    tunnel.NewDialer(tunnel.NewHopPool(hops, nil, logger), frameSize, logger),
		targetIDC: svc.TargetIDC,
	})

	if svc.Protocol == config.ProtocolHTTP || svc.Protocol == config.ProtocolHTTPS {
		inst.l7handler = proxy.NewHTTPConnHandler(svc, logger)
	}

	return inst, nil
}

// start binds the TCP port and launches the accept loop.
func (inst *serviceInstance) start() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", inst.cfg.Port))
	if err != nil {
		return fmt.Errorf("listen port %d (service %q): %w",
			inst.cfg.Port, inst.cfg.ID, err)
	}
	inst.listener = ln

	ctx, cancel := context.WithCancel(context.Background())
	inst.cancel = cancel

	inst.logger.Info("service started",
		"protocol", inst.cfg.Protocol,
		"target_idc", inst.cfg.TargetIDC,
	)

	inst.wg.Add(1)
	go inst.acceptLoop(ctx)
	return nil
}

// stop closes the listener and waits for all in-flight connections to finish.
func (inst *serviceInstance) stop() {
	if inst.cancel != nil {
		inst.cancel()
	}
	if inst.listener != nil {
		inst.listener.Close()
	}
	inst.wg.Wait()
	inst.logger.Info("service stopped")
}

// reloadGroups replaces the routing state atomically.
// In-flight connections keep the old state; new connections pick up the new one.
func (inst *serviceInstance) reloadGroups(groups []config.RouteGroup) {
	hops := config.SortGroups(groups)
	if len(hops) == 0 {
		inst.logger.Warn("reload: no hops in new groups, keeping current state")
		return
	}
	inst.state.Store(&serviceState{
		dialer:    tunnel.NewDialer(tunnel.NewHopPool(hops, nil, inst.logger), inst.frameSize, inst.logger),
		targetIDC: inst.cfg.TargetIDC,
	})
	inst.logger.Info("service routes reloaded", "hops", len(hops))
}

// acceptLoop runs in its own goroutine for the lifetime of the instance.
func (inst *serviceInstance) acceptLoop(ctx context.Context) {
	defer inst.wg.Done()
	for {
		conn, err := inst.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				inst.logger.Warn("accept error", "err", err)
				continue
			}
		}
		inst.wg.Add(1)
		go func() {
			defer inst.wg.Done()
			inst.handleConn(conn)
		}()
	}
}

// handleConn processes one inbound client connection.
func (inst *serviceInstance) handleConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
	}()

	clientAddr, _, _ := net.SplitHostPort(conn.RemoteAddr().String())

	// Snapshot routing state once. A concurrent reloadGroups() may replace
	// the pointer, but this connection uses the state it observed at accept
	// time for its full lifetime.
	state := inst.state.Load()

	inst.logger.Info("new connection",
		"client", conn.RemoteAddr(),
		"protocol", inst.cfg.Protocol,
		"target_idc", state.targetIDC,
	)

	req := &tunnel.HandshakeRequest{
		Version:   tunnel.Version,
		ServiceID: inst.cfg.ID,
		TargetIDC: state.targetIDC,
		Protocol:  string(inst.cfg.Protocol),
		ClientIP:  clientAddr,
		HopCount:  0,
	}

	tun, err := state.dialer.DialWithFallback(req)
	if err != nil {
		inst.logger.Error("tunnel dial failed",
			"target_idc", state.targetIDC, "err", err)
		return
	}

	defer func() {
		_ = tun.Close()
	}()

	switch inst.cfg.Protocol {
	case config.ProtocolHTTP, config.ProtocolHTTPS:
		inst.l7handler.Handle(conn, tun)
	default:
		tunnel.Relay(conn, tun, inst.logger)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// AccessNode
//
// Manages a map of serviceInstances, one per service ID.
//
// Reload diff semantics:
//   added   — new ServiceConfig not present in current map → start new instance
//   removed — running instance whose ID is absent from new config → stop
//   updated — same ID present in both → hot-reload groups only
//             (port/protocol changes are ignored; they require a restart)
// ─────────────────────────────────────────────────────────────────────────────

// AccessNode is the client-facing edge of the proxy pipeline.
type AccessNode struct {
	frameSize int64
	logger    *slog.Logger

	mu       sync.Mutex
	services map[string]*serviceInstance // keyed by service ID
}

var _ Node = (*AccessNode)(nil)

// NewAccessNode constructs an AccessNode from its dedicated role config.
// Service instances are created but not started; call Start() to bind ports.
func NewAccessNode(cfg *config.AccessConfig, logger *slog.Logger) (*AccessNode, error) {
	frameSize := cfg.MaxFrameSize
	if frameSize == 0 {
		frameSize = 64 * 1024
	}
	n := &AccessNode{
		frameSize: frameSize,
		logger:    logger,
		services:  make(map[string]*serviceInstance),
	}

	for _, svc := range cfg.Services {
		inst, err := newServiceInstance(svc, n.frameSize, logger)
		if err != nil {
			return nil, err
		}
		n.services[svc.ID] = inst
	}
	return n, nil
}

// Start binds all service ports and begins accepting connections.
// If any port fails to bind, already-started services are stopped before
// returning the error.
func (n *AccessNode) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	var started []*serviceInstance
	for _, inst := range n.services {
		if err := inst.start(); err != nil {
			for _, s := range started {
				s.stop()
			}
			return err
		}
		started = append(started, inst)
	}
	return nil
}

// Stop shuts down all service instances and waits for in-flight connections.
func (n *AccessNode) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, inst := range n.services {
		inst.stop()
	}
}

// Reload applies the diff between the running services and the new config:
//
//   - New services (in AccessServices, not currently running) are started.
//   - Removed services (running, absent from AccessServices) are stopped.
//   - Existing services have their Groups hot-reloaded from rc.Groups.
//
// Immutable fields (port, protocol) are not changed; a mismatch is logged
// as a warning and the running instance is kept as-is.
func (n *AccessNode) Reload(rc ReloadableConfig) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Build lookup set of incoming service IDs.
	incoming := make(map[string]config.ServiceConfig, len(rc.AccessServices))
	for _, svc := range rc.AccessServices {
		incoming[svc.ID] = svc
	}

	// ── 1. Stop removed services ──────────────────────────────────────────
	for id, inst := range n.services {
		if _, exists := incoming[id]; !exists {
			n.logger.Info("access reload: stopping removed service", "service", id)
			inst.stop()
			delete(n.services, id)
		}
	}

	// ── 2. Update existing / start new services ───────────────────────────
	for id, svc := range incoming {
		if inst, running := n.services[id]; running {
			// Warn if immutable fields changed; can't apply without restart.
			if inst.cfg.Port != svc.Port {
				n.logger.Warn("access reload: port change requires restart, ignored",
					"service", id, "old", inst.cfg.Port, "new", svc.Port)
			}
			if inst.cfg.Protocol != svc.Protocol {
				n.logger.Warn("access reload: protocol change requires restart, ignored",
					"service", id, "old", inst.cfg.Protocol, "new", svc.Protocol)
			}
			// Hot-reload the routing groups.
			if groups, ok := rc.Groups[id]; ok {
				inst.reloadGroups(groups)
			}
		} else {
			// New service: construct and start a fresh instance.
			n.logger.Info("access reload: starting new service", "service", id)
			inst, err := newServiceInstance(svc, n.frameSize, n.logger)
			if err != nil {
				n.logger.Error("access reload: failed to create new service",
					"service", id, "err", err)
				continue
			}
			if err := inst.start(); err != nil {
				n.logger.Error("access reload: failed to start new service",
					"service", id, "err", err)
				continue
			}
			n.services[id] = inst
		}
	}

	return nil
}
