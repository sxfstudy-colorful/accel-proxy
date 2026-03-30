package node

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
	"github.com/sxfstudy-colorful/accel-proxy/internal/proxy"
	"github.com/sxfstudy-colorful/accel-proxy/internal/tunnel"
)

const defaultProbeInterval = 10 * time.Second

// ─────────────────────────────────────────────────────────────────────────────
// serviceState / serviceInstance
// ─────────────────────────────────────────────────────────────────────────────

type serviceState struct {
	dialer    *tunnel.Dialer
	targetIDC string
}

type serviceInstance struct {
	cfg       config.ServiceConfig
	state     atomic.Pointer[serviceState]
	l7handler *proxy.HTTPConnHandler

	frameSize int64
	psk       string // PSK for tunnel auth
	logger    *slog.Logger

	listener  net.Listener
	cancel    context.CancelFunc
	proberCtx context.Context // context used by prober goroutines
	wg        sync.WaitGroup
}

func newServiceInstance(svc config.ServiceConfig, frameSize int64, psk string, logger *slog.Logger) (*serviceInstance, error) {
	hops := svc.SortedHops()
	if len(hops) == 0 {
		return nil, fmt.Errorf("service %q: no hops configured", svc.ID)
	}

	inst := &serviceInstance{
		cfg:       svc,
		frameSize: frameSize,
		psk:       psk,
		logger:    logger.With("service", svc.ID, "port", svc.Port),
	}

	inst.state.Store(&serviceState{
		dialer:    tunnel.NewDialer(tunnel.NewHopPool(hops, nil, logger), frameSize, psk, logger),
		targetIDC: svc.TargetIDC,
	})

	if svc.Protocol == config.ProtocolHTTP || svc.Protocol == config.ProtocolHTTPS {
		inst.l7handler = proxy.NewHTTPConnHandler(svc, logger)
	}

	return inst, nil
}

func (inst *serviceInstance) start() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", inst.cfg.Port))
	if err != nil {
		return fmt.Errorf("listen port %d (service %q): %w",
			inst.cfg.Port, inst.cfg.ID, err)
	}
	inst.listener = ln

	ctx, cancel := context.WithCancel(context.Background())
	inst.cancel = cancel
	inst.proberCtx = ctx

	// Activate background health probing for this service's endpoints.
	state := inst.state.Load()
	state.dialer.Pool().StartProber(ctx, defaultProbeInterval)

	inst.logger.Info("service started",
		"protocol", inst.cfg.Protocol,
		"target_idc", inst.cfg.TargetIDC,
	)

	inst.wg.Add(1)
	go inst.acceptLoop(ctx)
	return nil
}

// stop closes the listener and waits for in-flight connections with a timeout.
func (inst *serviceInstance) stop() {
	if inst.cancel != nil {
		inst.cancel()
	}
	if inst.listener != nil {
		inst.listener.Close()
	}

	// Wait with timeout — prevents long-lived connections from blocking shutdown.
	done := make(chan struct{})
	go func() {
		inst.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		// All connections drained cleanly.
	case <-time.After(shutdownTimeout):
		inst.logger.Warn("service shutdown timeout, some connections may not have drained",
			"timeout", shutdownTimeout)
	}
	inst.logger.Info("service stopped")
}

func (inst *serviceInstance) reloadGroups(groups []config.RouteGroup) {
	hops := config.SortGroups(groups)
	if len(hops) == 0 {
		inst.logger.Warn("reload: no hops in new groups, keeping current state")
		return
	}
	pool := tunnel.NewHopPool(hops, nil, inst.logger)
	// Start prober for the new pool using the instance's context.
	if inst.proberCtx != nil {
		pool.StartProber(inst.proberCtx, defaultProbeInterval)
	}
	inst.state.Store(&serviceState{
		dialer:    tunnel.NewDialer(pool, inst.frameSize, inst.psk, inst.logger),
		targetIDC: inst.cfg.TargetIDC,
	})
	inst.logger.Info("service routes reloaded", "hops", len(hops))
}

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

func (inst *serviceInstance) handleConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
	}()

	clientAddr, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
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
// ─────────────────────────────────────────────────────────────────────────────

type AccessNode struct {
	frameSize int64
	psk       string // PSK passed to dialers
	logger    *slog.Logger

	mu       sync.Mutex
	services map[string]*serviceInstance
}

var _ Node = (*AccessNode)(nil)

// NewAccessNode constructs an AccessNode. psk is the pre-shared key for
// tunnel authentication (empty = disabled).
func NewAccessNode(cfg *config.AccessConfig, psk string, logger *slog.Logger) (*AccessNode, error) {
	frameSize := cfg.MaxFrameSize
	if frameSize == 0 {
		frameSize = 64 * 1024
	}
	n := &AccessNode{
		frameSize: frameSize,
		psk:       psk,
		logger:    logger,
		services:  make(map[string]*serviceInstance),
	}

	for _, svc := range cfg.Services {
		inst, err := newServiceInstance(svc, n.frameSize, psk, logger)
		if err != nil {
			return nil, err
		}
		n.services[svc.ID] = inst
	}
	return n, nil
}

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

func (n *AccessNode) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, inst := range n.services {
		inst.stop()
	}
}

func (n *AccessNode) Reload(rc ReloadableConfig) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	incoming := make(map[string]config.ServiceConfig, len(rc.AccessServices))
	for _, svc := range rc.AccessServices {
		incoming[svc.ID] = svc
	}

	// Stop removed services.
	for id, inst := range n.services {
		if _, exists := incoming[id]; !exists {
			n.logger.Info("access reload: stopping removed service", "service", id)
			inst.stop()
			delete(n.services, id)
		}
	}

	// Update existing / start new services.
	for id, svc := range incoming {
		if inst, running := n.services[id]; running {
			if inst.cfg.Port != svc.Port {
				n.logger.Warn("access reload: port change requires restart, ignored",
					"service", id, "old", inst.cfg.Port, "new", svc.Port)
			}
			if inst.cfg.Protocol != svc.Protocol {
				n.logger.Warn("access reload: protocol change requires restart, ignored",
					"service", id, "old", inst.cfg.Protocol, "new", svc.Protocol)
			}
			if groups, ok := rc.Groups[id]; ok {
				inst.reloadGroups(groups)
			}
		} else {
			n.logger.Info("access reload: starting new service", "service", id)
			inst, err := newServiceInstance(svc, n.frameSize, n.psk, n.logger)
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
