package node

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
	"github.com/sxfstudy-colorful/accel-proxy/internal/tunnel"
)

// ─────────────────────────────────────────────────────────────────────────────
// EgressNode
// ─────────────────────────────────────────────────────────────────────────────

type originEntry struct {
	host        string
	port        int
	useTLS      bool
	dialTimeout time.Duration
}

func (e *originEntry) addr() string {
	return fmt.Sprintf("%s:%d", e.host, e.port)
}

type originTable map[string]*originEntry

type EgressNode struct {
	baseNode
	nodeID  string
	nodeIDC string

	origins  atomic.Pointer[originTable]
	reloadMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
}

var _ Node = (*EgressNode)(nil)
var _ CertReloader = (*EgressNode)(nil)

func NewEgressNode(nodeID, nodeIDC string, cfg *config.EgressConfig, logger *slog.Logger) *EgressNode {
	ctx, cancel := context.WithCancel(context.Background())
	n := &EgressNode{
		baseNode: baseNode{logger: logger},
		nodeID:   nodeID,
		nodeIDC:  nodeIDC,
		ctx:      ctx,
		cancel:   cancel,
	}
	tbl := buildOriginTable(cfg.Services)
	n.origins.Store(&tbl)
	n.server = tunnel.NewServer(&cfg.Tunnel, nodeID, n.handleTunnel, logger)
	return n
}

func (n *EgressNode) Start() error {
	n.logger.Info("egress node starting",
		"id", n.nodeID,
		"idc", n.nodeIDC,
		"addr", n.server.ListenAddr(),
	)
	return n.startServer("egress:" + n.nodeID)
}

func (n *EgressNode) Stop() {
	n.cancel() // Cancel context to abort pending origin dials.
	n.stopServer()
}

func (n *EgressNode) ReloadCert() error { return n.reloadCert() }

func (n *EgressNode) Reload(rc ReloadableConfig) error {
	if len(rc.Origins) == 0 {
		return nil
	}
	n.reloadMu.Lock()
	defer n.reloadMu.Unlock()

	current := *n.origins.Load()
	next := make(originTable, len(current))
	for k, v := range current {
		next[k] = v
	}
	for svcID, upd := range rc.Origins {
		dt := upd.DialTimeout
		if dt == 0 {
			dt = 10 * time.Second
		}
		next[svcID] = &originEntry{
			host: upd.Host, port: upd.Port,
			useTLS: upd.TLS, dialTimeout: dt,
		}
	}
	n.origins.Store(&next)
	n.logger.Info("egress: origins hot-reloaded", "updated_services", len(rc.Origins))
	return nil
}

func (n *EgressNode) handleTunnel(req *tunnel.HandshakeRequest, inbound *tunnel.Transport) {
	session := tunnel.NewTunnelSession(req, inbound, n.logger)
	defer session.Close()

	if req.TargetIDC != n.nodeIDC {
		session.Logger().Error("egress: target IDC mismatch — routing error",
			"want", n.nodeIDC, "got", req.TargetIDC)
		return
	}

	origins := *n.origins.Load()
	entry, ok := origins[req.ServiceID]
	if !ok {
		session.Logger().Error("egress: unknown service")
		return
	}

	origin, err := dialOrigin(n.ctx, entry.addr(), entry.useTLS || req.Protocol == "https", entry.dialTimeout)
	if err != nil {
		session.Logger().Error("egress: dial origin failed", "origin", entry.addr(), "err", err)
		return
	}
	session.RunOrigin(origin)
}

func buildOriginTable(services []config.ServiceConfig) originTable {
	tbl := make(originTable, len(services))
	for i := range services {
		svc := &services[i]
		dt := svc.Origin.DialTimeout
		if dt == 0 {
			dt = 10 * time.Second
		}
		tbl[svc.ID] = &originEntry{
			host: svc.Origin.Host, port: svc.Origin.Port,
			useTLS: svc.Origin.TLS, dialTimeout: dt,
		}
	}
	return tbl
}

// dialOrigin connects to the real origin server.
// Uses ctx for cancellation support during graceful shutdown.
func dialOrigin(ctx context.Context, addr string, useTLS bool, timeout time.Duration) (net.Conn, error) {
	dialCtx, dialCancel := context.WithTimeout(ctx, timeout)
	defer dialCancel()

	var d net.Dialer
	conn, err := d.DialContext(dialCtx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tcp dial %q: %w", addr, err)
	}

	if useTLS {
		if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("set deadline %q: %w", addr, err)
		}

		host, _, _ := net.SplitHostPort(addr)
		tlsConn := tls.Client(conn, &tls.Config{ServerName: host})
		if err := tlsConn.HandshakeContext(dialCtx); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("tls handshake %q: %w", addr, err)
		}

		if err := tlsConn.SetDeadline(time.Time{}); err != nil {
			_ = tlsConn.Close()
			return nil, fmt.Errorf("clear deadline %q: %w", addr, err)
		}
		return tlsConn, nil
	}
	return conn, nil
}
