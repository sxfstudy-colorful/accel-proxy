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
//
// 职责：终结隧道，连接本机房源站。
//
// 热重载：
//   originTable 通过 atomic.Pointer 持有，Reload() 原子替换。
//   新连接立即使用新源站地址；已建立的连接不受影响。
// ─────────────────────────────────────────────────────────────────────────────

// originEntry holds the dialing parameters for one service's origin.
type originEntry struct {
	host        string
	port        int
	useTLS      bool
	dialTimeout time.Duration
}

func (e *originEntry) addr() string {
	return fmt.Sprintf("%s:%d", e.host, e.port)
}

// originTable maps serviceID → originEntry. Immutable once stored.
type originTable map[string]*originEntry

// EgressNode terminates tunnels and connects to real origin servers.
type EgressNode struct {
	baseNode
	cfg     *config.Config
	origins atomic.Pointer[originTable]
	// reloadMu serialises concurrent Reload() calls.
	reloadMu sync.Mutex
}

var _ Node = (*EgressNode)(nil)
var _ CertReloader = (*EgressNode)(nil)

// NewEgressNode constructs an EgressNode.
func NewEgressNode(cfg *config.Config, logger *slog.Logger) *EgressNode {
	n := &EgressNode{
		baseNode: baseNode{logger: logger},
		cfg:      cfg,
	}
	tbl := buildOriginTable(cfg)
	n.origins.Store(&tbl)
	n.server = tunnel.NewServer(&cfg.Tunnel, cfg.Node.ID, n.handleTunnel, logger)
	return n
}

// Start begins accepting inbound tunnel connections (non-blocking).
func (n *EgressNode) Start() error {
	n.logger.Info("egress node starting",
		"id", n.cfg.Node.ID,
		"idc", n.cfg.Node.IDC,
		"addr", n.cfg.Tunnel.ListenAddr,
	)
	return n.startServer("egress:" + n.cfg.Node.ID)
}

// Stop shuts down the egress node gracefully.
func (n *EgressNode) Stop() { n.stopServer() }

// ReloadCert triggers an immediate TLS certificate reload.
func (n *EgressNode) ReloadCert() error { return n.reloadCert() }

// Reload applies updated origin addresses without dropping connections.
// In-flight sessions keep using the old entry; new sessions use the new one.
func (n *EgressNode) Reload(rc ReloadableConfig) error {
	if len(rc.Origins) == 0 {
		return nil
	}

	n.reloadMu.Lock()
	defer n.reloadMu.Unlock()

	current := *n.origins.Load()
	next := make(originTable, len(current))
	for id, e := range current {
		next[id] = e
	}
	for svcID, upd := range rc.Origins {
		dt := upd.DialTimeout
		if dt == 0 {
			dt = 10 * time.Second
		}
		next[svcID] = &originEntry{
			host:        upd.Host,
			port:        upd.Port,
			useTLS:      upd.TLS,
			dialTimeout: dt,
		}
	}

	n.origins.Store(&next)
	n.logger.Info("egress: origins hot-reloaded", "updated_services", len(rc.Origins))
	return nil
}

// handleTunnel is called for each accepted inbound tunnel.
func (n *EgressNode) handleTunnel(req *tunnel.HandshakeRequest, tun *tunnel.Transport) {
	defer tun.Close()

	// ── 1. IDC 校验 ────────────────────────────────────────────────────────
	if req.TargetIDC != n.cfg.Node.IDC {
		n.logger.Error("egress: target IDC mismatch — routing error",
			"want", n.cfg.Node.IDC,
			"got", req.TargetIDC,
			"service", req.ServiceID,
		)
		return
	}

	// ── 2. 查源站配置（快照，hot-reload 不影响本次连接）─────────────────────
	origins := *n.origins.Load()
	entry, ok := origins[req.ServiceID]
	if !ok {
		n.logger.Error("egress: unknown service", "service_id", req.ServiceID)
		return
	}

	useTLS := entry.useTLS || req.Protocol == "https"

	// ── 3. 拨连源站 ────────────────────────────────────────────────────────
	origin, err := dialOrigin(entry.addr(), useTLS, entry.dialTimeout)
	if err != nil {
		n.logger.Error("egress: dial origin failed",
			"service", req.ServiceID,
			"origin", entry.addr(),
			"err", err,
		)
		return
	}
	defer origin.Close()

	n.logger.Info("egress: origin connected",
		"service", req.ServiceID,
		"origin", entry.addr(),
		"client_ip", req.ClientIP,
		"hops", req.HopCount,
	)

	// ── 4. 桥接 tunnel ↔ origin ────────────────────────────────────────────
	tunnel.Relay(origin, tun, n.logger)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func buildOriginTable(cfg *config.Config) originTable {
	tbl := make(originTable, len(cfg.Services))
	for i := range cfg.Services {
		svc := &cfg.Services[i]
		dt := svc.Origin.DialTimeout
		if dt == 0 {
			dt = 10 * time.Second
		}
		tbl[svc.ID] = &originEntry{
			host:        svc.Origin.Host,
			port:        svc.Origin.Port,
			useTLS:      svc.Origin.TLS,
			dialTimeout: dt,
		}
	}
	return tbl
}

func dialOrigin(addr string, useTLS bool, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("tcp dial %q: %w", addr, err)
	}
	if useTLS {
		host, _, _ := net.SplitHostPort(addr)
		tlsConn := tls.Client(conn, &tls.Config{ServerName: host})
		if err := tlsConn.HandshakeContext(context.Background()); err != nil {
			conn.Close()
			return nil, fmt.Errorf("tls handshake %q: %w", addr, err)
		}
		return tlsConn, nil
	}
	return conn, nil
}
