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
// 构造：只依赖 *config.EgressConfig、nodeID、nodeIDC，与其他角色完全解耦。
// 热重载：originTable 通过 atomic.Pointer 持有，Reload() 原子替换。
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

// EgressNode terminates tunnels and connects to real origin servers.
type EgressNode struct {
	baseNode
	nodeID  string
	nodeIDC string

	origins  atomic.Pointer[originTable]
	reloadMu sync.Mutex
}

var _ Node         = (*EgressNode)(nil)
var _ CertReloader = (*EgressNode)(nil)

// NewEgressNode constructs an EgressNode from its dedicated role config.
func NewEgressNode(nodeID, nodeIDC string, cfg *config.EgressConfig, logger *slog.Logger) *EgressNode {
	n := &EgressNode{
		baseNode: baseNode{logger: logger},
		nodeID:   nodeID,
		nodeIDC:  nodeIDC,
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

func (n *EgressNode) Stop() { n.stopServer() }

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

	origin, err := dialOrigin(entry.addr(), entry.useTLS || req.Protocol == "https", entry.dialTimeout)
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
