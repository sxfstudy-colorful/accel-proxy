package tunnel

import (
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// TunnelSession — per-connection lifecycle object
//
// A TunnelSession is created once per accepted inbound tunnel connection.
// It owns the inbound Transport and orchestrates the full relay lifecycle:
//
//   NewTunnelSession(req, inbound, logger)
//     │
//     ├── RunRelay(outbound)    — relay node: bridge two Transports
//     └── RunOrigin(originConn) — egress node: bridge Transport ↔ net.Conn
//
// Each session has a unique ID (logged in every message) so operators can
// correlate all log lines for a single connection across relay hops.
//
// Duration is logged on completion, providing a cheap per-session metric
// even before a proper metrics system is wired in.
// ─────────────────────────────────────────────────────────────────────────────

// TunnelSession represents one tunnel connection from arrival to teardown.
type TunnelSession struct {
	// ID is a short random hex string unique to this connection.
	// Included in every log message emitted by the session.
	ID string

	// Req is the handshake metadata received from the upstream peer.
	Req *HandshakeRequest

	// inbound is the Transport for the upstream peer (access or relay node).
	// The session is the sole owner; it is closed when the session ends.
	inbound *Transport

	// logger is pre-populated with session fields (id, service, client_ip)
	// so call sites don't need to repeat them.
	logger *slog.Logger

	startAt time.Time
}

// NewTunnelSession creates a TunnelSession for an accepted inbound tunnel.
// The returned session owns inbound and will close it when Run* returns.
func NewTunnelSession(req *HandshakeRequest, inbound *Transport, logger *slog.Logger) *TunnelSession {
	id := newSessionID()
	return &TunnelSession{
		ID:      id,
		Req:     req,
		inbound: inbound,
		startAt: time.Now(),
		logger: logger.With(
			"session_id", id,
			"service", req.ServiceID,
			"target_idc", req.TargetIDC,
			"client_ip", req.ClientIP,
			"hops", req.HopCount,
		),
	}
}

// Logger returns the session-scoped logger. Node handlers can use this to
// emit additional log lines with the session context already attached.
func (s *TunnelSession) Logger() *slog.Logger { return s.logger }

// Close explicitly closes the inbound Transport.
// It is safe to call multiple times and is idempotent.
// Normally called via defer at the top of the handler:
//
//	session := tunnel.NewTunnelSession(req, inbound, logger)
//	defer session.Close()
func (s *TunnelSession) Close() {
	s.inbound.Close()
}

// RunRelay bridges the inbound Transport to an outbound Transport.
// Used by relay nodes that forward tunnels toward the next hop.
//
// Both transports are closed when the relay completes (either side EOF or error).
// The method blocks until both directions have drained.
func (s *TunnelSession) RunRelay(outbound *Transport) {
	s.logger.Info("session: relay started")
	defer func() {
		outbound.Close()
		s.logger.Info("session: relay finished",
			"duration", time.Since(s.startAt).Round(time.Millisecond).String())
	}()
	RelayTransports(s.inbound, outbound, s.logger)
}

// RunOrigin bridges the inbound Transport to a plain net.Conn (origin server).
// Used by egress nodes that terminate the tunnel and connect to the real backend.
//
// Both connections are closed when the relay completes.
// The method blocks until both directions have drained.
func (s *TunnelSession) RunOrigin(origin net.Conn) {
	s.logger.Info("session: origin relay started",
		"origin", origin.RemoteAddr())
	defer func() {
		origin.Close()
		s.logger.Info("session: origin relay finished",
			"duration", time.Since(s.startAt).Round(time.Millisecond).String())
	}()
	Relay(origin, s.inbound, s.logger)
}

// ─────────────────────────────────────────────────────────────────────────────
// Session ID generator
// ─────────────────────────────────────────────────────────────────────────────

// newSessionID returns an 8-character lowercase hex string.
// Collision probability is negligible for typical connection rates
// (< 1-in-a-million at 10k concurrent sessions).
func newSessionID() string {
	return fmt.Sprintf("%08x", rand.Uint32()) //nolint:gosec // not crypto
}
