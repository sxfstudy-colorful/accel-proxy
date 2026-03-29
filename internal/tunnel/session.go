package tunnel

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net"
	"time"
)

// TunnelSession represents one tunnel connection from arrival to teardown.
type TunnelSession struct {
	ID      string
	Req     *HandshakeRequest
	inbound *Transport
	logger  *slog.Logger
	startAt time.Time
}

// NewTunnelSession creates a TunnelSession for an accepted inbound tunnel.
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

// Logger returns the session-scoped logger.
func (s *TunnelSession) Logger() *slog.Logger { return s.logger }

// Close explicitly closes the inbound Transport. Idempotent.
func (s *TunnelSession) Close() {
	s.inbound.Close()
}

// RunRelay bridges the inbound Transport to an outbound Transport.
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
func (s *TunnelSession) RunOrigin(origin net.Conn) {
	s.logger.Info("session: origin relay started",
		"origin", origin.RemoteAddr())

	defer func() {
		_ = origin.Close()
		s.logger.Info("session: origin relay finished",
			"duration", time.Since(s.startAt).Round(time.Millisecond).String())
	}()

	Relay(origin, s.inbound, s.logger)
}

// newSessionID returns an 8-character hex string using crypto/rand.
func newSessionID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Extremely unlikely; fall back to zero ID rather than crashing.
		return "00000000"
	}
	return hex.EncodeToString(b[:])
}
