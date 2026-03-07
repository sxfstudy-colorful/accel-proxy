package tunnel

import (
	"log/slog"
	"time"
)

const tunnelDialTimeout = 10 * time.Second

// Dialer dials next-hop WebSocket tunnel endpoints via a HopPool.
//
// The HopPool owns endpoint selection (round-robin, latency-based), health
// state, and hot-reload. The Dialer's responsibilities are:
//  1. Ask the pool which endpoints to try (pick).
//  2. Attempt dial + handshake on each candidate in order.
//  3. Feed results back to the pool (reportResult).
//
// # Fallback safety boundary
//
// DialWithFallback is the ONLY place where retrying across multiple candidates
// is permitted — no user data has been sent yet, only routing metadata in
// HandshakeRequest. Once it returns a Transport, the pre-data phase ends.
// Transport.dead then enforces the no-retry contract for data relay.
type Dialer struct {
	pool      *HopPool
	frameSize int64
	logger    *slog.Logger
}

// NewDialer creates a Dialer backed by the given HopPool.
func NewDialer(pool *HopPool, frameSize int64, logger *slog.Logger) *Dialer {
	return &Dialer{pool: pool, frameSize: frameSize, logger: logger}
}

// Pool returns the underlying HopPool.
func (d *Dialer) Pool() *HopPool { return d.pool }

// DialWithFallback establishes a tunnel to one of the pool's healthy endpoints.
func (d *Dialer) DialWithFallback(req *HandshakeRequest) (*Transport, error) {
	for _, ep := range d.pool.pick() {
		tun, latency, err := d.tryEndpoint(ep, req)
		d.pool.reportResult(ep, err == nil, latency)
		if err == nil {
			return tun, nil
		}
	}
	return nil, hopPoolError(req.TargetIDC, req.ServiceID, d.pool.size())
}

// tryEndpoint attempts a single dial + handshake against ep.
func (d *Dialer) tryEndpoint(ep *HopEndpoint, req *HandshakeRequest) (*Transport, time.Duration, error) {
	start := time.Now()
	addr := ep.Addr() // snapshot: safe even if hot-reload concurrently updates addr

	// ── Step 1: TCP + WebSocket dial ─────────────────────────────────────
	// Uses addr.DialAddr() for TCP (IP:Port) and addr.WsURL() for HTTP Host/SNI.
	// Failure: no bytes sent, safe to try next candidate.
	conn, err := dialWebSocketAddr(addr)
	if err != nil {
		d.logger.Warn("next-hop dial failed, trying next candidate",
			"identity", addr.Identity(),
			"dial_addr", addr.DialAddr(),
			"host", addr.Host,
			"idc", req.TargetIDC,
			"err", err,
		)
		return nil, time.Since(start), err
	}

	// ── Step 2: tunnel handshake (routing metadata only, no user data) ───
	// Failure: only HandshakeRequest bytes sent. Safe to close and try next.
	if _, err := SendHandshake(conn, req); err != nil {
		conn.Close()
		d.logger.Warn("next-hop handshake failed, trying next candidate",
			"identity", addr.Identity(),
			"idc", req.TargetIDC,
			"err", err,
		)
		return nil, time.Since(start), err
	}

	elapsed := time.Since(start)
	d.logger.Debug("tunnel established",
		"identity", addr.Identity(),
		"host", addr.Host,
		"idc", req.TargetIDC,
		"service", req.ServiceID,
		"latency_ms", elapsed.Milliseconds(),
	)
	return NewTransport(conn, d.frameSize, d.logger), elapsed, nil
}


