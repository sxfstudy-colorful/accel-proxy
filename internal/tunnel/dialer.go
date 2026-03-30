package tunnel

import (
	"log/slog"
	"time"
)

const tunnelDialTimeout = 10 * time.Second

// Dialer dials next-hop WebSocket tunnel endpoints via a HopPool.
type Dialer struct {
	pool      *HopPool
	frameSize int64
	psk       string // pre-shared key for HMAC signing; empty = unsigned mode
	logger    *slog.Logger
}

// NewDialer creates a Dialer backed by the given HopPool.
func NewDialer(pool *HopPool, frameSize int64, psk string, logger *slog.Logger) *Dialer {
	return &Dialer{pool: pool, frameSize: frameSize, psk: psk, logger: logger}
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
	addr := ep.Addr()

	conn, err := dialWebSocketAddr(addr)
	if err != nil {
		d.logger.Warn("next-hop dial failed, trying next candidate",
			"identity", addr.Identity(),
			"dial_addr", addr.DialAddr(),
			"sni", addr.SNIHost(),
			"idc", req.TargetIDC,
			"err", err,
		)
		return nil, time.Since(start), err
	}

	// Sign the request before sending if PSK is configured.
	SignRequest(req, d.psk)

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
		"sni", addr.SNIHost(),
		"idc", req.TargetIDC,
		"service", req.ServiceID,
		"latency_ms", elapsed.Milliseconds(),
	)
	return NewTransport(conn, d.frameSize, d.logger), elapsed, nil
}
