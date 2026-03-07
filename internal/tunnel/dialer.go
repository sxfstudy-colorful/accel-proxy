package tunnel

import (
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/gorilla/websocket"

	"github.com/accel-proxy/internal/config"
)

// Dialer dials the next hop WebSocket tunnel endpoint.
// It implements simple round-robin load balancing over multiple next-hop URLs.
type Dialer struct {
	nextHops  []string
	counter   atomic.Uint64
	frameSize int64
	logger    *slog.Logger
}

// NewDialer creates a Dialer for the given list of next-hop URLs.
func NewDialer(nextHops []string, frameSize int64, logger *slog.Logger) *Dialer {
	return &Dialer{
		nextHops:  nextHops,
		frameSize: frameSize,
		logger:    logger,
	}
}

// Dial opens a tunnel to the next hop, performs the handshake, and returns a
// ready-to-use Transport.
func (d *Dialer) Dial(req *HandshakeRequest) (*Transport, error) {
	url := d.pickNextHop()

	conn, _, err := dialWebSocket(url)
	if err != nil {
		return nil, fmt.Errorf("dial next-hop %q: %w", url, err)
	}

	if _, err := SendHandshake(conn, req); err != nil {
		conn.Close()
		return nil, fmt.Errorf("handshake with %q: %w", url, err)
	}

	return NewTransport(conn, d.frameSize, d.logger), nil
}

// DialWithFallback tries each next-hop in order on failure.
func (d *Dialer) DialWithFallback(req *HandshakeRequest) (*Transport, error) {
	start := int(d.counter.Add(1)-1) % len(d.nextHops)
	for i := range d.nextHops {
		url := d.nextHops[(start+i)%len(d.nextHops)]
		conn, _, err := dialWebSocket(url)
		if err != nil {
			d.logger.Warn("next-hop dial failed, trying next", "url", url, "err", err)
			continue
		}
		if _, err := SendHandshake(conn, req); err != nil {
			conn.Close()
			d.logger.Warn("next-hop handshake failed, trying next", "url", url, "err", err)
			continue
		}
		return NewTransport(conn, d.frameSize, d.logger), nil
	}
	return nil, fmt.Errorf("all next-hops unavailable (%d tried)", len(d.nextHops))
}

func (d *Dialer) pickNextHop() string {
	idx := d.counter.Add(1) - 1
	return d.nextHops[idx%uint64(len(d.nextHops))]
}

func dialWebSocket(rawURL string) (*websocket.Conn, *http.Response, error) {
	dialer := websocket.Dialer{
		HandshakeTimeout: tunnelDialTimeout,
	}
	return dialer.Dial(rawURL, http.Header{
		"User-Agent": []string{"accel-proxy-tunnel/1"},
	})
}

// NewDialerFromService builds a Dialer from a ServiceConfig.
func NewDialerFromService(svc *config.ServiceConfig, frameSize int64, logger *slog.Logger) *Dialer {
	return NewDialer(svc.NextHops, frameSize, logger)
}
