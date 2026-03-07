package tunnel

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
)

// ─────────────────────────────────────────────────────────────────────────────
// HopEndpoint — one next-hop candidate with health and latency state
// ─────────────────────────────────────────────────────────────────────────────

// HopEndpoint pairs a config.HopAddr with its observed health and latency.
// All exported methods are safe for concurrent access.
type HopEndpoint struct {
	// addr is the current address configuration for this endpoint.
	// May be hot-updated (e.g. IP change on reload) while identity is stable.
	// Protected by addrMu; always read via Addr().
	addrMu sync.RWMutex
	addr   config.HopAddr

	// alive is false when the prober or DialWithFallback has detected this
	// endpoint is unreachable. Skipped by Pick() unless all are dead.
	alive atomic.Bool

	// latencyEWMA is an EWMA of successful dial+handshake RTT in milliseconds.
	// sync.Mutex because float64 is not atomically writable on all platforms.
	latMu          sync.Mutex
	latencyEWMA    float64
	latencySamples int
}

func newHopEndpoint(addr config.HopAddr) *HopEndpoint {
	e := &HopEndpoint{addr: addr, latencyEWMA: math.MaxFloat64}
	e.alive.Store(true)
	return e
}

// Addr returns a snapshot of the current address configuration.
func (e *HopEndpoint) Addr() config.HopAddr {
	e.addrMu.RLock()
	defer e.addrMu.RUnlock()
	return e.addr
}

func (e *HopEndpoint) updateAddr(a config.HopAddr) {
	e.addrMu.Lock()
	e.addr = a
	e.addrMu.Unlock()
}

// Alive reports whether this endpoint is currently considered healthy.
func (e *HopEndpoint) Alive() bool { return e.alive.Load() }

// LatencyEWMA returns the current latency estimate in milliseconds.
// Returns math.MaxFloat64 if no successful sample has been recorded yet.
func (e *HopEndpoint) LatencyEWMA() float64 {
	e.latMu.Lock()
	defer e.latMu.Unlock()
	return e.latencyEWMA
}

const ewmaAlpha = 0.2

func (e *HopEndpoint) recordLatency(d time.Duration) {
	ms := float64(d.Milliseconds())
	e.latMu.Lock()
	defer e.latMu.Unlock()
	if e.latencySamples == 0 {
		e.latencyEWMA = ms
	} else {
		e.latencyEWMA = ewmaAlpha*ms + (1-ewmaAlpha)*e.latencyEWMA
	}
	e.latencySamples++
}

// ─────────────────────────────────────────────────────────────────────────────
// HopSelector — strategy interface
// ─────────────────────────────────────────────────────────────────────────────

// HopSelector picks an ordered list of endpoints to try on each dial.
// Implementations must be safe for concurrent use.
type HopSelector interface {
	Pick(endpoints []*HopEndpoint) []*HopEndpoint
}

// RoundRobinSelector distributes connections evenly across alive endpoints.
// Falls back to the full list if all endpoints are currently marked dead.
type RoundRobinSelector struct {
	counter atomic.Uint64
}

func (s *RoundRobinSelector) Pick(endpoints []*HopEndpoint) []*HopEndpoint {
	alive := aliveOnly(endpoints)
	if len(alive) == 0 {
		alive = endpoints
	}
	n := len(alive)
	start := int(s.counter.Add(1)-1) % n
	result := make([]*HopEndpoint, n)
	for i := range alive {
		result[i] = alive[(start+i)%n]
	}
	return result
}

// LatencySelector picks the endpoint with the lowest EWMA latency among alive
// endpoints. Falls back to all alive endpoints (unordered) when no latency
// data is available yet.
type LatencySelector struct{}

func (s *LatencySelector) Pick(endpoints []*HopEndpoint) []*HopEndpoint {
	alive := aliveOnly(endpoints)
	if len(alive) == 0 {
		alive = endpoints
	}
	// Insertion sort — N is tiny in practice (typically 2–4 endpoints).
	sorted := make([]*HopEndpoint, len(alive))
	copy(sorted, alive)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].LatencyEWMA() < sorted[j-1].LatencyEWMA(); j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	return sorted
}

func aliveOnly(eps []*HopEndpoint) []*HopEndpoint {
	out := make([]*HopEndpoint, 0, len(eps))
	for _, e := range eps {
		if e.Alive() {
			out = append(out, e)
		}
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// HopPool
// ─────────────────────────────────────────────────────────────────────────────

// HopPool manages a set of HopEndpoints with a selection strategy and optional
// background probing.
//
// Lifecycle:
//
//	pool := NewHopPool(addrs, selector, logger)
//	pool.StartProber(ctx, interval)     // optional background health probe
//	dialer := NewDialer(pool, frameSize, logger)
//	pool.UpdateEndpoints(newAddrs)      // hot-reload: preserves health/latency
type HopPool struct {
	mu        sync.RWMutex
	endpoints []*HopEndpoint

	selector HopSelector
	logger   *slog.Logger
}

// NewHopPool creates a HopPool from a list of HopAddrs.
// If selector is nil, RoundRobinSelector is used.
func NewHopPool(addrs []config.HopAddr, selector HopSelector, logger *slog.Logger) *HopPool {
	if selector == nil {
		selector = &RoundRobinSelector{}
	}
	p := &HopPool{selector: selector, logger: logger}
	p.endpoints = makeEndpoints(addrs)
	return p
}

// Addrs returns a snapshot of the current endpoint addresses.
func (p *HopPool) Addrs() []config.HopAddr {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]config.HopAddr, len(p.endpoints))
	for i, e := range p.endpoints {
		out[i] = e.Addr()
	}
	return out
}

// UpdateEndpoints hot-reloads the endpoint list.
//
// Matching is by Identity() (IP+Port+TunnelPath):
//   - Same identity, same fields → reuse endpoint (alive + latency intact)
//   - Same identity, changed fields (e.g. Host, TLS) → update addr in-place,
//     preserving health and latency history
//   - New identity → fresh endpoint (alive=true, no latency history)
//   - Removed identity → discarded
func (p *HopPool) UpdateEndpoints(newAddrs []config.HopAddr) {
	p.mu.Lock()
	defer p.mu.Unlock()

	existing := make(map[string]*HopEndpoint, len(p.endpoints))
	for _, e := range p.endpoints {
		existing[e.addr.Identity()] = e
	}

	next := make([]*HopEndpoint, len(newAddrs))
	for i, a := range newAddrs {
		if e, ok := existing[a.Identity()]; ok {
			if !e.addr.Equal(a) {
				e.updateAddr(a)
				p.logger.Debug("hop addr updated",
					"identity", a.Identity(), "host", a.Host, "tls", a.TLS)
			}
			next[i] = e
		} else {
			next[i] = newHopEndpoint(a)
			p.logger.Debug("hop added", "identity", a.Identity())
		}
	}
	p.endpoints = next
	p.logger.Info("hop pool updated", "count", len(next))
}

// pick returns an ordered list of endpoints for the Dialer to try.
func (p *HopPool) pick() []*HopEndpoint {
	p.mu.RLock()
	eps := make([]*HopEndpoint, len(p.endpoints))
	copy(eps, p.endpoints)
	p.mu.RUnlock()
	return p.selector.Pick(eps)
}

// reportResult updates health and latency after a dial attempt.
func (p *HopPool) reportResult(e *HopEndpoint, success bool, latency time.Duration) {
	addr := e.Addr()
	if success {
		if !e.alive.Load() {
			p.logger.Info("hop recovered", "identity", addr.Identity(), "host", addr.Host)
		}
		e.alive.Store(true)
		e.recordLatency(latency)
	} else {
		if e.alive.Load() {
			p.logger.Warn("hop marked dead", "identity", addr.Identity(), "host", addr.Host)
		}
		e.alive.Store(false)
	}
}

func (p *HopPool) size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.endpoints)
}

// ─────────────────────────────────────────────────────────────────────────────
// Background prober
// ─────────────────────────────────────────────────────────────────────────────

// StartProber launches a background goroutine that probes all endpoints at the
// TCP layer (no WS handshake, no user data) on the given interval.
// Results update each endpoint's alive flag and latency EWMA.
// The goroutine stops when ctx is cancelled.
func (p *HopPool) StartProber(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.probeAll()
			}
		}
	}()
}

func (p *HopPool) probeAll() {
	p.mu.RLock()
	eps := make([]*HopEndpoint, len(p.endpoints))
	copy(eps, p.endpoints)
	p.mu.RUnlock()

	var wg sync.WaitGroup
	for _, e := range eps {
		wg.Add(1)
		go func(ep *HopEndpoint) {
			defer wg.Done()
			p.probeOne(ep)
		}(e)
	}
	wg.Wait()
}

// probeOne probes at TCP level (not WebSocket) — faster, lighter, and works
// for nodes that require authentication at the WS layer.
func (p *HopPool) probeOne(e *HopEndpoint) {
	addr := e.Addr()
	dialAddr := addr.DialAddr()

	start := time.Now()
	conn, err := net.DialTimeout("tcp", dialAddr, tunnelDialTimeout)
	elapsed := time.Since(start)

	if err != nil {
		p.reportResult(e, false, 0)
		p.logger.Debug("probe failed", "identity", addr.Identity(), "addr", dialAddr, "err", err)
		return
	}
	conn.Close()
	p.reportResult(e, true, elapsed)
	p.logger.Debug("probe ok",
		"identity", addr.Identity(), "addr", dialAddr,
		"latency_ms", elapsed.Milliseconds())
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func makeEndpoints(addrs []config.HopAddr) []*HopEndpoint {
	eps := make([]*HopEndpoint, len(addrs))
	for i, a := range addrs {
		eps[i] = newHopEndpoint(a)
	}
	return eps
}

func hopPoolError(idc, serviceID string, n int) error {
	return fmt.Errorf("all %d next-hop(s) unavailable for idc=%q service=%q", n, idc, serviceID)
}

// dialWebSocketAddr dials a WebSocket connection using addr.DialAddr() for TCP
// and addr.WsURL() for the HTTP upgrade. This lets us connect to a bare IP
// while presenting the correct Host header and TLS SNI.
func dialWebSocketAddr(addr config.HopAddr) (*websocket.Conn, error) {
	dialAddr := addr.DialAddr()
	wsURL := addr.WsURL()

	d := websocket.Dialer{
		HandshakeTimeout: tunnelDialTimeout,
		// NetDial bypasses the WS library's DNS resolution so we connect to
		// IP:Port directly even when wsURL contains a hostname.
		NetDial: func(network, _ string) (net.Conn, error) {
			return net.DialTimeout(network, dialAddr, tunnelDialTimeout)
		},
	}

	headers := http.Header{
		"User-Agent": []string{"accel-proxy-tunnel/1"},
	}
	if addr.Host != "" {
		headers.Set("Host", addr.Host)
	}

	conn, _, err := d.Dial(wsURL, headers)
	return conn, err
}
