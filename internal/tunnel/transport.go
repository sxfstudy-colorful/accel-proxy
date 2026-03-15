package tunnel

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultFrameSize    = 32 * 1024
	pingInterval        = 20 * time.Second
	pongWait            = 30 * time.Second
	writeDeadlineBuffer = 5 * time.Second
)

// ErrTransportDead is returned by Read and Write after a Transport has been
// permanently closed due to a write error or an explicit Close() call.
// Callers outside the tunnel package can use errors.Is to detect this condition.
var ErrTransportDead = errors.New("transport is dead")

// Transport wraps a *websocket.Conn and exposes it as an io.ReadWriteCloser.
//
// # Duplicate-prevention guarantees
//
// Write:
//   - Frames are written sequentially under writeMu; no two goroutines can
//     interleave frames on the wire.
//   - On the first write error the transport is immediately marked dead via
//     an atomic flag.  All subsequent Write calls return ErrTransportDead
//     without touching the connection, so no further data can be injected
//     after a partial failure.
//   - The caller (io.CopyBuffer) sees the error and stops reading from the
//     source; the source never retries, so no bytes are sent twice.
//
// Read:
//   - readPump is the SOLE consumer of incoming WebSocket frames. It writes
//     them into an io.Pipe (unbuffered, no seek). Read() drains from the read
//     end — each byte is consumed exactly once, never replayed.
//   - On read error readPump closes the pipe write-end, causing the next
//     Read() to return io.EOF, which terminates the relay goroutine cleanly.
type Transport struct {
	conn      *websocket.Conn
	frameSize int64

	// reader pipe: readPump (sole writer) → Read() callers (sole reader)
	pr *io.PipeReader
	pw *io.PipeWriter

	closeOnce sync.Once
	closed    chan struct{} // closed by markDead; unblocks pingPump
	writeMu   sync.Mutex

	// dead is set atomically to 1 on the first write error or Close().
	// Write() checks this before attempting any frame send.
	dead atomic.Int32

	logger *slog.Logger
}

// NewTransport creates a Transport around an already-handshaken WebSocket.
func NewTransport(conn *websocket.Conn, frameSize int64, logger *slog.Logger) *Transport {
	if frameSize <= 0 {
		frameSize = defaultFrameSize
	}
	pr, pw := io.Pipe()
	t := &Transport{
		conn:      conn,
		frameSize: frameSize,
		pr:        pr,
		pw:        pw,
		closed:    make(chan struct{}),
		logger:    logger,
	}
	conn.SetReadLimit(frameSize + 512)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	go t.readPump()
	go t.pingPump()
	return t
}

// Read implements io.Reader.
// Returns ErrTransportDead immediately if the transport has been permanently closed.
func (t *Transport) Read(p []byte) (int, error) {
	if t.dead.Load() == 1 {
		return 0, ErrTransportDead
	}
	return t.pr.Read(p)
}

// Write implements io.Writer.
//
// Atomicity guarantee: on the first write error the transport is marked dead
// before returning. All subsequent Write calls return ErrTransportDead without
// sending any data. This ensures no bytes can be re-sent after a partial failure.
func (t *Transport) Write(p []byte) (int, error) {
	if t.dead.Load() == 1 {
		return 0, ErrTransportDead
	}

	total := 0
	for len(p) > 0 {
		n := int(t.frameSize)
		if n > len(p) {
			n = len(p)
		}

		t.writeMu.Lock()
		if err := t.conn.SetWriteDeadline(time.Now().Add(writeDeadlineBuffer + pingInterval)); err != nil {
			t.writeMu.Unlock()
			t.markDead()
			return total, fmt.Errorf("set transport deadline err: %w", err)
		}

		err := t.conn.WriteMessage(websocket.BinaryMessage, p[:n])
		t.writeMu.Unlock()

		if err != nil {
			t.markDead()
			return total, fmt.Errorf("transport write err: %w", err)
		}
		p = p[n:]
		total += n
	}
	return total, nil
}

// Close shuts the transport down gracefully. Idempotent.
func (t *Transport) Close() error {
	t.markDead() // mark dead first so concurrent writers exit via the dead check
	var closeErr error
	t.closeOnce.Do(func() {
		t.pw.Close()
		t.writeMu.Lock()

		if err := t.conn.SetWriteDeadline(time.Now().Add(writeDeadlineBuffer)); err != nil {
			t.logger.Info("transport set write deadline", "err", err)
		}

		if err := t.conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		); err != nil {
			t.logger.Debug("transport close frame write", "err", err)
		}
		t.writeMu.Unlock()
		closeErr = t.conn.Close()
	})
	return closeErr
}

// markDead atomically marks the transport permanently unusable and
// signals pingPump to exit. Safe to call concurrently many times.
func (t *Transport) markDead() {
	if t.dead.CompareAndSwap(0, 1) {
		close(t.closed)
	}
}

// readPump drains incoming binary frames into the pipe.
// It is the SOLE reader of the WebSocket receive path.
func (t *Transport) readPump() {
	defer t.pw.Close()
	for {
		mt, r, err := t.conn.NextReader()
		if err != nil {
			if !isExpectedClose(err) {
				t.logger.Debug("tunnel read error", "err", err)
			}
			return
		}
		if mt != websocket.BinaryMessage {
			continue
		}
		if _, err := io.Copy(t.pw, r); err != nil {
			t.logger.Debug("tunnel pipe write error", "err", err)
			return
		}
	}
}

// pingPump sends periodic pings to keep the connection alive through NAT/LB.
// Exits when the transport is marked dead.
func (t *Transport) pingPump() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.writeMu.Lock()
			t.conn.SetWriteDeadline(time.Now().Add(writeDeadlineBuffer))
			err := t.conn.WriteMessage(websocket.PingMessage, nil)
			t.writeMu.Unlock()
			if err != nil {
				t.markDead()
				return
			}
		case <-t.closed:
			return
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Relay helpers
// ─────────────────────────────────────────────────────────────────────────────

// Relay copies data between a net.Conn and a Transport bidirectionally.
//
// Termination contract (prevents duplicate delivery):
//   - goroutine A owns the read of conn; goroutine B owns the read of tun.
//     No other goroutine reads either side. Each byte passes through exactly
//     one Read call and one Write call.
//   - When goroutine A finishes (EOF or error on conn), it calls tun.Close().
//     tun.Close() marks the transport dead and closes the pipe, causing
//     goroutine B's next Read from tun to return ErrTransportDead / io.EOF.
//     Goroutine B then calls conn.Close() and exits.
//   - The reverse path is symmetric.
//   - Because tun.Close() marks the transport dead before the pipe is closed,
//     no data written into tun after the close signal can reach the wire.
func Relay(conn net.Conn, tun *Transport, logger *slog.Logger) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer tun.Close()
		buf := make([]byte, 32*1024)
		if _, err := io.CopyBuffer(tun, conn, buf); err != nil && !isNetClosed(err) {
			logger.Debug("relay conn→tun", "err", err)
		}
	}()

	go func() {
		defer wg.Done()
		defer conn.Close()
		buf := make([]byte, 32*1024)
		if _, err := io.CopyBuffer(conn, tun, buf); err != nil && !isNetClosed(err) {
			logger.Debug("relay tun→conn", "err", err)
		}
	}()

	wg.Wait()
}

// RelayTransports copies data between two Transports (relay node path).
// Same termination contract as Relay.
func RelayTransports(a, b *Transport, logger *slog.Logger) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer b.Close()
		buf := make([]byte, 32*1024)
		if _, err := io.CopyBuffer(b, a, buf); err != nil && !isNetClosed(err) {
			logger.Debug("relay transport a→b", "err", err)
		}
	}()

	go func() {
		defer wg.Done()
		defer a.Close()
		buf := make([]byte, 32*1024)
		if _, err := io.CopyBuffer(a, b, buf); err != nil && !isNetClosed(err) {
			logger.Debug("relay transport b→a", "err", err)
		}
	}()

	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func isExpectedClose(err error) bool {
	return websocket.IsCloseError(err,
		websocket.CloseNormalClosure,
		websocket.CloseGoingAway,
		websocket.CloseNoStatusReceived,
	)
}

func isNetClosed(err error) bool {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, ErrTransportDead) {
		return true
	}
	var ne *net.OpError
	if errors.As(err, &ne) {
		return strings.Contains(ne.Err.Error(), "use of closed network connection")
	}
	return false
}
