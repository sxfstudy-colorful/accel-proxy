package tunnel

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/sxfstudy-colorful/accel-proxy/internal/netutil"
)

const (
	defaultFrameSize    = 32 * 1024
	pingInterval        = 20 * time.Second
	pongWait            = 30 * time.Second
	writeDeadlineBuffer = 5 * time.Second
	relayBufSize        = 128 * 1024 // 128 KiB relay buffer
)

// ErrTransportDead is returned by Read and Write after a Transport has been
// permanently closed due to a write error or an explicit Close() call.
var ErrTransportDead = errors.New("transport is dead")

// relayBufPool reuses relay copy buffers to reduce GC pressure under high
// connection counts. Each buffer is 128 KiB.
var relayBufPool = sync.Pool{
	New: func() any { return make([]byte, relayBufSize) },
}

// Transport wraps a *websocket.Conn and exposes it as an io.ReadWriteCloser.
type Transport struct {
	conn      *websocket.Conn
	frameSize int64

	pr *io.PipeReader
	pw *io.PipeWriter

	closeOnce sync.Once
	closed    chan struct{}
	writeMu   sync.Mutex

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

func (t *Transport) Read(p []byte) (int, error) {
	if t.dead.Load() == 1 {
		return 0, ErrTransportDead
	}
	return t.pr.Read(p)
}

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

func (t *Transport) Close() error {
	t.markDead()
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

func (t *Transport) markDead() {
	if t.dead.CompareAndSwap(0, 1) {
		close(t.closed)
	}
}

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
// Relay helpers — use sync.Pool for buffer reuse
// ─────────────────────────────────────────────────────────────────────────────

// Relay copies data between a net.Conn and a Transport bidirectionally.
func Relay(conn net.Conn, tun *Transport, logger *slog.Logger) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer tun.Close()
		buf := relayBufPool.Get().([]byte)
		defer relayBufPool.Put(buf)
		if _, err := io.CopyBuffer(tun, conn, buf); err != nil && !isNetClosed(err) {
			logger.Debug("relay conn→tun", "err", err)
		}
	}()

	go func() {
		defer wg.Done()
		defer conn.Close()
		buf := relayBufPool.Get().([]byte)
		defer relayBufPool.Put(buf)
		if _, err := io.CopyBuffer(conn, tun, buf); err != nil && !isNetClosed(err) {
			logger.Debug("relay tun→conn", "err", err)
		}
	}()

	wg.Wait()
}

// RelayTransports copies data between two Transports (relay node path).
func RelayTransports(a, b *Transport, logger *slog.Logger) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer b.Close()
		buf := relayBufPool.Get().([]byte)
		defer relayBufPool.Put(buf)
		if _, err := io.CopyBuffer(b, a, buf); err != nil && !isNetClosed(err) {
			logger.Debug("relay transport a→b", "err", err)
		}
	}()

	go func() {
		defer wg.Done()
		defer a.Close()
		buf := relayBufPool.Get().([]byte)
		defer relayBufPool.Put(buf)
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
	if errors.Is(err, ErrTransportDead) {
		return true
	}
	return netutil.IsExpectedCloseErr(err)
}
