package tunnel

import (
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultFrameSize    = 32 * 1024 // 32 KB per WS binary frame
	pingInterval        = 20 * time.Second
	pongWait            = 30 * time.Second
	writeDeadlineBuffer = 5 * time.Second
)

// Transport wraps a *websocket.Conn and exposes it as an io.ReadWriteCloser
// so the proxy layer can treat the tunnel like a plain TCP connection.
//
// Internally it:
//   - fragments large writes into bounded binary frames
//   - answers pings to keep the connection alive through NAT/load-balancers
//   - serialises concurrent writes (websocket.Conn is not goroutine-safe for writes)
type Transport struct {
	conn      *websocket.Conn
	frameSize int64

	// reader pipe: the read pump pushes reassembled payloads here
	pr *io.PipeReader
	pw *io.PipeWriter

	once   sync.Once
	closed chan struct{}
	writeMu sync.Mutex

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

	// Configure keep-alive pong handler.
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

// Read implements io.Reader. Blocks until data arrives from the remote side.
func (t *Transport) Read(p []byte) (int, error) {
	return t.pr.Read(p)
}

// Write implements io.Writer. Splits p into frames and sends them.
func (t *Transport) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n := int(t.frameSize)
		if n > len(p) {
			n = len(p)
		}
		t.writeMu.Lock()
		t.conn.SetWriteDeadline(time.Now().Add(writeDeadlineBuffer + pingInterval))
		err := t.conn.WriteMessage(websocket.BinaryMessage, p[:n])
		t.writeMu.Unlock()
		if err != nil {
			return total, err
		}
		p = p[n:]
		total += n
	}
	return total, nil
}

// Close shuts the transport down gracefully.
func (t *Transport) Close() error {
	var closeErr error
	t.once.Do(func() {
		close(t.closed)
		t.pw.Close()
		t.writeMu.Lock()
		t.conn.SetWriteDeadline(time.Now().Add(writeDeadlineBuffer))
		t.conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
		t.writeMu.Unlock()
		closeErr = t.conn.Close()
	})
	return closeErr
}

// readPump drains incoming binary frames into the pipe.
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
			continue // skip text/ping/pong frames
		}
		if _, err := io.Copy(t.pw, r); err != nil {
			t.logger.Debug("tunnel pipe write error", "err", err)
			return
		}
	}
}

// pingPump sends periodic pings to keep the connection alive.
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
				return
			}
		case <-t.closed:
			return
		}
	}
}

func isExpectedClose(err error) bool {
	return websocket.IsCloseError(err,
		websocket.CloseNormalClosure,
		websocket.CloseGoingAway,
		websocket.CloseNoStatusReceived,
	)
}

// Relay copies data between a net.Conn and a tunnel Transport concurrently,
// closing both sides when either direction ends.
func Relay(conn net.Conn, tun *Transport, logger *slog.Logger) {
	var wg sync.WaitGroup
	wg.Add(2)

	// conn → tunnel
	go func() {
		defer wg.Done()
		defer tun.Close()
		buf := make([]byte, 32*1024)
		if _, err := io.CopyBuffer(tun, conn, buf); err != nil && !isNetClosed(err) {
			logger.Debug("relay conn→tun", "err", err)
		}
	}()

	// tunnel → conn
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

// RelayTransports copies data between two tunnel Transports (relay node path).
func RelayTransports(a, b *Transport, logger *slog.Logger) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer b.Close()
		buf := make([]byte, 32*1024)
		io.CopyBuffer(b, a, buf) //nolint:errcheck
	}()

	go func() {
		defer wg.Done()
		defer a.Close()
		buf := make([]byte, 32*1024)
		io.CopyBuffer(a, b, buf) //nolint:errcheck
	}()

	wg.Wait()
}

func isNetClosed(err error) bool {
	if err == nil || err == io.EOF {
		return true
	}
	if ne, ok := err.(*net.OpError); ok {
		return ne.Err.Error() == "use of closed network connection"
	}
	return false
}
