package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux/muxproto"
)

// EdgeSession manages one TCP connection to an edge node.
type EdgeSession struct {
	nodeID       string
	conn         *muxproto.MuxConn
	server       *Server
	logger       *slog.Logger
	capabilities []string

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.RWMutex
	streams map[uint32]*muxproto.Stream

	once sync.Once
}

func (s *EdgeSession) hasCapability(cap string) bool {
	for _, c := range s.capabilities {
		if c == cap {
			return true
		}
	}
	return false
}

func newEdgeSession(nodeID string, conn *muxproto.MuxConn, srv *Server, logger *slog.Logger) *EdgeSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &EdgeSession{
		nodeID:  nodeID,
		conn:    conn,
		server:  srv,
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
		streams: make(map[uint32]*muxproto.Stream),
	}
}

func (s *EdgeSession) closeWithReason(reason string) {
	s.once.Do(func() {
		s.logger.Info("closing edge session", "reason", reason)
		s.cancel()
		s.conn.Close()
		s.mu.Lock()
		for _, st := range s.streams {
			st.OnRST("session closed: " + reason)
		}
		s.streams = make(map[uint32]*muxproto.Stream)
		s.mu.Unlock()
	})
}

// DoRequest pushes an HTTP request to the edge and waits for the response.
//
// Flow control (Direction A: server→client request body):
//
//	Before sending each DATA chunk the server acquires n bytes from
//	stream.sendWindowReq.  Credits are returned by the client via
//	WINDOW_UPDATE(WinDirRequest) frames emitted from stream.ReqBodyReader()
//	as the client's local handler reads the request body.
//
//	This ensures that stream.OnData (called from the client's readLoop) can
//	always write to reqBody without blocking, which prevents a slow consumer
//	on one stream from stalling all other streams on the connection.
func (s *EdgeSession) DoRequest(
	ctx context.Context,
	streamID uint32,
	reqMeta *muxproto.RequestMeta,
	reqBody io.Reader,
) (*muxproto.ResponseMeta, io.Reader, error) {

	stream := muxproto.NewStream(streamID, s.conn)

	s.mu.Lock()
	s.streams[streamID] = stream
	s.mu.Unlock()

	cleanup := func() {
		s.removeStream(streamID)
		stream.Close()
	}

	// Send request headers.
	if err := stream.SendHeaders(reqMeta, 0); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("send request headers: %w", err)
	}

	// Send request body with per-chunk window acquisition.
	if err := s.sendBodyWithFlowControl(ctx, stream, reqBody, true); err != nil {
		cleanup()
		return nil, nil, err
	}

	// Wait for response headers from edge.
	select {
	case <-ctx.Done():
		stream.SendRST("caller cancelled") //nolint:errcheck
		cleanup()
		return nil, nil, ctx.Err()
	case <-s.ctx.Done():
		cleanup()
		return nil, nil, fmt.Errorf("session closed")
	case respMeta, ok := <-stream.RecvResponseHeadersChan():
		if !ok || respMeta == nil {
			cleanup()
			return nil, nil, fmt.Errorf("stream reset before response headers")
		}
		// Wrap respBody in a WindowUpdateReader (Direction B receiver).
		// As the caller reads the response body, WINDOW_UPDATE(WinDirResponse)
		// frames are sent back to the client, allowing it to send more chunks.
		body := &streamCleanupReader{
			reader:   stream.RespBodyReader(),
			streamID: streamID,
			stream:   stream,
			session:  s,
		}
		return respMeta, body, nil
	}
}

// sendBodyWithFlowControl sends DATA frames for reqBody (isRequest=true) or
// respBody (isRequest=false), acquiring window credits before each chunk.
//
//   - isRequest=true  → acquires from sendWindowReq  (Direction A)
//   - isRequest=false → acquires from sendWindowResp (Direction B, server side)
func (s *EdgeSession) sendBodyWithFlowControl(
	ctx context.Context,
	stream *muxproto.Stream,
	body io.Reader,
	isRequest bool,
) error {
	if body == nil {
		return stream.SendData(nil, muxproto.FlagEndStream)
	}

	buf := make([]byte, muxproto.DataChunkSize)
	for {
		n, readErr := body.Read(buf)
		if n > 0 {
			var flags muxproto.Flags
			if readErr != nil {
				flags = muxproto.FlagEndStream
			}

			// Acquire window credit before sending.
			var acquireErr error
			if isRequest {
				acquireErr = stream.AcquireReqWindow(ctx, int64(n))
			}
			// (server never sends response body in this flow — that's client side)
			if acquireErr != nil {
				return fmt.Errorf("window acquire: %w", acquireErr)
			}

			if sendErr := stream.SendData(buf[:n], flags); sendErr != nil {
				return fmt.Errorf("send body DATA: %w", sendErr)
			}
		}
		if readErr != nil {
			if n == 0 {
				return stream.SendData(nil, muxproto.FlagEndStream)
			}
			return nil // END_STREAM was set on the last chunk above
		}
	}
}

func (s *EdgeSession) removeStream(id uint32) {
	s.mu.Lock()
	delete(s.streams, id)
	s.mu.Unlock()
}

// streamCleanupReader wraps the response body reader and removes the stream
// from the session map once the caller finishes consuming the response.
type streamCleanupReader struct {
	reader   io.Reader
	streamID uint32
	stream   *muxproto.Stream
	session  *EdgeSession
	once     sync.Once
}

func (r *streamCleanupReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil {
		r.once.Do(func() {
			r.session.removeStream(r.streamID)
			r.stream.Close()
		})
	}
	return n, err
}

// ─────────────────────────────────────────────────────────────────────────────
// Frame dispatch loop
// ─────────────────────────────────────────────────────────────────────────────

func (s *EdgeSession) readLoop() {
	go s.pingLoop()
	defer s.closeWithReason("read loop exited")

	for {
		f, err := s.conn.ReadFrame()
		if err != nil {
			if s.ctx.Err() == nil {
				s.logger.Debug("read frame error", "err", err)
			}
			return
		}

		switch {
		case f.StreamID == muxproto.ControlStreamID:
			s.handleControlFrame(f)

		case f.Type == muxproto.TypeHeaders ||
			f.Type == muxproto.TypeData ||
			f.Type == muxproto.TypeRST ||
			f.Type == muxproto.TypeWindowUpdate:
			s.handleStreamFrame(f)

		default:
			s.logger.Warn("unexpected frame", "type", f.Type, "sid", f.StreamID)
		}
	}
}

func (s *EdgeSession) handleControlFrame(f *muxproto.Frame) {
	switch f.Type {
	case muxproto.TypePong:
		// handled by pingLoop
	case muxproto.TypePing:
		s.conn.WriteFrame(&muxproto.Frame{Type: muxproto.TypePong}) //nolint:errcheck
	default:
		s.logger.Debug("unknown control frame", "type", f.Type)
	}
}

func (s *EdgeSession) handleStreamFrame(f *muxproto.Frame) {
	s.mu.RLock()
	stream := s.streams[f.StreamID]
	s.mu.RUnlock()

	if stream == nil {
		s.logger.Warn("frame for unknown stream", "sid", f.StreamID, "type", f.Type)
		return
	}

	switch f.Type {
	case muxproto.TypeHeaders:
		if err := stream.OnHeaders(f); err != nil {
			s.logger.Warn("stream headers error", "sid", f.StreamID, "err", err)
		}

	case muxproto.TypeData:
		// OnData is non-blocking because flow control ensures the pipe has room.
		if err := stream.OnData(f); err != nil {
			s.logger.Warn("stream data error — flow control violation, closing session",
				"sid", f.StreamID, "err", err)
			s.closeWithReason(fmt.Sprintf("flow control violation on stream %d", f.StreamID))
		}

	case muxproto.TypeWindowUpdate:
		// Client consumed bytes from reqBody → credits for Direction A.
		// Also handles Direction B if client is sending response body.
		var msg muxproto.WindowUpdateMsg
		if err := muxproto.Unmarshal(f.Payload, &msg); err != nil {
			s.logger.Warn("bad WINDOW_UPDATE payload", "sid", f.StreamID, "err", err)
			return
		}
		stream.OnWindowUpdate(msg.Dir, msg.Increment)

	case muxproto.TypeRST:
		var msg muxproto.RSTMsg
		muxproto.Unmarshal(f.Payload, &msg) //nolint:errcheck
		stream.OnRST(msg.Reason)
		s.removeStream(f.StreamID)
	}
}

func (s *EdgeSession) pingLoop() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	lastPong := time.Now()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if time.Since(lastPong) > pingInterval+pingTimeout {
				s.closeWithReason("ping timeout")
				return
			}
			s.conn.WriteFrame(&muxproto.Frame{Type: muxproto.TypePing}) //nolint:errcheck
			lastPong = time.Now() // optimistic; replace with pong channel in production
		}
	}
}
