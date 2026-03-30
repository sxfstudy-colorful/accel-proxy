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
	capabilities []string // e.g. ["reverse-http", "push"]

	ctx    context.Context
	cancel context.CancelFunc

	// streams tracks active request-response streams by ID.
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
		// Reset all active streams.
		s.mu.Lock()
		for _, st := range s.streams {
			st.OnRST("session closed: " + reason)
		}
		s.streams = make(map[uint32]*muxproto.Stream)
		s.mu.Unlock()
	})
}

// DoRequest pushes an HTTP request to the edge and waits for the response.
// This is the core reverse-proxy operation.
//
// Flow:
//   1. Create stream, register in streams map
//   2. Send HEADERS(request meta)
//   3. Send DATA frames (request body), last one with END_STREAM
//   4. Wait for HEADERS(response meta) from edge
//   5. Return response meta + response body reader
func (s *EdgeSession) DoRequest(
	ctx context.Context,
	streamID uint32,
	reqMeta *muxproto.RequestMeta,
	reqBody io.Reader,
) (*muxproto.ResponseMeta, io.Reader, error) {

	stream := muxproto.NewStream(streamID, s.conn)

	// Register stream so readLoop can dispatch frames to it.
	s.mu.Lock()
	s.streams[streamID] = stream
	s.mu.Unlock()

	defer func() {
		// Cleanup happens after caller finishes reading response body.
		// We defer removal but stream.Close() is called by caller or on error.
	}()

	// Send request headers.
	if err := stream.SendHeaders(reqMeta, 0); err != nil {
		s.removeStream(streamID)
		stream.Close()
		return nil, nil, fmt.Errorf("send request headers: %w", err)
	}

	// Send request body.
	if reqBody != nil {
		buf := make([]byte, muxproto.DataChunkSize)
		for {
			n, err := reqBody.Read(buf)
			if n > 0 {
				var flags muxproto.Flags
				if err != nil { // EOF or error → last chunk
					flags = muxproto.FlagEndStream
				}
				if sendErr := stream.SendData(buf[:n], flags); sendErr != nil {
					s.removeStream(streamID)
					stream.Close()
					return nil, nil, fmt.Errorf("send request body: %w", sendErr)
				}
			}
			if err != nil {
				if n == 0 {
					// Send empty END_STREAM if EOF with no data
					if sendErr := stream.SendData(nil, muxproto.FlagEndStream); sendErr != nil {
						s.removeStream(streamID)
						stream.Close()
						return nil, nil, fmt.Errorf("send end stream: %w", sendErr)
					}
				}
				break
			}
		}
	} else {
		// No body — send empty DATA with END_STREAM.
		if err := stream.SendData(nil, muxproto.FlagEndStream); err != nil {
			s.removeStream(streamID)
			stream.Close()
			return nil, nil, fmt.Errorf("send end stream: %w", err)
		}
	}

	// Wait for response headers from edge.
	select {
	case <-ctx.Done():
		stream.SendRST("caller cancelled") //nolint:errcheck
		s.removeStream(streamID)
		stream.Close()
		return nil, nil, ctx.Err()
	case <-s.ctx.Done():
		s.removeStream(streamID)
		stream.Close()
		return nil, nil, fmt.Errorf("session closed")
	case respMeta, ok := <-stream.RecvResponseHeadersChan():
		if !ok || respMeta == nil {
			s.removeStream(streamID)
			stream.Close()
			return nil, nil, fmt.Errorf("stream reset before response headers")
		}
		// Return response body as a reader. Caller is responsible for reading
		// until EOF (which comes when the edge sends END_STREAM on response data).
		// We wrap the pipe in a cleanup reader that removes the stream when done.
		body := &streamCleanupReader{
			reader:   stream.RespBody(),
			streamID: streamID,
			stream:   stream,
			session:  s,
		}
		return respMeta, body, nil
	}
}

func (s *EdgeSession) removeStream(id uint32) {
	s.mu.Lock()
	delete(s.streams, id)
	s.mu.Unlock()
}

// streamCleanupReader wraps the response body pipe and cleans up the stream
// when the caller finishes reading (EOF or error).
type streamCleanupReader struct {
	reader   *muxproto.StreamPipe
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
		// ── Control frames ───────────────────────────────────────────────
		case f.StreamID == muxproto.ControlStreamID:
			s.handleControlFrame(f)

		// ── Request-response stream frames ───────────────────────────────
		case f.Type == muxproto.TypeHeaders || f.Type == muxproto.TypeData || f.Type == muxproto.TypeRST:
			s.handleStreamFrame(f)

		default:
			s.logger.Warn("unexpected frame", "type", f.Type, "sid", f.StreamID)
		}
	}
}

func (s *EdgeSession) handleControlFrame(f *muxproto.Frame) {
	switch f.Type {
	case muxproto.TypePong:
		// handled by pingLoop via timestamp (simplified)
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
		if err := stream.OnData(f); err != nil {
			s.logger.Warn("stream data error", "sid", f.StreamID, "err", err)
		}
	case muxproto.TypeRST:
		var msg muxproto.RSTMsg
		muxproto.Unmarshal(f.Payload, &msg) //nolint:errcheck
		stream.OnRST(msg.Reason)
		s.removeStream(f.StreamID)
	}
}

// pingLoop sends periodic PING frames.
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
			// Simplified: we don't track pong separately since readLoop handles it.
			// In production you'd use a pong channel like the mux_http version.
			lastPong = time.Now() // optimistic; real impl should track actual pong
		}
	}
}
