package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux_http/muxproto"
)

const (
	defaultSendWindow = 8 * 1024 * 1024
	pingInterval      = 20 * time.Second
	pingTimeout       = 15 * time.Second
)

// ─────────────────────────────────────────────────────────────────────────────
// streamSendState (push mode, unchanged)
// ─────────────────────────────────────────────────────────────────────────────

type streamSendState struct {
	entry       *StreamEntry
	sendOffset  atomic.Int64
	ackedOffset atomic.Int64
	window      int64

	mu   sync.Mutex
	cond *sync.Cond

	evicted bool
}

func newStreamSendState(entry *StreamEntry) *streamSendState {
	s := &streamSendState{entry: entry, window: defaultSendWindow}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *streamSendState) inflight() int64 {
	return s.sendOffset.Load() - s.ackedOffset.Load()
}

func (s *streamSendState) waitWindow(ctx context.Context) error {
	quit := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.cond.Broadcast()
		case <-quit:
		}
	}()

	s.mu.Lock()
	for !s.evicted && s.inflight() >= s.window && ctx.Err() == nil {
		s.cond.Wait()
	}
	evicted := s.evicted
	s.mu.Unlock()
	close(quit)

	if ctx.Err() != nil {
		return ctx.Err()
	}
	if evicted {
		return fmt.Errorf("stream %d evicted", s.entry.ID)
	}
	return nil
}

func (s *streamSendState) updateAck(offset int64) {
	for {
		old := s.ackedOffset.Load()
		if offset <= old {
			return
		}
		if s.ackedOffset.CompareAndSwap(old, offset) {
			break
		}
	}
	s.mu.Lock()
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *streamSendState) evict() {
	s.mu.Lock()
	s.evicted = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeSession
// ─────────────────────────────────────────────────────────────────────────────

type EdgeSession struct {
	nodeID string
	conn   *muxproto.MuxConn
	logger *slog.Logger
	broker *PushBroker

	// Capabilities declared during registration.
	capPush      bool
	capHTTPProxy bool

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	streams map[uint32]*streamSendState // push streams

	// httpRespBuf accumulates streamed response body chunks per request ID.
	// Only used when body arrives in multiple TypeHTTPResponseBody frames.
	httpRespMu  sync.Mutex
	httpRespBuf map[uint32]*httpResponseAccumulator

	once sync.Once
}

// httpResponseAccumulator collects streamed response parts.
type httpResponseAccumulator struct {
	head *muxproto.HTTPResponseHeadMsg
	body []byte
}

func newEdgeSession(
	nodeID string,
	conn *muxproto.MuxConn,
	broker *PushBroker,
	logger *slog.Logger,
) *EdgeSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &EdgeSession{
		nodeID:      nodeID,
		conn:        conn,
		broker:      broker,
		logger:      logger.With("node_id", nodeID, "remote", conn.RemoteAddr()),
		ctx:         ctx,
		cancel:      cancel,
		streams:     make(map[uint32]*streamSendState),
		httpRespBuf: make(map[uint32]*httpResponseAccumulator),
	}
}

func (s *EdgeSession) closeWithReason(reason string) {
	s.once.Do(func() {
		s.logger.Info("closing edge session", "reason", reason)
		s.cancel()
		s.conn.Close()
		s.mu.Lock()
		for _, st := range s.streams {
			st.evict()
		}
		s.mu.Unlock()
	})
}

func (s *EdgeSession) addStream(entry *StreamEntry) *streamSendState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.streams[entry.ID]; ok {
		return st
	}
	st := newStreamSendState(entry)
	s.streams[entry.ID] = st
	return st
}

func (s *EdgeSession) startSender(st *streamSendState, resumeOffset int64) {
	st.sendOffset.Store(resumeOffset)
	st.ackedOffset.Store(resumeOffset)

	go func() {
		buf := make([]byte, muxproto.DataChunkSize)
		offset := resumeOffset
		entry := st.entry

		for {
			entry.Buffer.WaitForData(s.ctx, offset)
			if s.ctx.Err() != nil {
				return
			}

			if err := st.waitWindow(s.ctx); err != nil {
				return
			}

			n, err := entry.Buffer.Read(offset, buf)
			if err == io.EOF {
				s.conn.WriteFrame(&muxproto.Frame{ //nolint:errcheck
					StreamID: entry.ID,
					Type:     muxproto.TypeRST,
					Payload:  muxproto.Marshal(muxproto.RSTMsg{StreamID: entry.ID, Reason: "EOF"}),
				})
				return
			}
			if err != nil {
				s.logger.Warn("stream buffer read error", "stream", entry.Name, "err", err)
				s.closeWithReason(fmt.Sprintf("buffer error: %v", err))
				return
			}
			if n == 0 {
				continue
			}

			if err := s.conn.WriteFrame(&muxproto.Frame{
				StreamID: entry.ID,
				Type:     muxproto.TypeData,
				Payload:  buf[:n],
			}); err != nil {
				s.closeWithReason(fmt.Sprintf("write DATA: %v", err))
				return
			}
			st.sendOffset.Add(int64(n))
			offset += int64(n)
		}
	}()
}

// readLoop processes inbound frames from the edge.
// Handles both push ACKs and HTTP reverse proxy responses.
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

		switch f.Type {
		// ── Push stream frames ───────────────────────────────────────────
		case muxproto.TypeAck:
			var msg muxproto.AckMsg
			if err := muxproto.Unmarshal(f.Payload, &msg); err != nil {
				s.logger.Warn("bad ACK payload", "err", err)
				continue
			}
			s.mu.Lock()
			if st, ok := s.streams[msg.StreamID]; ok {
				st.updateAck(msg.Offset)
			}
			s.mu.Unlock()
			s.broker.onAck(s.nodeID, msg.StreamID, msg.Offset)

		// ── Keepalive ────────────────────────────────────────────────────
		case muxproto.TypePong:
			s.broker.onPong(s.nodeID)

		case muxproto.TypePing:
			s.conn.WriteFrame(&muxproto.Frame{Type: muxproto.TypePong}) //nolint:errcheck

		// ── HTTP reverse proxy response frames ───────────────────────────
		case muxproto.TypeHTTPResponseHead:
			s.handleHTTPResponseHead(f)

		case muxproto.TypeHTTPResponseBody:
			s.handleHTTPResponseBody(f)

		case muxproto.TypeHTTPResponseEnd:
			s.handleHTTPResponseEnd(f)

		case muxproto.TypeHTTPError:
			s.handleHTTPError(f)

		default:
			s.logger.Warn("unexpected frame type from edge", "type", f.Type)
		}
	}
}

// ── HTTP response frame handlers ─────────────────────────────────────────────

func (s *EdgeSession) handleHTTPResponseHead(f *muxproto.Frame) {
	reqID := f.StreamID
	var msg muxproto.HTTPResponseHeadMsg
	if err := muxproto.Unmarshal(f.Payload, &msg); err != nil {
		s.logger.Warn("bad HTTPResponseHead payload", "request_id", reqID, "err", err)
		return
	}

	// Start accumulating: store head, wait for body chunks + end.
	s.httpRespMu.Lock()
	s.httpRespBuf[reqID] = &httpResponseAccumulator{head: &msg}
	s.httpRespMu.Unlock()
}

func (s *EdgeSession) handleHTTPResponseBody(f *muxproto.Frame) {
	reqID := f.StreamID

	s.httpRespMu.Lock()
	acc, ok := s.httpRespBuf[reqID]
	if ok {
		acc.body = append(acc.body, f.Payload...)
	}
	s.httpRespMu.Unlock()

	if !ok {
		s.logger.Warn("HTTPResponseBody for unknown accumulator", "request_id", reqID)
	}
}

func (s *EdgeSession) handleHTTPResponseEnd(f *muxproto.Frame) {
	reqID := f.StreamID

	s.httpRespMu.Lock()
	acc, ok := s.httpRespBuf[reqID]
	delete(s.httpRespBuf, reqID)
	s.httpRespMu.Unlock()

	if !ok || acc.head == nil {
		s.logger.Warn("HTTPResponseEnd without head", "request_id", reqID)
		return
	}

	// Deliver the complete response to the waiting HTTP handler.
	s.broker.HTTPProxy.OnHTTPResponseComplete(reqID, acc.head, acc.body)
}

func (s *EdgeSession) handleHTTPError(f *muxproto.Frame) {
	reqID := f.StreamID
	var msg muxproto.HTTPErrorMsg
	if err := muxproto.Unmarshal(f.Payload, &msg); err != nil {
		s.logger.Warn("bad HTTPError payload", "request_id", reqID, "err", err)
		return
	}

	// Clean up any partial accumulator.
	s.httpRespMu.Lock()
	delete(s.httpRespBuf, reqID)
	s.httpRespMu.Unlock()

	s.broker.HTTPProxy.OnHTTPError(reqID, &msg)
}

// ── Ping loop (unchanged) ────────────────────────────────────────────────────

func (s *EdgeSession) pingLoop() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	lastPong := time.Now()
	pongCh := s.broker.registerPongCh(s.nodeID)
	defer s.broker.unregisterPongCh(s.nodeID)

	for {
		select {
		case <-s.ctx.Done():
			return
		case t := <-pongCh:
			lastPong = t
		case <-ticker.C:
			if time.Since(lastPong) > pingInterval+pingTimeout {
				s.closeWithReason("ping timeout")
				return
			}
			s.conn.WriteFrame(&muxproto.Frame{Type: muxproto.TypePing}) //nolint:errcheck
		}
	}
}
