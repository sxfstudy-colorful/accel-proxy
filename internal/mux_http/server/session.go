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
	// defaultSendWindow: max unacknowledged bytes per (edge, stream) pair
	// before the sender goroutine pauses.
	defaultSendWindow = 8 * 1024 * 1024 // 8 MiB

	// pingInterval / pingTimeout control the server-side keepalive.
	pingInterval = 20 * time.Second
	pingTimeout  = 15 * time.Second
)

// ─────────────────────────────────────────────────────────────────────────────
// streamSendState — flow-control bookkeeping for one stream → one edge
// ─────────────────────────────────────────────────────────────────────────────

type streamSendState struct {
	entry       *StreamEntry
	sendOffset  int64        // bytes written to wire for this edge
	ackedOffset atomic.Int64 // bytes the edge has confirmed received
	window      int64        // max allowed (sendOffset - ackedOffset)

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
	return s.sendOffset - s.ackedOffset.Load()
}

// waitWindow blocks until send-window credit is available, ctx is done, or
// the state has been evicted.
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

// updateAck records a cumulative ACK from the edge.
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
// EdgeSession — one long-lived connection from an edge node
// ─────────────────────────────────────────────────────────────────────────────

// EdgeSession manages one TCP connection from an edge node.
// It owns:
//   - a read loop (readLoop) that processes ACK/PING frames from the edge
//   - one sender goroutine per subscribed stream (startSender)
//   - a keepalive goroutine (pingLoop)
type EdgeSession struct {
	nodeID string
	conn   *muxproto.MuxConn
	logger *slog.Logger
	broker *PushBroker

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	streams map[uint32]*streamSendState // stream ID → send state

	once sync.Once
}

func newEdgeSession(
	nodeID string,
	conn *muxproto.MuxConn,
	broker *PushBroker,
	logger *slog.Logger,
) *EdgeSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &EdgeSession{
		nodeID:  nodeID,
		conn:    conn,
		broker:  broker,
		logger:  logger.With("node_id", nodeID, "remote", conn.RemoteAddr()),
		ctx:     ctx,
		cancel:  cancel,
		streams: make(map[uint32]*streamSendState),
	}
}

// closeWithReason tears down the session idempotently.
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

// addStream registers a stream to be pushed to this edge.
// Returns the (possibly pre-existing) send state.
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

// startSender launches a goroutine that reads from the StreamBuffer starting
// at resumeOffset and writes DATA frames to the edge connection.
func (s *EdgeSession) startSender(st *streamSendState, resumeOffset int64) {
	st.sendOffset = resumeOffset
	st.ackedOffset.Store(resumeOffset)

	go func() {
		buf := make([]byte, muxproto.DataChunkSize)
		offset := resumeOffset
		entry := st.entry

		for {
			// Wait for data to exist at current offset.
			entry.Buffer.WaitForData(s.ctx, offset)
			if s.ctx.Err() != nil {
				return
			}

			// Apply flow-control: pause when inflight ≥ window.
			if err := st.waitWindow(s.ctx); err != nil {
				return
			}

			n, err := entry.Buffer.Read(offset, buf)
			if err == io.EOF {
				// Stream finished: send RST(EOF) so the edge knows.
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
				// No data yet (race between WaitForData and Read); retry.
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
			st.sendOffset += int64(n)
			offset += int64(n)
		}
	}()
}

// readLoop processes inbound frames from the edge until the connection closes
// or the session is cancelled.  Blocking; call from the connection goroutine.
func (s *EdgeSession) readLoop() {
	// pingLoop runs independently so that ReadFrame (which blocks) does not
	// prevent us from sending pings.
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

		case muxproto.TypePong:
			// Keepalive acknowledged; pingLoop tracks the timestamp.
			s.broker.onPong(s.nodeID)

		case muxproto.TypePing:
			s.conn.WriteFrame(&muxproto.Frame{Type: muxproto.TypePong}) //nolint:errcheck

		default:
			s.logger.Warn("unexpected frame type from edge", "type", f.Type)
		}
	}
}

// pingLoop sends periodic PING frames and closes the session if no PONG is
// received within pingTimeout.
func (s *EdgeSession) pingLoop() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	lastPong := time.Now()
	// Register a callback so readLoop can update lastPong.
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
