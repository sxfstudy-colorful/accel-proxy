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
	defaultSendWindow = 8 * 1024 * 1024 // 8 MiB
	pingInterval      = 20 * time.Second
	pingTimeout       = 15 * time.Second
)

// ─────────────────────────────────────────────────────────────────────────────
// streamSendState — flow-control bookkeeping for one stream → one edge
//
// sendOffset is atomic because it is written by the sender goroutine and
// read by inflight() which may be called from waitWindow in the same goroutine.
// Although currently single-writer, making it atomic eliminates the data race
// that the race detector would flag and is future-proof against callers from
// other goroutines (e.g. monitoring/metrics).
// ─────────────────────────────────────────────────────────────────────────────

type streamSendState struct {
	entry       *StreamEntry
	sendOffset  atomic.Int64 // bytes written to wire for this edge
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

type EdgeSession struct {
	nodeID string
	conn   *muxproto.MuxConn
	logger *slog.Logger
	broker *PushBroker

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	streams map[uint32]*streamSendState

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

// startSender launches a goroutine that reads from the StreamBuffer starting
// at resumeOffset and writes DATA frames to the edge connection.
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

// readLoop processes inbound frames from the edge until the connection closes
// or the session is cancelled.
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
			s.broker.onPong(s.nodeID)

		case muxproto.TypePing:
			s.conn.WriteFrame(&muxproto.Frame{Type: muxproto.TypePong}) //nolint:errcheck

		default:
			s.logger.Warn("unexpected frame type from edge", "type", f.Type)
		}
	}
}

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
