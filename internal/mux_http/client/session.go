package client

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux_http/muxproto"
)

const (
	recvBufCap    = 64 * 1024 * 1024 // 64 MiB per stream receive buffer
	ackEveryBytes = 1 * 1024 * 1024  // send ACK every 1 MiB received
	ackInterval   = 200 * time.Millisecond

	reconnectBase = 1 * time.Second
	reconnectMax  = 30 * time.Second

	clientPingInterval = 20 * time.Second
	clientPingTimeout  = 15 * time.Second
)

// streamRecvState holds the per-stream receive state on the client side.
//
// recvOffset and ackedOffset are atomic.Int64 to eliminate the data race
// between readLoop (writes recvOffset) and ackLoop (reads both, writes
// ackedOffset).
type streamRecvState struct {
	name        string
	id          uint32
	recvOffset  atomic.Int64 // total bytes received (= resume offset on reconnect)
	ackedOffset atomic.Int64 // last offset reported to server
	buf         *RecvBuffer
}

// streamReaderWrapper implements io.Reader by forwarding to the current
// RecvBuffer of a streamRecvState. Because the underlying buf pointer never
// changes (we use Reopen instead of replacing the buf), this wrapper is
// technically unnecessary — but it keeps the public API clean and future-proof.
type streamReaderWrapper struct {
	st *streamRecvState
}

func (w *streamReaderWrapper) Read(p []byte) (int, error) {
	return w.st.buf.Read(p)
}

// EdgeClientSession manages the long-lived connection from an edge node to
// mux-server (through accel-proxy).
//
// Lifecycle:
//
//	NewEdgeClientSession → Run() [blocks, reconnects on failure] → Close()
type EdgeClientSession struct {
	nodeID     string
	accessAddr string
	subs       []string
	logger     *slog.Logger

	streams     map[string]*streamRecvState
	streamsByID map[uint32]*streamRecvState
	idMu        sync.RWMutex

	conn   *muxproto.MuxConn
	connMu sync.Mutex

	// pongCh carries pong signals from readLoop to pingLoop.
	// Recreated on each runOnce cycle.
	pongCh chan struct{}

	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc
}

// NewEdgeClientSession creates a session; call Run() to start connecting.
func NewEdgeClientSession(
	nodeID, accessAddr string,
	subs []string,
	logger *slog.Logger,
) *EdgeClientSession {
	ctx, cancel := context.WithCancel(context.Background())
	s := &EdgeClientSession{
		nodeID:     nodeID,
		accessAddr: accessAddr,
		subs:       subs,
		logger:     logger.With("node_id", nodeID),
		streams:    make(map[string]*streamRecvState, len(subs)),
		ctx:        ctx,
		cancel:     cancel,
	}
	for _, name := range subs {
		s.streams[name] = &streamRecvState{
			name: name,
			buf:  NewRecvBuffer(recvBufCap),
		}
	}
	return s
}

// Close permanently shuts down the session (no more reconnects).
func (s *EdgeClientSession) Close() {
	s.closed.Store(true)
	s.cancel()
	s.connMu.Lock()
	if s.conn != nil {
		s.conn.Close()
	}
	s.connMu.Unlock()
}

// StreamReader returns an io.Reader for the named stream.
// The reader follows the underlying RecvBuffer across reconnects — if the
// network drops and the session reconnects, new data continues to flow through
// the same reader without the consumer seeing any interruption (unless the
// server sends a clean RST/EOF).
//
// Returns nil if the name was not in the subscription list.
func (s *EdgeClientSession) StreamReader(name string) io.Reader {
	if st, ok := s.streams[name]; ok {
		return &streamReaderWrapper{st: st}
	}
	return nil
}

// ResumeOffset returns the byte offset the named stream has received so far.
func (s *EdgeClientSession) ResumeOffset(name string) int64 {
	if st, ok := s.streams[name]; ok {
		return st.recvOffset.Load()
	}
	return 0
}

// Run connects, registers, and reconnects with exponential back-off until
// Close() is called.
func (s *EdgeClientSession) Run() {
	backoff := reconnectBase
	for !s.closed.Load() {
		if err := s.runOnce(); err != nil {
			if s.closed.Load() {
				return
			}
			s.logger.Warn("session error, reconnecting",
				"err", err, "backoff", backoff)
			select {
			case <-time.After(backoff):
			case <-s.ctx.Done():
				return
			}
			if backoff < reconnectMax {
				backoff *= 2
			}
		} else {
			backoff = reconnectBase
		}
	}
}

func (s *EdgeClientSession) runOnce() error {
	// ── Reopen any closed RecvBuffers before reconnecting ────────────────
	// After a network disconnect the previous cycle may have closed some
	// buffers (e.g. via handleRST with a non-EOF reason or session teardown).
	// Reopen them so that Write can resume on the same buffer instance.
	// This keeps the consumer's io.Reader reference valid — they just see a
	// brief stall, then data resumes.
	//
	// Note: buffers closed by a clean RST(EOF) should NOT be reopened — the
	// stream is genuinely finished. However, distinguishing "network error
	// close" from "clean EOF close" would require extra state. The current
	// approach reopens all closed buffers on reconnect; if the server has
	// truly finished the stream, it will send RST(EOF) again and the buffer
	// will be cleanly closed once more.
	for _, st := range s.streams {
		if st.buf.IsClosed() {
			st.buf.Reopen()
			s.logger.Debug("reopened recv buffer for reconnect", "stream", st.name)
		}
	}

	conn, err := net.DialTimeout("tcp", s.accessAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s: %w", s.accessAddr, err)
	}
	mc := muxproto.NewMuxConn(conn)

	s.connMu.Lock()
	s.conn = mc
	s.connMu.Unlock()

	defer func() {
		mc.Close()
		s.connMu.Lock()
		if s.conn == mc {
			s.conn = nil
		}
		s.connMu.Unlock()
	}()

	// 1. Send REGISTER with resume offsets.
	resume := make(map[string]int64)
	for name, st := range s.streams {
		off := st.recvOffset.Load()
		if off > 0 {
			resume[name] = off
		}
	}
	regPayload := muxproto.Marshal(muxproto.RegisterMsg{
		NodeID: s.nodeID,
		Subs:   s.subs,
		Resume: resume,
	})
	if err := mc.WriteFrame(&muxproto.Frame{
		StreamID: muxproto.ControlStreamID,
		Type:     muxproto.TypeRegister,
		Payload:  regPayload,
	}); err != nil {
		return fmt.Errorf("write REGISTER: %w", err)
	}

	// 2. Read REGISTER_ACK.
	f, err := mc.ReadFrame()
	if err != nil {
		return fmt.Errorf("read REGISTER_ACK: %w", err)
	}
	if f.Type != muxproto.TypeRegisterAck {
		return fmt.Errorf("expected REGISTER_ACK, got type %d", f.Type)
	}
	var ack muxproto.RegisterAckMsg
	if err := muxproto.Unmarshal(f.Payload, &ack); err != nil {
		return fmt.Errorf("parse REGISTER_ACK: %w", err)
	}
	if !ack.OK {
		return fmt.Errorf("server rejected registration: %s", ack.Message)
	}

	// Rebuild streamsByID map.
	byID := make(map[uint32]*streamRecvState, len(ack.StreamIDs))
	for name, id := range ack.StreamIDs {
		if st, ok := s.streams[name]; ok {
			st.id = id
			byID[id] = st
		}
	}
	s.idMu.Lock()
	s.streamsByID = byID
	s.idMu.Unlock()

	s.logger.Info("registered with server", "stream_ids", ack.StreamIDs)

	// 3. Start ACK ticker goroutine.
	ackStop := make(chan struct{})
	go s.ackLoop(mc, ackStop)
	defer close(ackStop)

	// 4. Create pongCh and start keepalive goroutine.
	pongCh := make(chan struct{}, 4)
	s.pongCh = pongCh
	pingStop := make(chan struct{})
	go s.pingLoop(mc, pingStop, pongCh)
	defer close(pingStop)

	// 5. Read loop (blocks until connection dies or ctx cancelled).
	return s.readLoop(mc)
}

// readLoop processes DATA / RST / PING / PONG frames.
func (s *EdgeClientSession) readLoop(mc *muxproto.MuxConn) error {
	for {
		if s.ctx.Err() != nil {
			return nil
		}
		f, err := mc.ReadFrame()
		if err != nil {
			if s.ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read frame: %w", err)
		}

		switch f.Type {
		case muxproto.TypeData:
			s.handleData(f)
		case muxproto.TypeRST:
			var msg muxproto.RSTMsg
			if err := muxproto.Unmarshal(f.Payload, &msg); err == nil {
				s.handleRST(msg)
			}
		case muxproto.TypePing:
			mc.WriteFrame(&muxproto.Frame{Type: muxproto.TypePong}) //nolint:errcheck
		case muxproto.TypePong:
			select {
			case s.pongCh <- struct{}{}:
			default:
			}
		default:
			s.logger.Debug("unexpected frame type", "type", f.Type)
		}
	}
}

func (s *EdgeClientSession) handleData(f *muxproto.Frame) {
	s.idMu.RLock()
	st := s.streamsByID[f.StreamID]
	s.idMu.RUnlock()
	if st == nil {
		s.logger.Warn("DATA for unknown stream id", "id", f.StreamID)
		return
	}

	if err := st.buf.Write(s.ctx, f.Payload); err != nil {
		s.logger.Warn("recv buffer write error", "stream", st.name, "err", err)
		return
	}
	st.recvOffset.Add(int64(len(f.Payload)))

	if st.recvOffset.Load()-st.ackedOffset.Load() >= ackEveryBytes {
		s.sendAck(st)
	}
}

func (s *EdgeClientSession) handleRST(msg muxproto.RSTMsg) {
	s.idMu.RLock()
	st := s.streamsByID[msg.StreamID]
	s.idMu.RUnlock()
	if st == nil {
		return
	}
	var closeErr error
	if msg.Reason != "EOF" {
		closeErr = fmt.Errorf("server RST: %s", msg.Reason)
	}
	st.buf.Close(closeErr)
	s.logger.Info("stream closed by server",
		"stream", st.name, "reason", msg.Reason)
}

// pingLoop sends PING frames periodically and closes the connection on timeout.
func (s *EdgeClientSession) pingLoop(mc *muxproto.MuxConn, stop <-chan struct{}, pongCh <-chan struct{}) {
	ticker := time.NewTicker(clientPingInterval)
	defer ticker.Stop()
	lastPong := time.Now()

	for {
		select {
		case <-stop:
			return
		case <-s.ctx.Done():
			return
		case <-pongCh:
			lastPong = time.Now()
		case <-ticker.C:
			if time.Since(lastPong) > clientPingInterval+clientPingTimeout {
				s.logger.Warn("ping timeout, closing connection")
				mc.Close()
				return
			}
			mc.WriteFrame(&muxproto.Frame{Type: muxproto.TypePing}) //nolint:errcheck
		}
	}
}

// ackLoop periodically flushes pending ACKs for all streams.
func (s *EdgeClientSession) ackLoop(mc *muxproto.MuxConn, stop <-chan struct{}) {
	ticker := time.NewTicker(ackInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.idMu.RLock()
			sts := make([]*streamRecvState, 0, len(s.streamsByID))
			for _, st := range s.streamsByID {
				sts = append(sts, st)
			}
			s.idMu.RUnlock()
			for _, st := range sts {
				if st.recvOffset.Load() > st.ackedOffset.Load() {
					s.sendAckWithConn(mc, st)
				}
			}
		}
	}
}

func (s *EdgeClientSession) sendAck(st *streamRecvState) {
	s.connMu.Lock()
	mc := s.conn
	s.connMu.Unlock()
	if mc != nil {
		s.sendAckWithConn(mc, st)
	}
}

func (s *EdgeClientSession) sendAckWithConn(mc *muxproto.MuxConn, st *streamRecvState) {
	offset := st.recvOffset.Load()
	mc.WriteFrame(&muxproto.Frame{ //nolint:errcheck
		StreamID: muxproto.ControlStreamID,
		Type:     muxproto.TypeAck,
		Payload:  muxproto.Marshal(muxproto.AckMsg{StreamID: st.id, Offset: offset}),
	})
	st.ackedOffset.Store(offset)
}
