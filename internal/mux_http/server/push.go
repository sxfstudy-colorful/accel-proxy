package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux_http/muxproto"
)

const (
	bufferMaxRetain   = 512 * 1024 * 1024 // 512 MiB retained per stream
	trimInterval      = 5 * time.Second
	evictLagThreshold = int64(bufferMaxRetain) * 8 / 10 // 80%
)

// PushBroker is the central coordinator.
type PushBroker struct {
	logger  *slog.Logger
	nodes   *NodeRegistry
	subs    *SubscriptionTable
	streams *StreamRegistry

	ackMu    sync.Mutex
	ackTable map[ackKey]int64

	pongMu    sync.Mutex
	pongTable map[string]chan time.Time

	// FIX: lifecycle management — trimLoop stops when ctx is cancelled.
	ctx    context.Context
	cancel context.CancelFunc
}

type ackKey struct {
	nodeID   string
	streamID uint32
}

func NewPushBroker(logger *slog.Logger) *PushBroker {
	ctx, cancel := context.WithCancel(context.Background())
	b := &PushBroker{
		logger:    logger,
		nodes:     NewNodeRegistry(),
		subs:      NewSubscriptionTable(),
		streams:   NewStreamRegistry(bufferMaxRetain),
		ackTable:  make(map[ackKey]int64),
		pongTable: make(map[string]chan time.Time),
		ctx:       ctx,
		cancel:    cancel,
	}
	go b.trimLoop()
	return b
}

// Close stops all background goroutines (trimLoop, etc.).
func (b *PushBroker) Close() {
	b.cancel()
}

// ─────────────────────────────────────────────────────────────────────────────
// Pong tracking
// ─────────────────────────────────────────────────────────────────────────────

func (b *PushBroker) registerPongCh(nodeID string) <-chan time.Time {
	ch := make(chan time.Time, 4)
	b.pongMu.Lock()
	b.pongTable[nodeID] = ch
	b.pongMu.Unlock()
	return ch
}

func (b *PushBroker) unregisterPongCh(nodeID string) {
	b.pongMu.Lock()
	delete(b.pongTable, nodeID)
	b.pongMu.Unlock()
}

func (b *PushBroker) onPong(nodeID string) {
	b.pongMu.Lock()
	ch := b.pongTable[nodeID]
	b.pongMu.Unlock()
	if ch != nil {
		select {
		case ch <- time.Now():
		default:
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge connection handling
// ─────────────────────────────────────────────────────────────────────────────

func (b *PushBroker) HandleEdgeConn(conn *muxproto.MuxConn) {
	logger := b.logger.With("remote", conn.RemoteAddr())

	f, err := conn.ReadFrame()
	if err != nil {
		logger.Warn("edge: read REGISTER failed", "err", err)
		conn.Close()
		return
	}
	if f.Type != muxproto.TypeRegister || f.StreamID != muxproto.ControlStreamID {
		logger.Warn("edge: expected REGISTER", "got_type", f.Type)
		conn.Close()
		return
	}

	var reg muxproto.RegisterMsg
	if err := muxproto.Unmarshal(f.Payload, &reg); err != nil || reg.NodeID == "" {
		logger.Warn("edge: bad REGISTER payload", "err", err)
		conn.Close()
		return
	}
	logger = logger.With("node_id", reg.NodeID)
	logger.Info("edge connected", "subs", reg.Subs, "resume", reg.Resume)

	sess := newEdgeSession(reg.NodeID, conn, b, logger)

	streamIDs := make(map[string]uint32, len(reg.Subs))
	for _, name := range reg.Subs {
		entry := b.streams.GetOrCreate(name)
		st := sess.addStream(entry)
		streamIDs[name] = entry.ID
		b.subs.Subscribe(name, reg.NodeID)

		if reg.Resume != nil {
			if off, ok := reg.Resume[name]; ok {
				st.sendOffset.Store(off)
				st.ackedOffset.Store(off)
			}
		}
	}

	ackFrame := &muxproto.Frame{
		StreamID: muxproto.ControlStreamID,
		Type:     muxproto.TypeRegisterAck,
		Payload: muxproto.Marshal(muxproto.RegisterAckMsg{
			OK:        true,
			StreamIDs: streamIDs,
		}),
	}
	if err := conn.WriteFrame(ackFrame); err != nil {
		logger.Warn("edge: write REGISTER_ACK failed", "err", err)
		conn.Close()
		return
	}

	b.nodes.Register(reg.NodeID, sess)

	for _, name := range reg.Subs {
		entry := b.streams.GetOrCreate(name)
		sess.mu.Lock()
		st := sess.streams[entry.ID]
		sess.mu.Unlock()
		if st != nil {
			sess.startSender(st, st.sendOffset.Load())
		}
	}

	sess.readLoop()

	b.nodes.Unregister(reg.NodeID, sess)
	b.subs.Unsubscribe(reg.NodeID)
	logger.Info("edge session closed")
}

// ─────────────────────────────────────────────────────────────────────────────
// Origin push
// ─────────────────────────────────────────────────────────────────────────────

func (b *PushBroker) Push(ctx context.Context, streamName string, r io.Reader) (int64, error) {
	entry := b.streams.GetOrCreate(streamName)

	buf := make([]byte, 256*1024)
	var total int64

	for {
		if ctx.Err() != nil {
			entry.Buffer.Close(ctx.Err())
			return total, ctx.Err()
		}
		n, err := r.Read(buf)
		if n > 0 {
			if aerr := entry.Buffer.Append(buf[:n]); aerr != nil {
				entry.Buffer.Close(aerr)
				return total, aerr
			}
			total += int64(n)
		}
		if err == io.EOF {
			entry.Buffer.Close(nil)
			return total, nil
		}
		if err != nil {
			entry.Buffer.Close(err)
			return total, err
		}
	}
}

func (b *PushBroker) StreamInfo() map[string]int64 {
	names := b.streams.Names()
	out := make(map[string]int64, len(names))
	for _, name := range names {
		if e := b.streams.Get(name); e != nil {
			out[name] = e.Buffer.WriteOffset()
		}
	}
	return out
}

func (b *PushBroker) NodeIDs() []string {
	sessions := b.nodes.All()
	ids := make([]string, len(sessions))
	for i, s := range sessions {
		ids[i] = s.nodeID
	}
	return ids
}

// ─────────────────────────────────────────────────────────────────────────────
// ACK tracking + buffer trimming
// ─────────────────────────────────────────────────────────────────────────────

func (b *PushBroker) onAck(nodeID string, streamID uint32, offset int64) {
	b.ackMu.Lock()
	k := ackKey{nodeID, streamID}
	if offset > b.ackTable[k] {
		b.ackTable[k] = offset
	}
	b.ackMu.Unlock()

	entry, err := b.streams.GetByID(streamID)
	if err != nil {
		return
	}
	lag := entry.Buffer.WriteOffset() - offset
	if lag > evictLagThreshold {
		if sess := b.nodes.Get(nodeID); sess != nil {
			sess.closeWithReason(fmt.Sprintf(
				"stream %s lag %d bytes exceeds threshold", entry.Name, lag))
		}
	}
}

// trimLoop runs in the background, trimming buffers and cleaning up finished
// streams. FIX: now stoppable via ctx.
func (b *PushBroker) trimLoop() {
	ticker := time.NewTicker(trimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.trimAll()
		}
	}
}

func (b *PushBroker) trimAll() {
	// Build: streamID → minimum acked offset across all subscribers.
	type minAck struct {
		offset int64
		set    bool
	}
	byStream := make(map[uint32]*minAck)

	b.ackMu.Lock()
	for k, off := range b.ackTable {
		ma, ok := byStream[k.streamID]
		if !ok {
			ma = &minAck{}
			byStream[k.streamID] = ma
		}
		if !ma.set || off < ma.offset {
			ma.offset = off
			ma.set = true
		}
	}
	b.ackMu.Unlock()

	for streamID, ma := range byStream {
		if !ma.set {
			continue
		}
		entry, err := b.streams.GetByID(streamID)
		if err != nil {
			continue
		}
		entry.Buffer.Trim(ma.offset)
	}

	// FIX: clean up closed streams with no subscribers.
	for _, name := range b.streams.ClosedStreams() {
		subscribers := b.subs.Subscribers(name)
		if len(subscribers) == 0 {
			entry := b.streams.Get(name)
			if entry != nil {
				b.ackMu.Lock()
				for k := range b.ackTable {
					if k.streamID == entry.ID {
						delete(b.ackTable, k)
					}
				}
				b.ackMu.Unlock()
				b.streams.Remove(name)
				b.logger.Debug("removed finished stream", "stream", name)
			}
		}
	}
}
