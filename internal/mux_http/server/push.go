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

// PushBroker is the central coordinator:
//   - accepts edge node connections and drives their sessions
//   - accepts origin push requests and writes into stream buffers
//   - tracks ACKs, trims buffers, and evicts slow edges
type PushBroker struct {
	logger  *slog.Logger
	nodes   *NodeRegistry
	subs    *SubscriptionTable
	streams *StreamRegistry

	// ackTable: latest acked offset per (nodeID, streamID)
	ackMu    sync.Mutex
	ackTable map[ackKey]int64

	// pongTable: per-nodeID channel used by pingLoop to receive pong timestamps
	pongMu    sync.Mutex
	pongTable map[string]chan time.Time
}

type ackKey struct {
	nodeID   string
	streamID uint32
}

func NewPushBroker(logger *slog.Logger) *PushBroker {
	b := &PushBroker{
		logger:    logger,
		nodes:     NewNodeRegistry(),
		subs:      NewSubscriptionTable(),
		streams:   NewStreamRegistry(bufferMaxRetain),
		ackTable:  make(map[ackKey]int64),
		pongTable: make(map[string]chan time.Time),
	}
	go b.trimLoop()
	return b
}

// ─────────────────────────────────────────────────────────────────────────────
// Pong tracking (used by pingLoop in session.go)
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

// HandleEdgeConn is called for every new TCP connection from an edge node
// (forwarded by accel-proxy).  Performs the REGISTER handshake then drives
// the session until the connection closes.
func (b *PushBroker) HandleEdgeConn(conn *muxproto.MuxConn) {
	logger := b.logger.With("remote", conn.RemoteAddr())

	// 1. Expect REGISTER frame.
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

	// 2. Create session and stream send states.
	sess := newEdgeSession(reg.NodeID, conn, b, logger)

	streamIDs := make(map[string]uint32, len(reg.Subs))
	for _, name := range reg.Subs {
		entry := b.streams.GetOrCreate(name)
		st := sess.addStream(entry)
		streamIDs[name] = entry.ID
		b.subs.Subscribe(name, reg.NodeID)

		// Apply resume offset if provided.
		if reg.Resume != nil {
			if off, ok := reg.Resume[name]; ok {
				st.sendOffset = off
				st.ackedOffset.Store(off)
			}
		}
	}

	// 3. Send REGISTER_ACK.
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

	// 4. Register node and start per-stream sender goroutines.
	b.nodes.Register(reg.NodeID, sess)

	for _, name := range reg.Subs {
		entry := b.streams.GetOrCreate(name)
		sess.mu.Lock()
		st := sess.streams[entry.ID]
		sess.mu.Unlock()
		if st != nil {
			sess.startSender(st, st.sendOffset)
		}
	}

	// 5. Drive the read loop (blocks until connection closes).
	sess.readLoop()

	// 6. Cleanup.
	b.nodes.Unregister(reg.NodeID, sess)
	b.subs.Unsubscribe(reg.NodeID)
	logger.Info("edge session closed")
}

// ─────────────────────────────────────────────────────────────────────────────
// Origin push
// ─────────────────────────────────────────────────────────────────────────────

// Push reads from r and appends all bytes into the named stream's buffer.
// Fan-out to subscribed edge nodes happens in real time as data is appended.
// Blocks until r returns io.EOF or an error.
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

// StreamInfo returns a snapshot of stream names → write offsets.
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

// NodeIDs returns the IDs of currently connected edge nodes.
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

	// Evict edge if it is lagging too far behind.
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

func (b *PushBroker) trimLoop() {
	ticker := time.NewTicker(trimInterval)
	defer ticker.Stop()
	for range ticker.C {
		b.trimAll()
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
}
