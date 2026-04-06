package muxproto

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// Stream represents one bidirectional request-response stream with per-direction
// flow control.
//
// Flow control prevents a slow consumer on one stream from blocking all other
// streams on the same connection.  Each direction has a send window (credit
// tracker) and the receiver sends WINDOW_UPDATE frames as it consumes bytes.
type Stream struct {
	ID   uint32
	conn *MuxConn

	reqHeaders  chan *RequestMeta
	respHeaders chan *ResponseMeta

	reqBody  *StreamPipe // server writes, client reads
	respBody *StreamPipe // client writes, server reads

	phase atomic.Int32 // 0 = request, 1 = response

	sendWindowReq  *windowTracker // server→client send window (Direction A)
	sendWindowResp *windowTracker // client→server send window (Direction B)

	closeOnce sync.Once
	closed    chan struct{}
}

type StreamPhase int32

const (
	phaseRequest  StreamPhase = 0
	phaseResponse StreamPhase = 1
)

func (StreamPhase) toInt32() int32 {
	return int32(phaseRequest)
}

const (
	pipeBufSize = 512 * 1024 // 512 KiB per direction

	// windowUpdateBatch: minimum bytes consumed before sending WINDOW_UPDATE.
	windowUpdateBatch = 32 * 1024

	// Binary WINDOW_UPDATE payload size: 4(increment) + 1(dir) = 5 bytes.
	// StreamID is taken from the frame header, not duplicated in payload.
	winUpdatePayloadSize = 5
)

func NewStream(id uint32, conn *MuxConn) *Stream {
	return &Stream{
		ID:             id,
		conn:           conn,
		reqHeaders:     make(chan *RequestMeta, 1),
		respHeaders:    make(chan *ResponseMeta, 1),
		reqBody:        NewStreamPipe(pipeBufSize),
		respBody:       NewStreamPipe(pipeBufSize),
		sendWindowReq:  newWindowTracker(int64(pipeBufSize)),
		sendWindowResp: newWindowTracker(int64(pipeBufSize)),
		closed:         make(chan struct{}),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// windowTracker — per-direction send-window credit tracker
// ─────────────────────────────────────────────────────────────────────────────

type windowTracker struct {
	mu    sync.Mutex
	cond  *sync.Cond
	avail int64
}

func newWindowTracker(initial int64) *windowTracker {
	wt := &windowTracker{avail: initial}
	wt.cond = sync.NewCond(&wt.mu)
	return wt
}

// Acquire blocks until n bytes of credit are available, then subtracts n.
// Optimized: fast path avoids spawning a watcher goroutine when credits are
// already available (common case under normal load).
func (wt *windowTracker) Acquire(ctx context.Context, n int64, closed <-chan struct{}) error {
	// Fast path: credits available — no goroutine needed.
	wt.mu.Lock()
	if wt.avail >= n {
		wt.avail -= n
		wt.mu.Unlock()
		return nil
	}
	wt.mu.Unlock()

	// Slow path: need to wait. Spawn watcher for ctx/closed cancellation.
	quit := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			wt.cond.Broadcast()
		case <-closed:
			wt.cond.Broadcast()
		case <-quit:
		}
	}()

	wt.mu.Lock()
	for wt.avail < n {
		if ctx.Err() != nil || isClosed(closed) {
			wt.mu.Unlock()
			close(quit)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrStreamReset
		}
		wt.cond.Wait()
	}
	wt.avail -= n
	wt.mu.Unlock()
	close(quit)
	return nil
}

func (wt *windowTracker) Release(n int64) {
	wt.mu.Lock()
	wt.avail += n
	wt.cond.Broadcast()
	wt.mu.Unlock()
}

func (wt *windowTracker) WakeAll() {
	wt.mu.Lock()
	wt.cond.Broadcast()
	wt.mu.Unlock()
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (s *Stream) Phase() StreamPhase {
	return StreamPhase(s.phase.Load())
}

// ─────────────────────────────────────────────────────────────────────────────
// Dispatcher methods — called by readLoop (must not block)
// ─────────────────────────────────────────────────────────────────────────────

func (s *Stream) OnHeaders(f *Frame) error {
	if s.Phase() == phaseRequest {
		var meta RequestMeta
		if err := Unmarshal(f.Payload, &meta); err != nil {
			return fmt.Errorf("parse request headers: %w", err)
		}
		select {
		case s.reqHeaders <- &meta:
		default:
		}
	} else {
		var meta ResponseMeta
		if err := Unmarshal(f.Payload, &meta); err != nil {
			return fmt.Errorf("parse response headers: %w", err)
		}
		select {
		case s.respHeaders <- &meta:
		default:
		}
	}
	return nil
}

// OnData writes incoming body data into the appropriate pipe (non-blocking).
func (s *Stream) OnData(f *Frame) error {
	phase := s.Phase()
	var pipe *StreamPipe
	if s.Phase() == phaseRequest {
		pipe = s.reqBody
	} else {
		pipe = s.respBody
	}

	if len(f.Payload) > 0 {
		if _, err := pipe.WriteNonBlock(f.Payload); err != nil {
			return fmt.Errorf("stream %d pipe full (flow control violation): %w", s.ID, err)
		}
	}
	if f.HasFlag(FlagEndStream) {
		pipe.CloseWrite(nil)
		if phase == phaseRequest {
			s.phase.Store(phaseResponse.toInt32())
		}
	}
	return nil
}

func (s *Stream) OnRST(reason string) {
	err := fmt.Errorf("%w: %s", ErrStreamReset, reason)
	s.reqBody.CloseWrite(err)
	s.respBody.CloseWrite(err)
	s.sendWindowReq.WakeAll()
	s.sendWindowResp.WakeAll()
	s.Close()
}

// OnWindowUpdate handles binary-encoded WINDOW_UPDATE payload.
func (s *Stream) OnWindowUpdate(payload []byte) {
	if len(payload) < winUpdatePayloadSize {
		return
	}
	increment := int32(binary.BigEndian.Uint32(payload[0:4]))
	dir := WinDir(payload[4])
	if dir == WinDirRequest {
		s.sendWindowReq.Release(int64(increment))
	} else {
		s.sendWindowResp.Release(int64(increment))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Consumer methods
// ─────────────────────────────────────────────────────────────────────────────

func (s *Stream) RecvRequestHeaders() (*RequestMeta, error) {
	select {
	case meta := <-s.reqHeaders:
		return meta, nil
	case <-s.closed:
		return nil, ErrStreamReset
	}
}

func (s *Stream) RecvResponseHeaders() (*ResponseMeta, error) {
	select {
	case meta := <-s.respHeaders:
		return meta, nil
	case <-s.closed:
		return nil, ErrStreamReset
	}
}

func (s *Stream) RecvResponseHeadersChan() <-chan *ResponseMeta {
	return s.respHeaders
}

func (s *Stream) ReqBody() *StreamPipe  { return s.reqBody }
func (s *Stream) RespBody() *StreamPipe { return s.respBody }

// ReqBodyReader returns a reader that auto-sends WINDOW_UPDATE(WinDirRequest).
func (s *Stream) ReqBodyReader() *WindowUpdateReader {
	return &WindowUpdateReader{pipe: s.reqBody, conn: s.conn, sid: s.ID, dir: WinDirRequest}
}

// RespBodyReader returns a reader that auto-sends WINDOW_UPDATE(WinDirResponse).
func (s *Stream) RespBodyReader() *WindowUpdateReader {
	return &WindowUpdateReader{pipe: s.respBody, conn: s.conn, sid: s.ID, dir: WinDirResponse}
}

func (s *Stream) AcquireReqWindow(ctx context.Context, n int64) error {
	return s.sendWindowReq.Acquire(ctx, n, s.closed)
}

func (s *Stream) AcquireRespWindow(ctx context.Context, n int64) error {
	return s.sendWindowResp.Acquire(ctx, n, s.closed)
}

// ─────────────────────────────────────────────────────────────────────────────
// Sending helpers
// ─────────────────────────────────────────────────────────────────────────────

func (s *Stream) SendHeaders(meta any, flags Flags) error {
	return s.conn.WriteFrame(&Frame{
		StreamID: s.ID, Type: TypeHeaders, Flags: flags, Payload: Marshal(meta),
	})
}

func (s *Stream) SendData(data []byte, flags Flags) error {
	return s.conn.WriteFrame(&Frame{
		StreamID: s.ID, Type: TypeData, Flags: flags, Payload: data,
	})
}

func (s *Stream) SendRST(reason string) error {
	return s.conn.WriteFrame(&Frame{
		StreamID: s.ID, Type: TypeRST, Payload: Marshal(RSTMsg{Reason: reason}),
	})
}

// SendBodyWithFlowControl reads from body and sends DATA frames with per-chunk
// window acquisition. dir selects which window to acquire from.
// Extracted as a Stream method to eliminate code duplication between server and client.
func (s *Stream) SendBodyWithFlowControl(ctx context.Context, body io.Reader, dir WinDir) error {
	if body == nil {
		return s.SendData(nil, FlagEndStream)
	}

	buf := chunkBufPool.Get().([]byte)
	defer chunkBufPool.Put(buf)

	for {
		n, readErr := body.Read(buf)
		if n > 0 {
			var flags Flags
			var acquireErr error
			if dir == WinDirRequest {
				acquireErr = s.AcquireReqWindow(ctx, int64(n))
			} else {
				acquireErr = s.AcquireRespWindow(ctx, int64(n))
			}

			if acquireErr != nil {
				return fmt.Errorf("window acquire: %w", acquireErr)
			}

			if sendErr := s.SendData(buf[:n], flags); sendErr != nil {
				return fmt.Errorf("send body DATA: %w", sendErr)
			}
		}

		if readErr == io.EOF {
			return s.SendData(nil, FlagEndStream)
		} else if readErr != nil {
			return readErr
		}
	}
}

func (s *Stream) Close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.sendWindowReq.WakeAll()
		s.sendWindowResp.WakeAll()
	})
}

func (s *Stream) Done() <-chan struct{} { return s.closed }

// ─────────────────────────────────────────────────────────────────────────────
// WindowUpdateReader — sends binary WINDOW_UPDATE as bytes are consumed
// ─────────────────────────────────────────────────────────────────────────────

type WindowUpdateReader struct {
	pipe       *StreamPipe
	conn       *MuxConn
	sid        uint32
	dir        WinDir
	pendingInc int32
}

func (r *WindowUpdateReader) Read(p []byte) (int, error) {
	n, err := r.pipe.Read(p)
	if n > 0 {
		r.pendingInc += int32(n)
		if r.pendingInc >= windowUpdateBatch || err != nil {
			r.flush()
		}
	}
	return n, err
}

func (r *WindowUpdateReader) flush() {
	if r.pendingInc <= 0 {
		return
	}
	// Binary encode: 4(increment) + 1(dir) = 5 bytes.
	var payload [winUpdatePayloadSize]byte
	binary.BigEndian.PutUint32(payload[0:4], uint32(r.pendingInc))
	payload[4] = byte(r.dir)

	if err := r.conn.WriteFrame(&Frame{
		StreamID: r.sid,
		Type:     TypeWindowUpdate,
		Payload:  payload[:],
	}); err != nil {
		// Don't clear pendingInc — connection is likely dead.
		// The session's readLoop will detect the broken connection and clean up.
		return
	}
	r.pendingInc = 0
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared pool for DATA chunk buffers (32 KiB)
// ─────────────────────────────────────────────────────────────────────────────

var chunkBufPool = sync.Pool{
	New: func() any { return make([]byte, DataChunkSize) },
}
