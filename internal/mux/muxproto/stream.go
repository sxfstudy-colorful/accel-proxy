package muxproto

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Stream represents one bidirectional request-response stream.
//
// A stream has two phases:
//
//	Phase 1: Server sends HEADERS + DATA (request body) → client
//	Phase 2: Client sends HEADERS + DATA (response body) → server
//
// Flow control (per direction):
//
//	Direction A (req body, server→client):
//	  Server tracks sendWindowReq.  Before sending each DATA chunk the server
//	  calls AcquireReqWindow(n).  When the client reads from ReqBody it sends
//	  a WINDOW_UPDATE(WinDirRequest, n), which calls OnWindowUpdate and
//	  releases the credits so the server can proceed.
//
//	Direction B (resp body, client→server):
//	  Client tracks sendWindowResp.  Same mechanism, roles swapped.
//
// Both windows are initialised to pipeBufSize so the remote StreamPipe can
// always absorb the maximum in-flight bytes without blocking the readLoop.
type Stream struct {
	ID   uint32
	conn *MuxConn

	// reqHeaders/respHeaders deliver parsed metadata frames.
	reqHeaders  chan *RequestMeta
	respHeaders chan *ResponseMeta

	// reqBody  — request body pipe  (server writes, client reads)
	// respBody — response body pipe (client writes, server reads)
	reqBody  *StreamPipe
	respBody *StreamPipe

	// phase: 0 = request direction active, 1 = response direction active.
	phase atomic.Int32

	// sendWindowReq — server-side send window for Direction A.
	// AcquireReqWindow blocks the server goroutine until credits are available.
	// Released by OnWindowUpdate(WinDirRequest, n) which is driven by the
	// client's windowUpdateReader as it consumes bytes from reqBody.
	sendWindowReq *windowTracker

	// sendWindowResp — client-side send window for Direction B.
	// AcquireRespWindow blocks the client goroutine.
	// Released by OnWindowUpdate(WinDirResponse, n) driven by the server's
	// windowUpdateReader as it consumes bytes from respBody.
	sendWindowResp *windowTracker

	closeOnce sync.Once
	closed    chan struct{}
}

const (
	phaseRequest  = 0
	phaseResponse = 1
	pipeBufSize   = 512 * 1024 // 512 KiB per direction

	// windowUpdateBatch: minimum bytes consumed before sending a WINDOW_UPDATE.
	// Batching prevents a flood of tiny frames on small reads.
	windowUpdateBatch = 32 * 1024 // 32 KiB
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
// windowTracker — per-direction send-window bookkeeping
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

// Acquire blocks until n bytes of send credit are available (or ctx/closed
// fires).  It subtracts n from the available window atomically on success.
func (wt *windowTracker) Acquire(ctx context.Context, n int64, closed <-chan struct{}) error {
	// Watcher goroutine: wake the cond when ctx/closed fires so the Wait
	// loop can check exit conditions without busy-spinning.
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

// Release adds n bytes back to the available window and wakes any waiter.
// Called from OnWindowUpdate — must not block.
func (wt *windowTracker) Release(n int64) {
	wt.mu.Lock()
	wt.avail += n
	wt.cond.Broadcast()
	wt.mu.Unlock()
}

// WakeAll wakes all waiters without changing the window (used on stream close).
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

// ─────────────────────────────────────────────────────────────────────────────
// Dispatcher methods — called by the readLoop (must not block)
// ─────────────────────────────────────────────────────────────────────────────

func (s *Stream) OnHeaders(f *Frame) error {
	if s.phase.Load() == phaseRequest {
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

// OnData is called by the readLoop when a DATA frame arrives.
//
// Because we use per-stream flow control (sendWindowReq / sendWindowResp),
// the remote sender only transmits as many bytes as the local pipe can hold.
// Therefore StreamPipe.Write should never block here — if it does it means
// the sender violated the window protocol, and we return an error to let the
// readLoop close the session.
func (s *Stream) OnData(f *Frame) error {
	phase := s.phase.Load()
	var pipe *StreamPipe
	if phase == phaseRequest {
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
			s.phase.Store(phaseResponse)
		}
	}
	return nil
}

// OnRST is called when the peer resets this stream.
func (s *Stream) OnRST(reason string) {
	err := fmt.Errorf("%w: %s", ErrStreamReset, reason)
	s.reqBody.CloseWrite(err)
	s.respBody.CloseWrite(err)
	// Wake any goroutine blocked in AcquireReqWindow / AcquireRespWindow.
	s.sendWindowReq.WakeAll()
	s.sendWindowResp.WakeAll()
	s.Close()
}

// OnWindowUpdate is called by the dispatcher when a WINDOW_UPDATE frame
// arrives for this stream.  Must not block.
func (s *Stream) OnWindowUpdate(dir WinDir, increment int32) {
	if dir == WinDirRequest {
		s.sendWindowReq.Release(int64(increment))
	} else {
		s.sendWindowResp.Release(int64(increment))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Consumer / sender methods
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

// ReqBody returns the raw request-body pipe.
// Prefer ReqBodyReader which auto-sends WINDOW_UPDATE as bytes are consumed.
func (s *Stream) ReqBody() *StreamPipe { return s.reqBody }

// RespBody returns the raw response-body pipe.
// Prefer RespBodyReader which auto-sends WINDOW_UPDATE as bytes are consumed.
func (s *Stream) RespBody() *StreamPipe { return s.respBody }

// ReqBodyReader returns an io.Reader wrapping reqBody that automatically sends
// WINDOW_UPDATE(WinDirRequest) frames as bytes are consumed.  The server's
// send window is replenished in real time, keeping the pipeline full without
// over-filling the local pipe.
func (s *Stream) ReqBodyReader() *WindowUpdateReader {
	return &WindowUpdateReader{
		pipe: s.reqBody,
		conn: s.conn,
		sid:  s.ID,
		dir:  WinDirRequest,
	}
}

// RespBodyReader returns an io.Reader wrapping respBody that automatically
// sends WINDOW_UPDATE(WinDirResponse) frames as bytes are consumed.
func (s *Stream) RespBodyReader() *WindowUpdateReader {
	return &WindowUpdateReader{
		pipe: s.respBody,
		conn: s.conn,
		sid:  s.ID,
		dir:  WinDirResponse,
	}
}

// AcquireReqWindow blocks the calling goroutine until n bytes of Direction-A
// send credit are available, then subtracts n from the window.
// Used by the server before sending each request-body DATA chunk.
func (s *Stream) AcquireReqWindow(ctx context.Context, n int64) error {
	return s.sendWindowReq.Acquire(ctx, n, s.closed)
}

// AcquireRespWindow blocks until n bytes of Direction-B send credit are
// available.  Used by the client before sending each response-body DATA chunk.
func (s *Stream) AcquireRespWindow(ctx context.Context, n int64) error {
	return s.sendWindowResp.Acquire(ctx, n, s.closed)
}

// ─────────────────────────────────────────────────────────────────────────────
// Sending helpers
// ─────────────────────────────────────────────────────────────────────────────

func (s *Stream) SendHeaders(meta any, flags Flags) error {
	return s.conn.WriteFrame(&Frame{
		StreamID: s.ID,
		Type:     TypeHeaders,
		Flags:    flags,
		Payload:  Marshal(meta),
	})
}

func (s *Stream) SendData(data []byte, flags Flags) error {
	return s.conn.WriteFrame(&Frame{
		StreamID: s.ID,
		Type:     TypeData,
		Flags:    flags,
		Payload:  data,
	})
}

func (s *Stream) SendRST(reason string) error {
	return s.conn.WriteFrame(&Frame{
		StreamID: s.ID,
		Type:     TypeRST,
		Payload:  Marshal(RSTMsg{Reason: reason}),
	})
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
// WindowUpdateReader — sends WINDOW_UPDATE as bytes are consumed
// ─────────────────────────────────────────────────────────────────────────────

// WindowUpdateReader wraps a StreamPipe and sends batched WINDOW_UPDATE frames
// as bytes are read, replenishing the remote sender's window in real time.
//
// This is the key mechanism that prevents the readLoop from blocking:
//
//	readLoop reads DATA frame → OnData writes to pipe (non-blocking, window ensures room)
//	Consumer reads from WindowUpdateReader → sends WINDOW_UPDATE → server sends more
type WindowUpdateReader struct {
	pipe       *StreamPipe
	conn       *MuxConn
	sid        uint32
	dir        WinDir
	pendingInc int32 // bytes consumed since last WINDOW_UPDATE
}

func (r *WindowUpdateReader) Read(p []byte) (int, error) {
	n, err := r.pipe.Read(p)
	if n > 0 {
		r.pendingInc += int32(n)
		// Flush WINDOW_UPDATE when we've accumulated enough or on stream end.
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
	r.conn.WriteFrame(&Frame{ //nolint:errcheck
		StreamID: r.sid,
		Type:     TypeWindowUpdate,
		Payload: Marshal(WindowUpdateMsg{
			StreamID:  r.sid,
			Increment: r.pendingInc,
			Dir:       r.dir,
		}),
	})
	r.pendingInc = 0
}
