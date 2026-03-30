package muxproto

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Stream represents one bidirectional request-response stream.
//
// A stream has two phases:
//   Phase 1: Initiator sends HEADERS + DATA (request body) → peer
//   Phase 2: Peer sends HEADERS + DATA (response body) → initiator
//
// Each direction has its own StreamPipe for body data.
// Headers are delivered as parsed structs via channels.
type Stream struct {
	ID     uint32
	conn   *MuxConn
	logger interface{ Debug(string, ...any) } // accepts *slog.Logger

	// reqHeaders receives the parsed RequestMeta from a HEADERS frame.
	// Buffered(1) so the dispatcher can write without blocking.
	reqHeaders chan *RequestMeta

	// respHeaders receives the parsed ResponseMeta from a HEADERS frame.
	respHeaders chan *ResponseMeta

	// reqBody is written by the dispatcher when DATA frames arrive (request direction).
	// Read by the stream consumer (client-side handler).
	reqBody *StreamPipe

	// respBody is written by the dispatcher when DATA frames arrive (response direction).
	// Read by the stream initiator (server-side caller).
	respBody *StreamPipe

	// phase tracks whether we're in request or response phase.
	// 0 = request (server→client), 1 = response (client→server).
	phase atomic.Int32

	closeOnce sync.Once
	closed    chan struct{}
}

const (
	phaseRequest  = 0
	phaseResponse = 1
	pipeBufSize   = 512 * 1024 // 512 KiB per direction
)

func NewStream(id uint32, conn *MuxConn) *Stream {
	return &Stream{
		ID:          id,
		conn:        conn,
		reqHeaders:  make(chan *RequestMeta, 1),
		respHeaders: make(chan *ResponseMeta, 1),
		reqBody:     NewStreamPipe(pipeBufSize),
		respBody:    NewStreamPipe(pipeBufSize),
		closed:      make(chan struct{}),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Dispatcher methods — called by the readLoop when frames for this stream arrive
// ─────────────────────────────────────────────────────────────────────────────

// OnHeaders is called by the dispatcher when a HEADERS frame arrives.
func (s *Stream) OnHeaders(f *Frame) error {
	phase := s.phase.Load()
	if phase == phaseRequest {
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

// OnData is called by the dispatcher when a DATA frame arrives.
func (s *Stream) OnData(f *Frame) error {
	phase := s.phase.Load()
	var pipe *StreamPipe
	if phase == phaseRequest {
		pipe = s.reqBody
	} else {
		pipe = s.respBody
	}

	if len(f.Payload) > 0 {
		if _, err := pipe.Write(f.Payload); err != nil {
			return err
		}
	}
	if f.HasFlag(FlagEndStream) {
		pipe.CloseWrite(nil) // signal EOF to reader
		if phase == phaseRequest {
			// Transition to response phase — client will now send response headers + body
			s.phase.Store(phaseResponse)
		}
		// If phase == response, both directions are done → stream is complete
	}
	return nil
}

// OnRST is called when the peer resets this stream.
func (s *Stream) OnRST(reason string) {
	err := fmt.Errorf("%w: %s", ErrStreamReset, reason)
	s.reqBody.CloseWrite(err)
	s.respBody.CloseWrite(err)
	s.Close()
}

// ─────────────────────────────────────────────────────────────────────────────
// Consumer methods — used by server/client code to interact with the stream
// ─────────────────────────────────────────────────────────────────────────────

// RecvRequestHeaders blocks until the request headers arrive (client side).
func (s *Stream) RecvRequestHeaders() (*RequestMeta, error) {
	select {
	case meta := <-s.reqHeaders:
		return meta, nil
	case <-s.closed:
		return nil, ErrStreamReset
	}
}

// RecvResponseHeaders blocks until the response headers arrive (server side).
func (s *Stream) RecvResponseHeaders() (*ResponseMeta, error) {
	select {
	case meta := <-s.respHeaders:
		return meta, nil
	case <-s.closed:
		return nil, ErrStreamReset
	}
}

// RecvResponseHeadersChan returns the channel for select-based waiting.
func (s *Stream) RecvResponseHeadersChan() <-chan *ResponseMeta {
	return s.respHeaders
}

// ReqBody returns the reader for the request body (client side reads this).
func (s *Stream) ReqBody() *StreamPipe { return s.reqBody }

// RespBody returns the reader for the response body (server side reads this).
func (s *Stream) RespBody() *StreamPipe { return s.respBody }

// ─────────────────────────────────────────────────────────────────────────────
// Sending methods — write frames to the peer
// ─────────────────────────────────────────────────────────────────────────────

// SendHeaders sends a HEADERS frame with the given JSON-serializable metadata.
func (s *Stream) SendHeaders(meta any, flags Flags) error {
	return s.conn.WriteFrame(&Frame{
		StreamID: s.ID,
		Type:     TypeHeaders,
		Flags:    flags,
		Payload:  Marshal(meta),
	})
}

// SendData sends a DATA frame. Set FlagEndStream on the last chunk.
func (s *Stream) SendData(data []byte, flags Flags) error {
	return s.conn.WriteFrame(&Frame{
		StreamID: s.ID,
		Type:     TypeData,
		Flags:    flags,
		Payload:  data,
	})
}

// SendBodyFromReader reads from r and sends DATA frames in chunks.
// The last frame carries FlagEndStream.
func (s *Stream) SendBodyFromReader(r interface{ Read([]byte) (int, error) }) error {
	buf := make([]byte, DataChunkSize)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			var flags Flags
			if err != nil { // EOF or error → this is the last chunk
				flags = FlagEndStream
			}
			if sendErr := s.SendData(buf[:n], flags); sendErr != nil {
				return sendErr
			}
		}
		if err != nil {
			if err.Error() == "EOF" { // io.EOF
				if n == 0 {
					// No data in this read, but need to send END_STREAM
					return s.SendData(nil, FlagEndStream)
				}
				return nil // END_STREAM was already set on the last data frame
			}
			return err
		}
	}
}

// SendRST sends a RST frame to abort this stream.
func (s *Stream) SendRST(reason string) error {
	return s.conn.WriteFrame(&Frame{
		StreamID: s.ID,
		Type:     TypeRST,
		Payload:  Marshal(RSTMsg{Reason: reason}),
	})
}

// Close marks the stream as done. Idempotent.
func (s *Stream) Close() {
	s.closeOnce.Do(func() {
		close(s.closed)
	})
}

// Done returns a channel that is closed when the stream is finished.
func (s *Stream) Done() <-chan struct{} { return s.closed }
