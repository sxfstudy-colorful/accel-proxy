package server

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux_http/muxproto"
)

const segmentSize = 4 * 1024 * 1024 // 4 MiB per segment

type segment struct {
	baseOffset int64
	data       []byte
}

// StreamBuffer is a concurrent append-only byte store for one push stream.
//
// Append is called by the PushBroker (origin writer goroutine).
// Read + WaitForData are called by EdgeSession sender goroutines (one per
// subscribed edge node).
//
// Back-pressure: Append blocks when retained bytes ≥ maxRetain.
// Trim is called periodically to free segments all subscribers have ACKed.
type StreamBuffer struct {
	mu   sync.Mutex
	cond *sync.Cond

	segments    []*segment
	baseOffset  int64 // byte offset of segments[0].data[0]
	writeOffset int64 // total bytes ever appended

	maxRetain int64

	closed   bool
	closeErr error // set to io.EOF on clean close
}

func NewStreamBuffer(maxRetain int64) *StreamBuffer {
	b := &StreamBuffer{maxRetain: maxRetain}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Append adds data to the stream.
// Blocks when the buffer is full (back-pressures the origin reader).
func (b *StreamBuffer) Append(data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for !b.closed && b.writeOffset-b.baseOffset >= b.maxRetain {
		b.cond.Wait()
	}
	if b.closed {
		return fmt.Errorf("stream buffer closed")
	}

	written := 0
	for written < len(data) {
		if len(b.segments) == 0 || len(b.segments[len(b.segments)-1].data) >= segmentSize {
			b.segments = append(b.segments, &segment{
				baseOffset: b.writeOffset + int64(written),
				data:       make([]byte, 0, segmentSize),
			})
		}
		last := b.segments[len(b.segments)-1]
		avail := segmentSize - len(last.data)
		n := len(data) - written
		if n > avail {
			n = avail
		}
		last.data = append(last.data, data[written:written+n]...)
		written += n
	}
	b.writeOffset += int64(len(data))
	b.cond.Broadcast()
	return nil
}

// Close marks the stream finished.  Pass nil for a clean EOF.
func (b *StreamBuffer) Close(closeErr error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	if closeErr == nil {
		closeErr = io.EOF
	}
	b.closeErr = closeErr
	b.cond.Broadcast()
}

// WriteOffset returns total bytes written so far.
func (b *StreamBuffer) WriteOffset() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.writeOffset
}

// Read reads up to len(buf) bytes starting at the given absolute offset.
//
//   - (n>0, nil)   data read successfully
//   - (0,  nil)    no data yet at offset — call WaitForData then retry
//   - (0,  io.EOF) stream closed cleanly and all data has been consumed
//   - (0,  err)    origin error or muxproto.ErrOffsetEvicted
func (b *StreamBuffer) Read(offset int64, buf []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if offset < b.baseOffset {
		return 0, muxproto.ErrOffsetEvicted
	}
	if offset >= b.writeOffset {
		if b.closed {
			return 0, b.closeErr
		}
		return 0, nil
	}

	n := 0
	for _, seg := range b.segments {
		segEnd := seg.baseOffset + int64(len(seg.data))
		if segEnd <= offset {
			continue // entirely before the requested offset
		}
		// start within this segment
		start := offset + int64(n) - seg.baseOffset
		if start < 0 {
			start = 0
		}
		end := int64(len(seg.data))
		want := int64(len(buf) - n)
		if end-start > want {
			end = start + want
		}
		copy(buf[n:], seg.data[start:end])
		n += int(end - start)
		if n >= len(buf) {
			break
		}
	}
	return n, nil
}

// WaitForData blocks until data is available past offset, the stream is
// closed, or ctx is cancelled.  It is safe to call concurrently.
//
// Implementation: a helper goroutine watches ctx and calls cond.Broadcast()
// when the context fires, so the cond.Wait() loop wakes cleanly without
// busy-spinning.  The helper is bounded to the lifetime of this call.
func (b *StreamBuffer) WaitForData(ctx context.Context, offset int64) {
	b.mu.Lock()
	// Fast path: condition already satisfied.
	if b.closed || b.writeOffset > offset || ctx.Err() != nil {
		b.mu.Unlock()
		return
	}

	// Slow path: spawn watcher, then wait.
	quit := make(chan struct{}) // closed when WaitForData returns
	go func() {
		select {
		case <-ctx.Done():
			b.cond.Broadcast()
		case <-quit:
			// WaitForData returned normally; nothing to do.
		}
	}()

	for !b.closed && b.writeOffset <= offset && ctx.Err() == nil {
		b.cond.Wait()
	}
	b.mu.Unlock()
	close(quit)
}

// Trim frees all segments whose data ends at or before minOffset.
// Called by the PushBroker with the minimum ACKed offset across all subscribers.
func (b *StreamBuffer) Trim(minOffset int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	trimmed := 0
	for _, seg := range b.segments {
		if seg.baseOffset+int64(len(seg.data)) <= minOffset {
			trimmed++
		} else {
			break
		}
	}
	if trimmed == 0 {
		return
	}
	b.segments = b.segments[trimmed:]
	if len(b.segments) > 0 {
		b.baseOffset = b.segments[0].baseOffset
	} else {
		b.baseOffset = b.writeOffset
	}
	b.cond.Broadcast() // unblock any Append waiting for space
}
