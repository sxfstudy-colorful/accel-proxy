package server

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux_http/muxproto"
)

const segmentSize = 4 * 1024 * 1024 // 4 MiB per segment

type segment struct {
	baseOffset int64
	data       []byte
}

// StreamBuffer is a concurrent append-only byte store for one push stream.
type StreamBuffer struct {
	mu   sync.Mutex
	cond *sync.Cond

	segments    []*segment
	baseOffset  int64
	writeOffset int64

	maxRetain int64

	closed   bool
	closeErr error
}

func NewStreamBuffer(maxRetain int64) *StreamBuffer {
	b := &StreamBuffer{maxRetain: maxRetain}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Append adds data to the stream. Blocks when the buffer is full.
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

// Close marks the stream finished. Pass nil for a clean EOF.
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

// IsClosed reports whether the stream has been closed.
func (b *StreamBuffer) IsClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// WriteOffset returns total bytes written so far.
func (b *StreamBuffer) WriteOffset() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.writeOffset
}

// Read reads up to len(buf) bytes starting at the given absolute offset.
// Uses binary search to locate the starting segment (O(log N) instead of O(N)).
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

	// Binary search: find the first segment whose data range covers 'offset'.
	// Each segment spans [baseOffset, baseOffset+len(data)).
	// We want the first segment where baseOffset + len(data) > offset.
	startIdx := sort.Search(len(b.segments), func(i int) bool {
		return b.segments[i].baseOffset+int64(len(b.segments[i].data)) > offset
	})

	n := 0
	for i := startIdx; i < len(b.segments) && n < len(buf); i++ {
		seg := b.segments[i]
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
	}
	return n, nil
}

// WaitForData blocks until data is available past offset, stream is closed,
// or ctx is cancelled.
func (b *StreamBuffer) WaitForData(ctx context.Context, offset int64) {
	b.mu.Lock()
	if b.closed || b.writeOffset > offset || ctx.Err() != nil {
		b.mu.Unlock()
		return
	}

	quit := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			b.cond.Broadcast()
		case <-quit:
		}
	}()

	for !b.closed && b.writeOffset <= offset && ctx.Err() == nil {
		b.cond.Wait()
	}
	b.mu.Unlock()
	close(quit)
}

// Trim frees all segments whose data ends at or before minOffset.
func (b *StreamBuffer) Trim(minOffset int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Binary search: find the first segment NOT fully consumed.
	trimCount := sort.Search(len(b.segments), func(i int) bool {
		return b.segments[i].baseOffset+int64(len(b.segments[i].data)) > minOffset
	})

	if trimCount == 0 {
		return
	}
	b.segments = b.segments[trimCount:]
	if len(b.segments) > 0 {
		b.baseOffset = b.segments[0].baseOffset
	} else {
		b.baseOffset = b.writeOffset
	}
	b.cond.Broadcast()
}
