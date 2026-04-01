package muxproto

import (
	"fmt"
	"io"
	"sync"
)

// StreamPipe is a bounded in-memory ring-buffer pipe for one direction of a
// bidirectional stream.
//
// With flow control, OnData only calls WriteNonBlock which never blocks.
// Without flow control (push streams), the blocking Write is still available.
type StreamPipe struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond

	buf  []byte // lazily allocated on first write
	head int
	tail int
	size int
	cap  int

	closed   bool
	closeErr error
}

func NewStreamPipe(capacity int) *StreamPipe {
	if capacity <= 0 {
		capacity = 256 * 1024
	}
	p := &StreamPipe{cap: capacity} // buf is nil — allocated lazily
	p.notEmpty = sync.NewCond(&p.mu)
	p.notFull = sync.NewCond(&p.mu)
	return p
}

// ensureBuf lazily allocates the ring buffer on first use.
// Must be called with p.mu held.
func (p *StreamPipe) ensureBuf() {
	if p.buf == nil {
		p.buf = make([]byte, p.cap)
	}
}

// Write appends data. Blocks if full. Returns error if pipe is closed.
func (p *StreamPipe) Write(data []byte) (int, error) {
	written := 0
	for written < len(data) {
		p.mu.Lock()
		for p.size == p.cap && !p.closed {
			p.notFull.Wait()
		}
		if p.closed {
			p.mu.Unlock()
			if p.closeErr != nil {
				return written, p.closeErr
			}
			return written, io.ErrClosedPipe
		}
		p.ensureBuf()

		avail := p.cap - p.size
		chunk := len(data) - written
		if chunk > avail {
			chunk = avail
		}
		p.ringWrite(data[written : written+chunk])
		written += chunk
		p.notEmpty.Broadcast()
		p.mu.Unlock()
	}
	return written, nil
}

// WriteNonBlock writes data without blocking.
// Returns ErrPipeFull if there is not enough space.
// With flow control the pipe should always have room — a full pipe indicates
// the remote sender violated the window protocol.
func (p *StreamPipe) WriteNonBlock(data []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, io.ErrClosedPipe
	}
	if len(data) > p.cap-p.size {
		return 0, ErrPipeFull
	}
	p.ensureBuf()
	p.ringWrite(data)
	p.notEmpty.Broadcast()
	return len(data), nil
}

// Read reads up to len(b) bytes. Blocks until data available or closed.
func (p *StreamPipe) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.size == 0 {
		if p.closed {
			if p.closeErr != nil {
				return 0, p.closeErr
			}
			return 0, io.EOF
		}
		p.notEmpty.Wait()
	}

	n := len(b)
	if n > p.size {
		n = p.size
	}
	p.ringRead(b[:n])
	p.notFull.Broadcast()
	return n, nil
}

// CloseWrite signals no more data will be written.
func (p *StreamPipe) CloseWrite(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	p.closeErr = err
	p.notEmpty.Broadcast()
	p.notFull.Broadcast()
}

func (p *StreamPipe) Available() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.size
}

// ─── Ring buffer ops (caller must hold mu) ───────────────────────────────────

func (p *StreamPipe) ringWrite(src []byte) {
	n := len(src)
	first := p.cap - p.tail
	if first >= n {
		copy(p.buf[p.tail:], src)
	} else {
		copy(p.buf[p.tail:], src[:first])
		copy(p.buf[0:], src[first:])
	}
	p.tail = (p.tail + n) % p.cap
	p.size += n
}

func (p *StreamPipe) ringRead(dst []byte) {
	n := len(dst)
	first := p.cap - p.head
	if first >= n {
		copy(dst, p.buf[p.head:p.head+n])
	} else {
		copy(dst[:first], p.buf[p.head:])
		copy(dst[first:], p.buf[0:n-first])
	}
	p.head = (p.head + n) % p.cap
	p.size -= n
}

var ErrPipeFull = fmt.Errorf("stream pipe full: flow control violated")
