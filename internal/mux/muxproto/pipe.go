package muxproto

import (
	"fmt"
	"io"
	"sync"
)

// StreamPipe is a bounded in-memory pipe for one direction of a bidirectional
// stream. It decouples the frame-reading goroutine (writer) from the consumer
// goroutine (reader).
//
// Usage:
//
//	pipe := NewStreamPipe(bufSize)
//	// Writer side (frame dispatcher goroutine):
//	pipe.Write(framePayload)
//	pipe.CloseWrite(nil)  // signal EOF
//	// Reader side (consumer goroutine):
//	io.ReadAll(pipe)
type StreamPipe struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond

	buf  []byte
	head int
	tail int
	size int
	cap  int

	closed   bool
	closeErr error // nil → io.EOF to reader
}

func NewStreamPipe(capacity int) *StreamPipe {
	if capacity <= 0 {
		capacity = 256 * 1024 // 256 KiB default
	}
	p := &StreamPipe{buf: make([]byte, capacity), cap: capacity}
	p.notEmpty = sync.NewCond(&p.mu)
	p.notFull = sync.NewCond(&p.mu)
	return p
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

		avail := p.cap - p.size
		chunk := len(data) - written
		if chunk > avail {
			chunk = avail
		}
		first := p.cap - p.tail
		if first >= chunk {
			copy(p.buf[p.tail:], data[written:written+chunk])
		} else {
			copy(p.buf[p.tail:], data[written:written+first])
			copy(p.buf[0:], data[written+first:written+chunk])
		}
		p.tail = (p.tail + chunk) % p.cap
		p.size += chunk
		written += chunk
		p.notEmpty.Broadcast()
		p.mu.Unlock()
	}
	return written, nil
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
	first := p.cap - p.head
	if first >= n {
		copy(b, p.buf[p.head:p.head+n])
	} else {
		copy(b[:first], p.buf[p.head:])
		copy(b[first:], p.buf[0:n-first])
	}
	p.head = (p.head + n) % p.cap
	p.size -= n
	p.notFull.Broadcast()
	return n, nil
}

// CloseWrite signals that no more data will be written.
// Pass nil for clean EOF; pass an error for abnormal termination.
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

// Available returns bytes currently buffered.
func (p *StreamPipe) Available() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.size
}

// WriteNonBlock writes data into the pipe without blocking.
// Returns ErrPipeFull if there is not enough space.
// Used by OnData: with flow control the pipe should always have room,
// so a full pipe indicates a protocol violation by the remote sender.
var ErrPipeFull = fmt.Errorf("stream pipe full: flow control violated")

func (p *StreamPipe) WriteNonBlock(data []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, io.ErrClosedPipe
	}
	avail := p.cap - p.size
	if len(data) > avail {
		return 0, ErrPipeFull
	}

	n := len(data)
	first := p.cap - p.tail
	if first >= n {
		copy(p.buf[p.tail:], data)
	} else {
		copy(p.buf[p.tail:], data[:first])
		copy(p.buf[0:], data[first:])
	}
	p.tail = (p.tail + n) % p.cap
	p.size += n
	p.notEmpty.Broadcast()
	return n, nil
}
