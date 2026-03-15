package client

import (
	"context"
	"io"
	"sync"
)

// RecvBuffer is a bounded in-memory byte buffer that decouples the DATA-frame
// reader (producer) from the local HTTP consumer (reader).
//
// The buffer is a simple byte slice ring.  When the buffer is full the
// producer blocks, which stops ACKs from advancing, which causes the server to
// pause sending.  This is the backpressure chain:
//
//	slow HTTP consumer → RecvBuffer full → producer blocks → no new ACKs
//	→ server's inflight ≥ window → server pauses DATA frames
//	→ accel-proxy TCP recv-window shrinks → origin slows
type RecvBuffer struct {
	mu       sync.Mutex
	notFull  *sync.Cond
	notEmpty *sync.Cond

	buf    []byte
	head   int // next read position
	tail   int // next write position
	size   int // bytes currently in buffer
	cap    int

	// closed is set by Close to signal that no more data will arrive.
	closed   bool
	closeErr error // io.EOF for clean end, otherwise origin/transport error
}

// NewRecvBuffer creates a buffer with the given capacity in bytes.
func NewRecvBuffer(capacity int) *RecvBuffer {
	b := &RecvBuffer{buf: make([]byte, capacity), cap: capacity}
	b.notFull  = sync.NewCond(&b.mu)
	b.notEmpty = sync.NewCond(&b.mu)
	return b
}

// Write appends data into the buffer.  Blocks if the buffer is full until
// space is available or ctx is cancelled.
func (b *RecvBuffer) Write(ctx context.Context, data []byte) error {
	written := 0
	for written < len(data) {
		b.mu.Lock()
		for b.size == b.cap && !b.closed {
			b.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			b.mu.Lock()
		}
		if b.closed {
			b.mu.Unlock()
			return b.closeErr
		}

		// Copy as many bytes as fit.
		avail := b.cap - b.size
		chunk := len(data) - written
		if chunk > avail {
			chunk = avail
		}
		for i := 0; i < chunk; i++ {
			b.buf[(b.tail+i)%b.cap] = data[written+i]
		}
		b.tail  = (b.tail + chunk) % b.cap
		b.size += chunk
		written += chunk
		b.notEmpty.Broadcast()
		b.mu.Unlock()
	}
	return nil
}

// Read reads up to len(p) bytes into p.  Implements io.Reader.
// Blocks until data is available or the buffer is closed.
func (b *RecvBuffer) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for b.size == 0 {
		if b.closed {
			return 0, b.closeErr
		}
		b.notEmpty.Wait()
	}

	n := len(p)
	if n > b.size {
		n = b.size
	}
	for i := 0; i < n; i++ {
		p[i] = b.buf[(b.head+i)%b.cap]
	}
	b.head  = (b.head + n) % b.cap
	b.size -= n
	b.notFull.Broadcast()
	return n, nil
}

// Close marks the buffer as closed.  Subsequent Read calls drain remaining
// bytes then return closeErr (io.EOF for a clean stream end).
func (b *RecvBuffer) Close(closeErr error) {
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
	b.notEmpty.Broadcast()
	b.notFull.Broadcast()
}

// Available returns the number of bytes currently readable without blocking.
func (b *RecvBuffer) Available() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}
