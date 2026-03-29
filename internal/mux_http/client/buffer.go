package client

import (
	"context"
	"io"
	"sync"
)

// RecvBuffer is a bounded in-memory byte buffer that decouples the DATA-frame
// reader (producer) from the local HTTP consumer (reader).
//
// Back-pressure chain:
//
//	slow HTTP consumer → RecvBuffer full → producer blocks → no new ACKs
//	→ server's inflight ≥ window → server pauses DATA frames
//	→ accel-proxy TCP recv-window shrinks → origin slows
//
// Lifecycle across reconnects:
//
//	Normal push:  Write → Write → ... → Close(nil) → Read drains → EOF
//	Network error: Write → Write → [conn dies] → Reopen() → Write → ...
type RecvBuffer struct {
	mu       sync.Mutex
	notFull  *sync.Cond
	notEmpty *sync.Cond

	buf  []byte
	head int
	tail int
	size int
	cap  int

	closed   bool
	closeErr error
}

// NewRecvBuffer creates a buffer with the given capacity in bytes.
func NewRecvBuffer(capacity int) *RecvBuffer {
	b := &RecvBuffer{buf: make([]byte, capacity), cap: capacity}
	b.notFull = sync.NewCond(&b.mu)
	b.notEmpty = sync.NewCond(&b.mu)
	return b
}

// Write appends data into the buffer. Blocks if the buffer is full until
// space is available or ctx is cancelled.
func (b *RecvBuffer) Write(ctx context.Context, data []byte) error {
	written := 0
	for written < len(data) {
		b.mu.Lock()

		if b.size == b.cap && !b.closed {
			quit := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					b.notFull.Broadcast()
				case <-quit:
				}
			}()

			for b.size == b.cap && !b.closed && ctx.Err() == nil {
				b.notFull.Wait()
			}
			close(quit)

			if ctx.Err() != nil {
				b.mu.Unlock()
				return ctx.Err()
			}
		}

		if b.closed {
			b.mu.Unlock()
			return b.closeErr
		}

		avail := b.cap - b.size
		chunk := len(data) - written
		if chunk > avail {
			chunk = avail
		}
		b.ringWrite(data[written:written+chunk])
		written += chunk
		b.notEmpty.Broadcast()
		b.mu.Unlock()
	}
	return nil
}

// Read reads up to len(p) bytes into p. Implements io.Reader.
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
	b.ringRead(p[:n])
	b.notFull.Broadcast()
	return n, nil
}

// Close marks the buffer as closed.
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

// Reopen clears the closed flag so that Write can resume after a transient
// network disconnect.
func (b *RecvBuffer) Reopen() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		return
	}
	b.closed = false
	b.closeErr = nil
	b.notFull.Broadcast()
}

// IsClosed reports whether the buffer has been closed.
func (b *RecvBuffer) IsClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// Available returns the number of bytes currently readable without blocking.
func (b *RecvBuffer) Available() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}

// ─────────────────────────────────────────────────────────────────────────────
// Ring buffer bulk operations — replaces per-byte loops with copy()
// ─────────────────────────────────────────────────────────────────────────────

// ringWrite copies src into the ring buffer starting at b.tail.
// Caller must hold b.mu and ensure len(src) <= b.cap - b.size.
func (b *RecvBuffer) ringWrite(src []byte) {
	n := len(src)
	first := b.cap - b.tail // bytes from tail to physical end
	if first >= n {
		// No wrap: single copy.
		copy(b.buf[b.tail:], src)
	} else {
		// Wrap: copy to end, then copy remainder to beginning.
		copy(b.buf[b.tail:], src[:first])
		copy(b.buf[0:], src[first:])
	}
	b.tail = (b.tail + n) % b.cap
	b.size += n
}

// ringRead copies n bytes from the ring buffer starting at b.head into dst.
// Caller must hold b.mu and ensure len(dst) <= b.size.
func (b *RecvBuffer) ringRead(dst []byte) {
	n := len(dst)
	first := b.cap - b.head // bytes from head to physical end
	if first >= n {
		copy(dst, b.buf[b.head:b.head+n])
	} else {
		copy(dst[:first], b.buf[b.head:])
		copy(dst[first:], b.buf[0:n-first])
	}
	b.head = (b.head + n) % b.cap
	b.size -= n
}
