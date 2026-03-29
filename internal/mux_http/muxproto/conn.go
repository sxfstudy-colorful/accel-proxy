package muxproto

import (
	"bufio"
	"net"
	"sync"
)

// MuxConn wraps a net.Conn and provides frame-level read/write with:
//   - buffered reading  (reduces syscall count on the read path)
//   - buffered writing  (merges header+payload into one syscall per frame)
//   - serialised writing (multiple goroutines can safely call WriteFrame)
type MuxConn struct {
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
	wmu  sync.Mutex
}

func NewMuxConn(conn net.Conn) *MuxConn {
	return &MuxConn{
		conn: conn,
		br:   bufio.NewReaderSize(conn, 256*1024),
		bw:   bufio.NewWriterSize(conn, 64*1024),
	}
}

// ReadFrame reads the next frame. Blocking; returns error on any I/O failure.
func (c *MuxConn) ReadFrame() (*Frame, error) {
	return ReadFrame(c.br)
}

// WriteFrame serialises and writes one frame. Thread-safe.
// The header and payload are buffered and flushed together, reducing the
// per-frame syscall count from 2 to 1.
func (c *MuxConn) WriteFrame(f *Frame) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := f.WriteTo(c.bw); err != nil {
		return err
	}
	return c.bw.Flush()
}

func (c *MuxConn) Close() error         { return c.conn.Close() }
func (c *MuxConn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }
