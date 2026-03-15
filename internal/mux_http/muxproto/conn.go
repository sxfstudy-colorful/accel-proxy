package muxproto

import (
	"bufio"
	"net"
	"sync"
)

// MuxConn wraps a net.Conn and provides frame-level read/write with:
//   - buffered reading (reduces syscall count on the hot path)
//   - serialised writing (multiple goroutines can safely call WriteFrame)
type MuxConn struct {
	conn net.Conn
	br   *bufio.Reader
	wmu  sync.Mutex
}

func NewMuxConn(conn net.Conn) *MuxConn {
	return &MuxConn{
		conn: conn,
		br:   bufio.NewReaderSize(conn, 256*1024),
	}
}

// ReadFrame reads the next frame. Blocking; returns error on any I/O failure.
func (c *MuxConn) ReadFrame() (*Frame, error) {
	return ReadFrame(c.br)
}

// WriteFrame serialises and writes one frame. Thread-safe.
func (c *MuxConn) WriteFrame(f *Frame) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return f.WriteTo(c.conn)
}

func (c *MuxConn) Close() error              { return c.conn.Close() }
func (c *MuxConn) RemoteAddr() net.Addr      { return c.conn.RemoteAddr() }
