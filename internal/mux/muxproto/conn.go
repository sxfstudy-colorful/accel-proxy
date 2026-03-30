package muxproto

import (
	"bufio"
	"net"
	"sync"
)

// MuxConn provides frame-level read/write with buffered I/O and serialised writes.
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

func (c *MuxConn) ReadFrame() (*Frame, error) {
	return ReadFrame(c.br)
}

// WriteFrame serialises one frame. Thread-safe.
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
