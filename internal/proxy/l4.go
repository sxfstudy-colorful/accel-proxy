// Package proxy implements Layer-4 and Layer-7 proxy logic.
package proxy

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"
)

// L4Proxy is a transparent TCP proxy.
// It accepts a client net.Conn and forwards raw bytes to/from the target.
type L4Proxy struct {
	DialTimeout time.Duration
	logger      *slog.Logger
}

// NewL4Proxy creates an L4Proxy.
func NewL4Proxy(dialTimeout time.Duration, logger *slog.Logger) *L4Proxy {
	if dialTimeout == 0 {
		dialTimeout = 10 * time.Second
	}
	return &L4Proxy{DialTimeout: dialTimeout, logger: logger}
}

// Handle connects to target and relays bytes bidirectionally until either
// side closes the connection.
func (p *L4Proxy) Handle(client net.Conn, targetHost string, targetPort int) {
	defer client.Close()

	addr := fmt.Sprintf("%s:%d", targetHost, targetPort)
	origin, err := net.DialTimeout("tcp", addr, p.DialTimeout)
	if err != nil {
		p.logger.Error("L4 dial origin failed", "addr", addr, "err", err)
		return
	}
	defer origin.Close()

	p.logger.Debug("L4 relay started",
		"client", client.RemoteAddr(),
		"origin", addr,
	)

	relay(client, origin, p.logger)
}

// relay copies data between two connections concurrently.
func relay(a, b net.Conn, logger *slog.Logger) {
	var wg sync.WaitGroup
	wg.Add(2)

	copyHalf := func(dst, src net.Conn, label string) {
		defer wg.Done()
		defer dst.Close()
		buf := make([]byte, 32*1024)
		n, err := io.CopyBuffer(dst, src, buf)
		if err != nil && !isClosedErr(err) {
			logger.Debug("relay copy", "dir", label, "bytes", n, "err", err)
		}
	}

	go copyHalf(a, b, "origin→client")
	go copyHalf(b, a, "client→origin")

	wg.Wait()
}

func isClosedErr(err error) bool {
	if err == nil || errors.Is(err, io.EOF) {
		return true
	}
	var ne *net.OpError
	if errors.As(err, &ne) {
		s := ne.Err.Error()
		return strings.Contains(s, "use of closed network connection") ||
			strings.Contains(s, "connection reset by peer") ||
			strings.Contains(s, "broken pipe")
	}
	return false
}
