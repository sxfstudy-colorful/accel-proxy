// Package netutil provides shared network utility functions used across
// the proxy, tunnel, and mux_http packages.
package netutil

import (
	"errors"
	"io"
	"net"
	"strings"
)

// IsExpectedCloseErr reports whether err is a "normal" connection-close error
// that should be silently ignored in relay/copy loops.
//
// Covered cases:
//   - nil, io.EOF                          — clean close
//   - "use of closed network connection"   — local side closed first
//   - "connection reset by peer"           — remote side reset
//   - "broken pipe"                        — write to already-closed conn
func IsExpectedCloseErr(err error) bool {
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
