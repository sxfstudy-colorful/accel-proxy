// Package proxy implements Layer-4 and Layer-7 proxy logic.
package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
	"github.com/sxfstudy-colorful/accel-proxy/internal/tunnel"
)

// HTTPConnHandler processes raw client connections for L7 (HTTP/HTTPS) services.
type HTTPConnHandler struct {
	svc    config.ServiceConfig
	logger *slog.Logger
}

// NewHTTPConnHandler creates an HTTPConnHandler for the given service.
func NewHTTPConnHandler(svc config.ServiceConfig, logger *slog.Logger) *HTTPConnHandler {
	return &HTTPConnHandler{svc: svc, logger: logger}
}

// Handle processes a single client connection through the given tunnel.
// tunnelConn must be a *tunnel.Transport — the dead-flag contract depends on it.
func (h *HTTPConnHandler) Handle(clientConn net.Conn, tunnelConn *tunnel.Transport) {
	clientIPStr := remoteIP(clientConn)
	clientBR := bufio.NewReaderSize(clientConn, 64*1024)
	tunnelBR := bufio.NewReaderSize(tunnelConn, 64*1024)

	for {
		req, err := http.ReadRequest(clientBR)
		if err != nil {
			if err != io.EOF && !isClosedConn(err) {
				h.logger.Debug("L7 read request", "client", clientIPStr, "err", err)
			}
			return
		}

		h.logger.Info("L7 request",
			"method", req.Method,
			"host", req.Host,
			"url", req.URL.String(),
			"client", clientIPStr,
			"upgrade", req.Header.Get("Upgrade"),
		)

		h.mutateRequest(req, clientIPStr)

		switch {
		case isWebSocketUpgrade(req):
			h.handleWebSocket(req, clientConn, clientBR, tunnelConn, tunnelBR, clientIPStr)
			return
		case req.Method == http.MethodConnect:
			h.handleConnect(req, clientConn, tunnelConn, tunnelBR, clientIPStr)
			return
		default:
			if !h.handleHTTP(req, clientConn, tunnelConn, tunnelBR, clientIPStr) {
				return
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket upgrade
// ─────────────────────────────────────────────────────────────────────────────

func (h *HTTPConnHandler) handleWebSocket(
	req *http.Request,
	clientConn net.Conn,
	clientBR *bufio.Reader,
	tunnelConn *tunnel.Transport,
	tunnelBR *bufio.Reader,
	clientIPStr string,
) {
	if err := req.Write(tunnelConn); err != nil {
		h.logger.Error("WS: write upgrade to tunnel", "err", err)
		writeHTTPError(clientConn, http.StatusBadGateway, "tunnel write error", h.logger)
		return
	}

	resp, err := http.ReadResponse(tunnelBR, req)
	if err != nil {
		h.logger.Error("WS: read upgrade response", "err", err)
		writeHTTPError(clientConn, http.StatusBadGateway, "bad gateway", h.logger)
		return
	}

	if resp.StatusCode != http.StatusSwitchingProtocols {
		h.logger.Warn("WS: origin rejected upgrade",
			"status", resp.StatusCode, "service", h.svc.ID)
		if err := resp.Write(clientConn); err != nil {
			h.logger.Debug("WS: write rejection to client", "err", err)
		}
		return
	}

	if err := resp.Write(clientConn); err != nil {
		h.logger.Error("WS: write 101 to client", "err", err)
		return
	}

	h.logger.Info("WS: upgrade complete, entering full-duplex relay",
		"client", clientIPStr, "service", h.svc.ID)

	relayBuffered(clientConn, clientBR, tunnelConn, tunnelBR, h.logger)
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP CONNECT
// ─────────────────────────────────────────────────────────────────────────────

func (h *HTTPConnHandler) handleConnect(
	req *http.Request,
	clientConn net.Conn,
	tunnelConn *tunnel.Transport,
	tunnelBR *bufio.Reader,
	clientIPStr string,
) {
	if err := req.Write(tunnelConn); err != nil {
		h.logger.Error("CONNECT: write to tunnel", "err", err)
		writeHTTPError(clientConn, http.StatusBadGateway, "tunnel write error", h.logger)
		return
	}

	resp, err := http.ReadResponse(tunnelBR, req)
	if err != nil {
		h.logger.Error("CONNECT: read response", "err", err)
		writeHTTPError(clientConn, http.StatusBadGateway, "bad gateway", h.logger)
		return
	}

	if resp.StatusCode != http.StatusOK {
		if err := resp.Write(clientConn); err != nil {
			h.logger.Debug("CONNECT: write rejection to client", "err", err)
		}
		return
	}

	if _, err := clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		h.logger.Error("CONNECT: write 200 to client", "err", err)
		return
	}

	h.logger.Info("CONNECT: tunnel established",
		"target", req.Host, "client", clientIPStr)

	relayBuffered(clientConn, nil, tunnelConn, tunnelBR, h.logger)
}

// ─────────────────────────────────────────────────────────────────────────────
// Plain HTTP (keep-alive aware)
// ─────────────────────────────────────────────────────────────────────────────

func (h *HTTPConnHandler) handleHTTP(
	req *http.Request,
	clientConn net.Conn,
	tunnelConn *tunnel.Transport,
	tunnelBR *bufio.Reader,
	clientIPStr string,
) (keepAlive bool) {
	if err := req.Write(tunnelConn); err != nil {
		h.logger.Error("HTTP: write request to tunnel", "err", err)
		return false
	}

	resp, err := http.ReadResponse(tunnelBR, req)
	if err != nil {
		h.logger.Error("HTTP: read response from tunnel", "err", err)
		writeHTTPError(clientConn, http.StatusBadGateway, "bad gateway", h.logger)
		return false
	}
	defer resp.Body.Close()

	resp.Header.Set("X-Proxy", "accel-proxy")

	if err := resp.Write(clientConn); err != nil {
		h.logger.Debug("HTTP: write response to client", "err", err)
		return false
	}

	if strings.EqualFold(req.Header.Get("Connection"), "close") {
		return false
	}
	if strings.EqualFold(resp.Header.Get("Connection"), "close") {
		return false
	}
	return req.ProtoAtLeast(1, 1)
}

// ─────────────────────────────────────────────────────────────────────────────
// Header mutation
// ─────────────────────────────────────────────────────────────────────────────

func (h *HTTPConnHandler) mutateRequest(req *http.Request, clientIPStr string) {
	svc := h.svc

	if prior := req.Header.Get("X-Forwarded-For"); prior != "" {
		req.Header.Set("X-Forwarded-For", prior+", "+clientIPStr)
	} else {
		req.Header.Set("X-Forwarded-For", clientIPStr)
	}
	req.Header.Set("X-Real-IP", clientIPStr)
	req.Header.Set("Via", "1.1 accel-proxy")

	if svc.Origin.TLS {
		req.Header.Set("X-Forwarded-Proto", "https")
	} else {
		req.Header.Set("X-Forwarded-Proto", "http")
	}

	for _, del := range svc.HTTP.RemoveRequestHeaders {
		req.Header.Del(del)
	}
	for k, v := range svc.HTTP.AddRequestHeaders {
		req.Header.Set(k, v)
	}

	if svc.HTTP.RewriteHost {
		req.Host = fmt.Sprintf("%s:%d", svc.Origin.Host, svc.Origin.Port)
	}

	if req.URL.Host == "" {
		req.URL.Host = req.Host
	}
	if req.URL.Scheme == "" {
		if svc.Origin.TLS {
			req.URL.Scheme = "https"
		} else {
			req.URL.Scheme = "http"
		}
	}

	if !isWebSocketUpgrade(req) {
		for _, hdr := range hopByHopHeaders {
			req.Header.Del(hdr)
		}
	}
}

var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "TE", "Trailers", "Transfer-Encoding",
}

// ─────────────────────────────────────────────────────────────────────────────
// Bidirectional raw relay (bufio-aware)
// ─────────────────────────────────────────────────────────────────────────────

// relayBuffered copies bytes between a net.Conn and a tunnel Transport
// without duplication.
//
// b must be a *tunnel.Transport so the dead-flag contract is enforced:
// if the a→b direction fails, b is poisoned (dead=true) before Close,
// which causes b→a's io.CopyBuffer(a, b) to see EOF from the pipe and
// exit cleanly rather than racing on a half-closed Transport.
//
// Safety ordering:
//  1. Flush phase  (serial)     — drain bufio residuals into the peer.
//  2. Relay phase  (concurrent) — one goroutine per direction, no shared
//                                 read state, Transport.dead enforces
//                                 no-retry on write failure.
func relayBuffered(
	a net.Conn, aBR *bufio.Reader,
	b *tunnel.Transport, bBR *bufio.Reader,
	logger *slog.Logger,
) {
	// ── Phase 1: flush bufio residuals (serial) ───────────────────────────
	if err := flushBufio(bBR, a); err != nil {
		logger.Debug("relay flush b→a", "err", err)
		return
	}
	if err := flushBufio(aBR, b); err != nil {
		logger.Debug("relay flush a→b", "err", err)
		return
	}

	// ── Phase 2: raw bidirectional relay (concurrent) ─────────────────────
	done := make(chan struct{}, 2)

	// a → b: when this fails, b.Close() poisons b.dead=true, which makes
	// the b→a goroutine's Read(b) return EOF via the closed io.Pipe.
	go func() {
		defer func() { b.Close(); done <- struct{}{} }()
		buf := make([]byte, 32*1024)
		if _, err := io.CopyBuffer(b, a, buf); err != nil &&
			!isClosedConn(err) && !errors.Is(err, tunnel.ErrTransportDead) {
			logger.Debug("relay a→b", "err", err)
		}
	}()

	// b → a: Read(b) drains the io.Pipe; when the pipe is closed by
	// b.Close() above, Read returns EOF and this goroutine exits.
	go func() {
		defer func() { a.Close(); done <- struct{}{} }()
		buf := make([]byte, 32*1024)
		if _, err := io.CopyBuffer(a, b, buf); err != nil && !isClosedConn(err) {
			logger.Debug("relay b→a", "err", err)
		}
	}()

	<-done
	<-done
}

// flushBufio drains any bytes buffered in br and writes them to dst.
// br may be nil (no-op). Returns an error only if the write to dst fails —
// a partial flush that leaves dst in an inconsistent state is always fatal.
func flushBufio(br *bufio.Reader, dst io.Writer) error {
	if br == nil || br.Buffered() == 0 {
		return nil
	}
	drained := make([]byte, br.Buffered())
	// br.Read into a same-size buffer is guaranteed to succeed and return
	// exactly br.Buffered() bytes — it never blocks or returns io.EOF here.
	if _, err := br.Read(drained); err != nil {
		// Should be unreachable: bufio.Reader.Read on buffered data never errors.
		return fmt.Errorf("bufio drain read: %w", err)
	}
	if _, err := dst.Write(drained); err != nil {
		return fmt.Errorf("bufio drain write: %w", err)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func isWebSocketUpgrade(req *http.Request) bool {
	return strings.EqualFold(req.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(req.Header.Get("Connection")), "upgrade")
}

func remoteIP(conn net.Conn) string {
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn.RemoteAddr().String()
	}
	return host
}

func isClosedConn(err error) bool {
	if err == nil || errors.Is(err, io.EOF) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "use of closed network connection") ||
		strings.Contains(s, "connection reset by peer") ||
		strings.Contains(s, "broken pipe")
}

// writeHTTPError sends a minimal HTTP error response and logs write failures.
func writeHTTPError(conn net.Conn, code int, msg string, logger *slog.Logger) {
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		logger.Debug("writeHTTPError: set deadline", "err", err)
		return
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf,
		"HTTP/1.1 %d %s\r\nContent-Type: text/plain\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
		code, http.StatusText(code), len(msg), msg,
	)
	if _, err := conn.Write(buf.Bytes()); err != nil && !isClosedConn(err) {
		logger.Debug("writeHTTPError: write", "code", code, "err", err)
	}
}

// clientIP kept for l4.go compat.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
