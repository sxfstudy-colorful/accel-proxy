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
	"github.com/sxfstudy-colorful/accel-proxy/internal/netutil"
	"github.com/sxfstudy-colorful/accel-proxy/internal/tunnel"
)

// HTTPConnHandler processes raw client connections for L7 (HTTP/HTTPS) services.
type HTTPConnHandler struct {
	svc    config.ServiceConfig
	logger *slog.Logger
}

func NewHTTPConnHandler(svc config.ServiceConfig, logger *slog.Logger) *HTTPConnHandler {
	return &HTTPConnHandler{svc: svc, logger: logger}
}

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
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

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
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		h.logger.Warn("CONNECT: upstream rejected",
			"status", resp.StatusCode, "target", req.Host)
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
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	resp.Header.Set("X-Proxy", "accel-proxy")

	if err := resp.Write(clientConn); err != nil {
		h.logger.Debug("HTTP: write response to client", "err", err)
		return false
	}

	// FIX: use connectionHasClose for multi-value Connection header parsing.
	if connectionHasClose(req.Header) {
		return false
	}
	if connectionHasClose(resp.Header) {
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

	// FIX: use RewriteHostValue instead of Origin.Host:Port.
	// Origin is only configured on egress nodes; on access nodes it is zero-valued,
	// which would produce ":0". RewriteHostValue is explicitly set in the config.
	if svc.HTTP.RewriteHost && svc.HTTP.RewriteHostValue != "" {
		req.Host = svc.HTTP.RewriteHostValue
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

// hopByHopHeaders lists headers that must not be forwarded by proxies.
// Transfer-Encoding is intentionally excluded — Go's http.Request.Write
// handles chunked encoding automatically.
var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "TE", "Trailers",
}

// ─────────────────────────────────────────────────────────────────────────────
// Bidirectional raw relay (bufio-aware)
// ─────────────────────────────────────────────────────────────────────────────

func relayBuffered(
	a net.Conn, aBR *bufio.Reader,
	b *tunnel.Transport, bBR *bufio.Reader,
	logger *slog.Logger,
) {
	if err := flushBufio(bBR, a); err != nil {
		logger.Debug("relay flush b→a", "err", err)
		return
	}
	if err := flushBufio(aBR, b); err != nil {
		logger.Debug("relay flush a→b", "err", err)
		return
	}

	done := make(chan struct{}, 2)

	go func() {
		defer func() { _ = b.Close(); done <- struct{}{} }()
		buf := make([]byte, 32*1024)
		if _, err := io.CopyBuffer(b, a, buf); err != nil &&
			!isClosedConn(err) && !errors.Is(err, tunnel.ErrTransportDead) {
			logger.Debug("relay a→b", "err", err)
		}
	}()

	go func() {
		defer func() { _ = a.Close(); done <- struct{}{} }()
		buf := make([]byte, 32*1024)
		if _, err := io.CopyBuffer(a, b, buf); err != nil && !isClosedConn(err) {
			logger.Debug("relay b→a", "err", err)
		}
	}()

	<-done
	<-done
}

func flushBufio(br *bufio.Reader, dst io.Writer) error {
	if br == nil || br.Buffered() == 0 {
		return nil
	}
	drained := make([]byte, br.Buffered())
	if _, err := br.Read(drained); err != nil {
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

// connectionHasClose checks whether the Connection header contains the "close"
// token. The header value may be a comma-separated list of tokens, e.g.
// "close, X-Custom" or "keep-alive".
func connectionHasClose(header http.Header) bool {
	for _, v := range header["Connection"] {
		for _, token := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "close") {
				return true
			}
		}
	}
	return false
}

func remoteIP(conn net.Conn) string {
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn.RemoteAddr().String()
	}
	return host
}

func isClosedConn(err error) bool {
	return netutil.IsExpectedCloseErr(err)
}

func writeHTTPError(conn net.Conn, code int, msg string, logger *slog.Logger) {
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		logger.Debug("writeHTTPError: set deadline", "err", err)
		return
	}
	var buf bytes.Buffer
	_, _ = fmt.Fprintf(&buf,
		"HTTP/1.1 %d %s\r\nContent-Type: text/plain\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
		code, http.StatusText(code), len(msg), msg,
	)
	if _, err := conn.Write(buf.Bytes()); err != nil && !isClosedConn(err) {
		logger.Debug("writeHTTPError: write", "code", code, "err", err)
	}
}
