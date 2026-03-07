package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/accel-proxy/internal/config"
)

// L7Proxy is an HTTP/1.1 reverse proxy with header manipulation.
// It handles:
//   - Plain HTTP forwarding
//   - CONNECT tunnelling (for HTTPS pass-through)
//   - WebSocket upgrade pass-through
type L7Proxy struct {
	cfg     config.ServiceConfig
	origin  *url.URL
	rp      *httputil.ReverseProxy
	logger  *slog.Logger
	transport http.RoundTripper
}

// NewL7Proxy creates an L7Proxy configured for the given service.
func NewL7Proxy(svc config.ServiceConfig, logger *slog.Logger) (*L7Proxy, error) {
	scheme := "http"
	if svc.Origin.TLS {
		scheme = "https"
	}
	originURL := &url.URL{
		Scheme: scheme,
		Host:   fmt.Sprintf("%s:%d", svc.Origin.Host, svc.Origin.Port),
	}

	p := &L7Proxy{
		cfg:    svc,
		origin: originURL,
		logger: logger,
		transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   svc.Origin.DialTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          200,
			MaxIdleConnsPerHost:   50,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     false, // keep HTTP/1.1 for simplicity
		},
	}

	director := func(req *http.Request) {
		req.URL.Scheme = originURL.Scheme
		req.URL.Host = originURL.Host

		if svc.HTTP.RewriteHost {
			req.Host = originURL.Host
		}

		// Propagate client IP.
		if prior := req.Header.Get("X-Forwarded-For"); prior != "" {
			req.Header.Set("X-Forwarded-For", prior+", "+clientIP(req))
		} else {
			req.Header.Set("X-Forwarded-For", clientIP(req))
		}
		req.Header.Set("X-Forwarded-Proto", "http")
		req.Header.Set("X-Real-IP", clientIP(req))

		// Apply config-driven header manipulation.
		for _, h := range svc.HTTP.RemoveRequestHeaders {
			req.Header.Del(h)
		}
		for k, v := range svc.HTTP.AddRequestHeaders {
			req.Header.Set(k, v)
		}

		// Ensure Via header.
		req.Header.Set("Via", "1.1 accel-proxy")
	}

	modifyResponse := func(resp *http.Response) error {
		resp.Header.Set("X-Proxy", "accel-proxy")
		return nil
	}

	p.rp = &httputil.ReverseProxy{
		Director:       director,
		Transport:      p.transport,
		ModifyResponse: modifyResponse,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			logger.Error("L7 proxy error", "url", r.URL, "err", err)
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		},
		BufferPool: newBufPool(),
	}

	return p, nil
}

// ServeHTTP implements http.Handler.  It dispatches based on method:
//   - CONNECT → HTTPS tunnel
//   - Upgrade: websocket → WS pass-through
//   - everything else → standard HTTP reverse proxy
func (p *L7Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.handleConnect(w, r)
		return
	}

	if isWebSocketUpgrade(r) {
		p.handleWebSocketUpgrade(w, r)
		return
	}

	p.rp.ServeHTTP(w, r)
}

// handleConnect handles HTTP CONNECT tunnelling (HTTPS through a plain-HTTP proxy).
func (p *L7Proxy) handleConnect(w http.ResponseWriter, r *http.Request) {
	dest := r.Host
	if dest == "" {
		http.Error(w, "missing host", http.StatusBadRequest)
		return
	}

	origin, err := net.DialTimeout("tcp",
		fmt.Sprintf("%s:%d", p.cfg.Origin.Host, p.cfg.Origin.Port),
		p.cfg.Origin.DialTimeout,
	)
	if err != nil {
		p.logger.Error("CONNECT dial origin", "dest", dest, "err", err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack not supported", http.StatusInternalServerError)
		origin.Close()
		return
	}
	client, buf, err := hj.Hijack()
	if err != nil {
		http.Error(w, "hijack failed", http.StatusInternalServerError)
		origin.Close()
		return
	}

	// Flush any buffered data and acknowledge the tunnel.
	buf.Flush()
	client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")) //nolint:errcheck

	relay(client, origin, p.logger)
}

// handleWebSocketUpgrade passes an HTTP Upgrade request to the origin verbatim.
func (p *L7Proxy) handleWebSocketUpgrade(w http.ResponseWriter, r *http.Request) {
	backendAddr := fmt.Sprintf("%s:%d", p.cfg.Origin.Host, p.cfg.Origin.Port)
	backendConn, err := net.DialTimeout("tcp", backendAddr, p.cfg.Origin.DialTimeout)
	if err != nil {
		p.logger.Error("WS upgrade dial origin", "addr", backendAddr, "err", err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack not supported", http.StatusInternalServerError)
		backendConn.Close()
		return
	}
	clientConn, buf, err := hj.Hijack()
	if err != nil {
		http.Error(w, "hijack failed", http.StatusInternalServerError)
		backendConn.Close()
		return
	}
	buf.Flush()

	// Forward the original upgrade request to the backend.
	r.Header.Set("X-Forwarded-For", clientIP(r))
	r.Write(backendConn) //nolint:errcheck

	// Relay raw bytes (WebSocket frames are binary after the HTTP upgrade).
	relay(clientConn, backendConn, p.logger)
}

// ListenAndServe starts the L7 HTTP proxy on the given address.
func (p *L7Proxy) ListenAndServe(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:         addr,
		Handler:      p,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx) //nolint:errcheck
	}()

	p.logger.Info("L7 proxy listening", "addr", addr, "origin", p.origin)
	return srv.ListenAndServe()
}

// ──────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// bufPool implements httputil.BufferPool to reduce allocations.
type bufPool struct{}

func newBufPool() httputil.BufferPool { return &bufPool{} }
func (b *bufPool) Get() []byte        { return make([]byte, 32*1024) }
func (b *bufPool) Put([]byte)          {}

// ParseHTTPRequest reads an HTTP request from a raw connection
// (used when the access node receives raw bytes and needs to inspect L7).
func ParseHTTPRequest(conn net.Conn) (*http.Request, *bufio.Reader, error) {
	br := bufio.NewReader(conn)
	req, err := http.ReadRequest(br)
	if err != nil {
		return nil, nil, fmt.Errorf("parse HTTP request: %w", err)
	}
	return req, br, nil
}

// WriteHTTPResponse sends an HTTP response on a raw connection.
func WriteHTTPResponse(conn net.Conn, status int, body string) error {
	resp := &http.Response{
		Status:     http.StatusText(status),
		StatusCode: status,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	resp.Header.Set("Content-Type", "text/plain")
	return resp.Write(conn)
}
