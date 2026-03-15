// proxy-test-server is a standalone origin server for accel-proxy integration testing.
//
// It runs two servers simultaneously:
//
//	TCP echo server  — echoes every byte back to the sender
//	HTTP server      — several endpoints for functional and benchmark testing
//
// Usage:
//
//	proxy-test-server [flags]
//
//	-tcp-addr  string   TCP echo listen address  (default "0.0.0.0:19000")
//	-http-addr string   HTTP listen address       (default "0.0.0.0:19001")
//
// HTTP endpoints:
//
//	GET  /hello        simple health-check, returns "Hello from origin\n"
//	GET  /echo         returns "METHOD /path\n"
//	GET  /headers      returns all request headers; useful for verifying proxy injection
//	POST /body         echoes the request body back verbatim
//	GET  /large?n=N    streams N bytes of 'A' (default N=1048576)
//	GET  /slow?ms=N    sleeps N ms before responding (default N=500)
//	GET  /stats        returns request counters as JSON
//	WS   /ws           WebSocket echo: every message frame is echoed back unchanged
//	WS   /ws-latency   WebSocket echo with server-side timestamp in each reply
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// ─────────────────────────────────────────────────────────────────────────────
// Counters
// ─────────────────────────────────────────────────────────────────────────────

type stats struct {
	TCPConns    atomic.Int64
	HTTPReqs    atomic.Int64
	WSConns     atomic.Int64
	WSMessages  atomic.Int64
	BytesEchoed atomic.Int64
}

var globalStats stats

// wsUpgrader is shared across all WebSocket handlers.
// CheckOrigin always returns true — this is a test server, not a browser app.
var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  64 * 1024,
	WriteBufferSize: 64 * 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	tcpAddr := flag.String("tcp-addr", "0.0.0.0:19000", "TCP echo listen address")
	httpAddr := flag.String("http-addr", "0.0.0.0:19001", "HTTP listen address")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// ── TCP echo server ───────────────────────────────────────────────────
	tcpLn, err := net.Listen("tcp", *tcpAddr)
	if err != nil {
		logger.Error("TCP listen failed", "addr", *tcpAddr, "err", err)
		os.Exit(1)
	}
	logger.Info("TCP echo server listening", "addr", tcpLn.Addr())
	go serveTCP(tcpLn, logger)

	// ── HTTP + WebSocket server ───────────────────────────────────────────
	mux := buildMux(logger)
	httpServer := &http.Server{Addr: *httpAddr, Handler: mux}

	httpLn, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		logger.Error("HTTP listen failed", "addr", *httpAddr, "err", err)
		os.Exit(1)
	}
	logger.Info("HTTP/WS server listening", "addr", httpLn.Addr())
	go func() {
		if err := httpServer.Serve(httpLn); err != http.ErrServerClosed {
			logger.Error("HTTP server error", "err", err)
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	logger.Info("signal received, shutting down", "signal", sig)

	tcpLn.Close()
	httpServer.Close()
	logger.Info("shutdown complete",
		"tcp_conns", globalStats.TCPConns.Load(),
		"http_reqs", globalStats.HTTPReqs.Load(),
		"ws_conns", globalStats.WSConns.Load(),
		"ws_messages", globalStats.WSMessages.Load(),
		"bytes_echoed", globalStats.BytesEchoed.Load(),
	)
}

// ─────────────────────────────────────────────────────────────────────────────
// TCP echo
// ─────────────────────────────────────────────────────────────────────────────

func serveTCP(ln net.Listener, logger *slog.Logger) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		globalStats.TCPConns.Add(1)
		logger.Info("TCP connection accepted", "remote", conn.RemoteAddr())
		go handleTCPConn(conn, logger)
	}
}

func handleTCPConn(conn net.Conn, logger *slog.Logger) {
	defer conn.Close()
	n, err := io.Copy(conn, conn)
	globalStats.BytesEchoed.Add(n)
	if err != nil && !isClosedErr(err) {
		logger.Debug("TCP echo error", "remote", conn.RemoteAddr(), "err", err)
	}
	logger.Info("TCP connection closed", "remote", conn.RemoteAddr(), "bytes_echoed", n)
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP + WebSocket handlers
// ─────────────────────────────────────────────────────────────────────────────

func buildMux(logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		globalStats.HTTPReqs.Add(1)
		logger.Info("HTTP /hello", "remote", r.RemoteAddr, "method", r.Method)
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "Hello from origin")
	})

	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		globalStats.HTTPReqs.Add(1)
		logger.Info("HTTP /echo", "remote", r.RemoteAddr)
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "%s %s\n", r.Method, r.URL.Path)
	})

	mux.HandleFunc("/headers", func(w http.ResponseWriter, r *http.Request) {
		globalStats.HTTPReqs.Add(1)
		logger.Info("HTTP /headers", "remote", r.RemoteAddr)
		w.Header().Set("Content-Type", "text/plain")
		for name, vals := range r.Header {
			fmt.Fprintf(w, "%s: %s\n", name, strings.Join(vals, ", "))
		}
	})

	mux.HandleFunc("/body", func(w http.ResponseWriter, r *http.Request) {
		globalStats.HTTPReqs.Add(1)
		body, _ := io.ReadAll(r.Body)
		logger.Info("HTTP /body", "remote", r.RemoteAddr, "size", len(body))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(body) //nolint:errcheck
	})

	mux.HandleFunc("/large", func(w http.ResponseWriter, r *http.Request) {
		globalStats.HTTPReqs.Add(1)
		n := int64(1024 * 1024)
		if s := r.URL.Query().Get("n"); s != "" {
			if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
				n = v
			}
		}
		logger.Info("HTTP /large", "remote", r.RemoteAddr, "bytes", n)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
		chunk := strings.Repeat("A", 32*1024)
		for remaining := n; remaining > 0; {
			write := int64(len(chunk))
			if write > remaining {
				write = remaining
			}
			w.Write([]byte(chunk[:write])) //nolint:errcheck
			remaining -= write
		}
	})

	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		globalStats.HTTPReqs.Add(1)
		ms := 500
		if s := r.URL.Query().Get("ms"); s != "" {
			if v, err := strconv.Atoi(s); err == nil && v >= 0 {
				ms = v
			}
		}
		logger.Info("HTTP /slow", "remote", r.RemoteAddr, "delay_ms", ms)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "slept %d ms\n", ms)
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int64{ //nolint:errcheck
			"tcp_conns":    globalStats.TCPConns.Load(),
			"http_reqs":    globalStats.HTTPReqs.Load(),
			"ws_conns":     globalStats.WSConns.Load(),
			"ws_messages":  globalStats.WSMessages.Load(),
			"bytes_echoed": globalStats.BytesEchoed.Load(),
		})
	})

	// ── WebSocket echo ────────────────────────────────────────────────────
	//
	// /ws — echoes every message frame back unchanged, preserving message type
	// (text or binary). This is the standard WS echo used by most WS test suites.
	//
	// The proxy's handleWebSocket forwards the HTTP Upgrade transparently, so
	// from the origin's point of view this is a direct WS connection.
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		globalStats.HTTPReqs.Add(1)
		conn, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Warn("WS /ws: upgrade failed", "remote", r.RemoteAddr, "err", err)
			return
		}
		globalStats.WSConns.Add(1)
		logger.Info("WS /ws: connection opened", "remote", r.RemoteAddr)
		defer func() {
			conn.Close()
			logger.Info("WS /ws: connection closed", "remote", r.RemoteAddr)
		}()

		for {
			msgType, msg, err := conn.ReadMessage()
			if err != nil {
				if !websocket.IsCloseError(err,
					websocket.CloseNormalClosure,
					websocket.CloseGoingAway,
					websocket.CloseNoStatusReceived,
				) && !isClosedErr(err) {
					logger.Debug("WS /ws: read error", "err", err)
				}
				return
			}
			globalStats.WSMessages.Add(1)
			logger.Debug("WS /ws: echo message", "type", msgType, "len", len(msg))

			if err := conn.WriteMessage(msgType, msg); err != nil {
				if !isClosedErr(err) {
					logger.Debug("WS /ws: write error", "err", err)
				}
				return
			}
		}
	})

	// /ws-latency — like /ws but wraps each reply in a JSON envelope that
	// includes the origin's receive timestamp and the original payload.
	// Lets the client measure round-trip time by comparing send and receive
	// wall-clock times.
	//
	// Request frame (text):  any string
	// Response frame (text): {"sent_at":"<RFC3339Nano>","payload":"<original>"}
	mux.HandleFunc("/ws-latency", func(w http.ResponseWriter, r *http.Request) {
		globalStats.HTTPReqs.Add(1)
		conn, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Warn("WS /ws-latency: upgrade failed", "remote", r.RemoteAddr, "err", err)
			return
		}
		globalStats.WSConns.Add(1)
		logger.Info("WS /ws-latency: connection opened", "remote", r.RemoteAddr)
		defer conn.Close()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			globalStats.WSMessages.Add(1)

			reply, _ := json.Marshal(map[string]string{
				"recv_at": time.Now().UTC().Format(time.RFC3339Nano),
				"payload": string(msg),
			})
			if err := conn.WriteMessage(websocket.TextMessage, reply); err != nil {
				return
			}
		}
	})

	return mux
}

func isClosedErr(err error) bool {
	if err == nil {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "use of closed network connection") ||
		strings.Contains(s, "EOF") ||
		strings.Contains(s, "connection reset by peer")
}
