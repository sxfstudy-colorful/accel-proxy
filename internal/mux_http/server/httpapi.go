package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux_http/muxproto"
)

// Server ties together:
//   - a TCP listener for edge node connections (forwarded by accel-proxy)
//   - an HTTP listener for origin push requests, HTTP proxy requests, and admin queries
type Server struct {
	broker     *PushBroker
	logger     *slog.Logger
	edgeAddr   string
	httpAddr   string
	pushAPIKey string

	edgeLn  net.Listener
	httpSrv *http.Server
}

// NewServer creates a mux Server.
func NewServer(edgeAddr, httpAddr, pushAPIKey string, logger *slog.Logger) *Server {
	return &Server{
		broker:     NewPushBroker(logger),
		logger:     logger,
		edgeAddr:   edgeAddr,
		httpAddr:   httpAddr,
		pushAPIKey: pushAPIKey,
	}
}

func (s *Server) Run() error {
	errCh := make(chan error, 2)
	go func() { errCh <- s.runEdgeListener() }()
	go func() { errCh <- s.runHTTPServer() }()
	return <-errCh
}

func (s *Server) Close(ctx context.Context) error {
	var firstErr error
	if s.edgeLn != nil {
		if err := s.edgeLn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.httpSrv != nil {
		if err := s.httpSrv.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.broker.Close()
	return firstErr
}

func (s *Server) runEdgeListener() error {
	ln, err := net.Listen("tcp", s.edgeAddr)
	if err != nil {
		return fmt.Errorf("edge listener %s: %w", s.edgeAddr, err)
	}
	s.edgeLn = ln
	s.logger.Info("edge listener started", "addr", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("edge accept: %w", err)
		}
		muxConn := muxproto.NewMuxConn(conn)
		go s.broker.HandleEdgeConn(muxConn)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP API
//
//	POST /push/{stream_name}          — origin pushes data into a named stream
//	POST /proxy/{node_id}             — send HTTP request to edge node, get response
//	GET  /streams                     — list streams and their write offsets
//	GET  /nodes                       — list connected edge nodes with capabilities
//	GET  /health                      — liveness probe
// ─────────────────────────────────────────────────────────────────────────────

func (s *Server) runHTTPServer() error {
	mux := http.NewServeMux()

	// ── POST /push/{stream_name} (stream push) ──────────────────────────
	mux.HandleFunc("/push/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !s.checkAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		name := r.URL.Path[len("/push/"):]
		if name == "" {
			http.Error(w, "stream name required", http.StatusBadRequest)
			return
		}

		start := time.Now()
		s.logger.Info("push started", "stream", name, "remote", r.RemoteAddr)

		n, err := s.broker.Push(r.Context(), name, r.Body)
		dur := time.Since(start)
		if err != nil {
			s.logger.Error("push error", "stream", name, "bytes", n, "dur", dur, "err", err)
			return
		}
		s.logger.Info("push complete", "stream", name, "bytes", n, "dur", dur)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"stream": name, "bytes": n, "dur_ms": dur.Milliseconds(),
		})
	})

	// ── POST /proxy/{node_id} (HTTP reverse proxy) ──────────────────────
	//
	// The request body is the HTTP request to forward to the edge node.
	// The edge node executes it against its local target service and streams
	// the response back. The server reassembles it and returns it as the
	// HTTP response of this endpoint.
	//
	// Headers:
	//   X-Target-URL: http://localhost:8080/api/data  (required — where the edge should forward)
	//   X-Target-Method: GET                          (optional — defaults to request method)
	//
	// The request body is forwarded as-is to the target URL.
	mux.HandleFunc("/proxy/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !s.checkAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		nodeID := r.URL.Path[len("/proxy/"):]
		if nodeID == "" {
			http.Error(w, "node_id required in path", http.StatusBadRequest)
			return
		}

		targetURL := r.Header.Get("X-Target-URL")
		if targetURL == "" {
			http.Error(w, "X-Target-URL header required", http.StatusBadRequest)
			return
		}
		targetMethod := r.Header.Get("X-Target-Method")
		if targetMethod == "" {
			targetMethod = r.Method
		}

		// Build the HTTPRequestMsg from the incoming request.
		reqMsg, err := BuildHTTPRequestMsg(r)
		if err != nil {
			http.Error(w, fmt.Sprintf("build request: %v", err), http.StatusBadRequest)
			return
		}
		// Override with target URL and method.
		reqMsg.URL = targetURL
		reqMsg.Method = targetMethod

		s.logger.Info("http proxy: forwarding",
			"node", nodeID,
			"method", targetMethod,
			"target", targetURL,
			"remote", r.RemoteAddr,
		)

		result, err := s.broker.HTTPProxy.SendRequest(r.Context(), nodeID, reqMsg)
		if err != nil {
			s.logger.Error("http proxy: failed", "node", nodeID, "err", err)
			http.Error(w, fmt.Sprintf("proxy error: %v", err), http.StatusBadGateway)
			return
		}

		WriteProxyHTTPResponse(w, result)
		s.logger.Info("http proxy: complete",
			"node", nodeID,
			"status", result.statusCode,
			"body_bytes", len(result.body),
		)
	})

	// ── GET /streams ─────────────────────────────────────────────────────
	mux.HandleFunc("/streams", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.broker.StreamInfo()) //nolint:errcheck
	})

	// ── GET /nodes ───────────────────────────────────────────────────────
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.broker.NodeInfo()) //nolint:errcheck
	})

	// ── GET /health ──────────────────────────────────────────────────────
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	})

	s.httpSrv = &http.Server{Addr: s.httpAddr, Handler: mux}
	s.logger.Info("HTTP API server started", "addr", s.httpAddr)
	return s.httpSrv.ListenAndServe()
}

func (s *Server) checkAuth(r *http.Request) bool {
	if s.pushAPIKey == "" {
		return true
	}
	auth := r.Header.Get("Authorization")
	expected := "Bearer " + s.pushAPIKey
	return strings.EqualFold(auth, expected)
}
