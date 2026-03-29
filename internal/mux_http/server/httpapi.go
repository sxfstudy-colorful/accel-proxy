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
//   - an HTTP listener for origin push requests and admin queries
type Server struct {
	broker     *PushBroker
	logger     *slog.Logger
	edgeAddr   string
	httpAddr   string
	pushAPIKey string // if non-empty, POST /push requires Authorization: Bearer <key>

	edgeLn  net.Listener // saved for graceful shutdown
	httpSrv *http.Server
}

// NewServer creates a mux Server.
// pushAPIKey: if non-empty, all POST /push requests must include
// "Authorization: Bearer <pushAPIKey>". Empty = no auth (backward-compatible).
func NewServer(edgeAddr, httpAddr, pushAPIKey string, logger *slog.Logger) *Server {
	return &Server{
		broker:     NewPushBroker(logger),
		logger:     logger,
		edgeAddr:   edgeAddr,
		httpAddr:   httpAddr,
		pushAPIKey: pushAPIKey,
	}
}

// Run starts both listeners and blocks until one of them fails.
func (s *Server) Run() error {
	errCh := make(chan error, 2)

	go func() { errCh <- s.runEdgeListener() }()
	go func() { errCh <- s.runHTTPServer() }()

	return <-errCh
}

// Close gracefully shuts down the server.
func (s *Server) Close(ctx context.Context) error {
	var firstErr error

	// FIX: close the edge TCP listener so Accept() returns.
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

// ─────────────────────────────────────────────────────────────────────────────
// Edge TCP listener
// ─────────────────────────────────────────────────────────────────────────────

func (s *Server) runEdgeListener() error {
	ln, err := net.Listen("tcp", s.edgeAddr)
	if err != nil {
		return fmt.Errorf("edge listener %s: %w", s.edgeAddr, err)
	}
	s.edgeLn = ln // save for Close()
	s.logger.Info("edge listener started", "addr", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			// After Close() is called, Accept returns an error — this is normal.
			return fmt.Errorf("edge accept: %w", err)
		}
		muxConn := muxproto.NewMuxConn(conn)
		go s.broker.HandleEdgeConn(muxConn)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP API
//
//	POST /push/{stream_name}   — origin pushes data (auth required if pushAPIKey set)
//	GET  /streams              — list streams and their write offsets
//	GET  /nodes                — list connected edge node IDs
//	GET  /health               — liveness probe
// ─────────────────────────────────────────────────────────────────────────────

func (s *Server) runHTTPServer() error {
	mux := http.NewServeMux()

	// POST /push/{stream_name}
	mux.HandleFunc("/push/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Auth check.
		if s.pushAPIKey != "" {
			auth := r.Header.Get("Authorization")
			expected := "Bearer " + s.pushAPIKey
			if !strings.EqualFold(auth, expected) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
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
			"stream": name,
			"bytes":  n,
			"dur_ms": dur.Milliseconds(),
		})
	})

	// GET /streams
	mux.HandleFunc("/streams", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.broker.StreamInfo()) //nolint:errcheck
	})

	// GET /nodes
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.broker.NodeIDs()) //nolint:errcheck
	})

	// GET /health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	})

	s.httpSrv = &http.Server{Addr: s.httpAddr, Handler: mux}
	s.logger.Info("HTTP API server started", "addr", s.httpAddr)
	return s.httpSrv.ListenAndServe()
}
