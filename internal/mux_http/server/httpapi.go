package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux_http/muxproto"
)

// Server ties together:
//   - a TCP listener for edge node connections (forwarded by accel-proxy)
//   - an HTTP listener for origin push requests and admin queries
type Server struct {
	broker   *PushBroker
	logger   *slog.Logger
	edgeAddr string
	httpAddr string
}

func NewServer(edgeAddr, httpAddr string, logger *slog.Logger) *Server {
	return &Server{
		broker:   NewPushBroker(logger),
		logger:   logger,
		edgeAddr: edgeAddr,
		httpAddr: httpAddr,
	}
}

// Run starts both listeners and blocks until one of them fails.
func (s *Server) Run() error {
	errCh := make(chan error, 2)

	go func() { errCh <- s.runEdgeListener() }()
	go func() { errCh <- s.runHTTPServer() }()

	return <-errCh
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge TCP listener (receives connections forwarded by accel-proxy)
// ─────────────────────────────────────────────────────────────────────────────

func (s *Server) runEdgeListener() error {
	ln, err := net.Listen("tcp", s.edgeAddr)
	if err != nil {
		return fmt.Errorf("edge listener %s: %w", s.edgeAddr, err)
	}
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
//	POST /push/{stream_name}         — origin pushes data into the named stream
//	GET  /streams                    — list streams and their write offsets
//	GET  /nodes                      — list connected edge node IDs
//	GET  /health                     — liveness probe
// ─────────────────────────────────────────────────────────────────────────────

func (s *Server) runHTTPServer() error {
	mux := http.NewServeMux()

	// POST /push/{stream_name}
	mux.HandleFunc("/push/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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
			// Headers may already be sent if we streamed; log but don't WriteHeader again.
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

	srv := &http.Server{Addr: s.httpAddr, Handler: mux}
	s.logger.Info("HTTP API server started", "addr", s.httpAddr)
	return srv.ListenAndServe()
}
