package client

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

// LocalHTTPServer exposes the received push streams as HTTP endpoints so that
// local processes on the edge node can consume them without knowing anything
// about the mux protocol.
//
// Endpoints:
//
//	GET  /stream/{name}              — stream bytes as chunked response (blocks until EOF)
//	GET  /stream/{name}/offset       — current receive offset (JSON)
//	GET  /streams                    — list all subscribed streams + offsets (JSON)
//	GET  /health                     — liveness probe
type LocalHTTPServer struct {
	sess   *EdgeClientSession
	addr   string
	logger *slog.Logger
}

func NewLocalHTTPServer(sess *EdgeClientSession, addr string, logger *slog.Logger) *LocalHTTPServer {
	return &LocalHTTPServer{sess: sess, addr: addr, logger: logger}
}

// Run starts the HTTP server and blocks until it fails.
func (h *LocalHTTPServer) Run() error {
	mux := http.NewServeMux()

	// GET /stream/{name}
	mux.HandleFunc("/stream/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path[len("/stream/"):]
		if path == "" {
			http.Error(w, "stream name required", http.StatusBadRequest)
			return
		}

		// Strip optional /offset suffix.
		if len(path) > 7 && path[len(path)-7:] == "/offset" {
			name := path[:len(path)-7]
			off := h.sess.ResumeOffset(name)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"stream": name,
				"offset": off,
			})
			return
		}

		name := path
		reader := h.sess.StreamReader(name)
		if reader == nil {
			http.Error(w, fmt.Sprintf("unknown stream %q", name), http.StatusNotFound)
			return
		}

		h.logger.Info("stream consumer connected", "stream", name, "remote", r.RemoteAddr)

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Transfer-Encoding", "chunked")
		w.Header().Set("X-Mux-Stream", name)

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		buf := make([]byte, 64*1024)
		var total int64
		for {
			n, err := reader.Read(buf)
			if n > 0 {
				if _, werr := w.Write(buf[:n]); werr != nil {
					h.logger.Debug("consumer write error", "stream", name, "err", werr)
					return
				}
				total += int64(n)
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
			if err == io.EOF {
				h.logger.Info("stream EOF", "stream", name, "total_bytes", total)
				return
			}
			if err != nil {
				h.logger.Warn("stream read error", "stream", name, "err", err)
				return
			}
			if r.Context().Err() != nil {
				h.logger.Debug("consumer disconnected", "stream", name)
				return
			}
		}
	})

	// GET /streams
	mux.HandleFunc("/streams", func(w http.ResponseWriter, r *http.Request) {
		result := make(map[string]int64, len(h.sess.subs))
		for _, name := range h.sess.subs {
			result[name] = h.sess.ResumeOffset(name)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result) //nolint:errcheck
	})

	// GET /health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	})

	h.logger.Info("local HTTP server started", "addr", h.addr)
	return http.ListenAndServe(h.addr, mux)
}
