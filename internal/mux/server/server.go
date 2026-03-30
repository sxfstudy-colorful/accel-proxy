package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux/muxproto"
)

const (
	pingInterval = 20 * time.Second
	pingTimeout  = 15 * time.Second
)

// Server manages edge connections and exposes an HTTP API for reverse-proxying
// requests to edge nodes and pushing data streams.
type Server struct {
	logger     *slog.Logger
	edgeAddr   string
	httpAddr   string
	pushAPIKey string

	nodes   sync.Map // nodeID → *EdgeSession
	nextSID atomic.Uint32

	edgeLn  net.Listener
	httpSrv *http.Server

	ctx    context.Context
	cancel context.CancelFunc
}

func NewServer(edgeAddr, httpAddr, pushAPIKey string, logger *slog.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		logger:     logger,
		edgeAddr:   edgeAddr,
		httpAddr:   httpAddr,
		pushAPIKey: pushAPIKey,
		ctx:        ctx,
		cancel:     cancel,
	}
	s.nextSID.Store(1) // odd IDs for server-initiated streams
	return s
}

func (s *Server) allocStreamID() uint32 {
	for {
		id := s.nextSID.Add(2) - 2 // 1, 3, 5, ...
		if id != 0 {
			return id
		}
	}
}

func (s *Server) Run() error {
	errCh := make(chan error, 2)
	go func() { errCh <- s.runEdgeListener() }()
	go func() { errCh <- s.runHTTPAPI() }()
	return <-errCh
}

func (s *Server) Close(ctx context.Context) error {
	s.cancel()
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
	return firstErr
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge TCP listener
// ─────────────────────────────────────────────────────────────────────────────

func (s *Server) runEdgeListener() error {
	ln, err := net.Listen("tcp", s.edgeAddr)
	if err != nil {
		return fmt.Errorf("edge listen %s: %w", s.edgeAddr, err)
	}
	s.edgeLn = ln
	s.logger.Info("edge listener started", "addr", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleEdge(muxproto.NewMuxConn(conn))
	}
}

func (s *Server) handleEdge(conn *muxproto.MuxConn) {
	logger := s.logger.With("remote", conn.RemoteAddr())

	f, err := conn.ReadFrame()
	if err != nil || f.Type != muxproto.TypeRegister || f.StreamID != muxproto.ControlStreamID {
		logger.Warn("edge: bad REGISTER", "err", err)
		conn.Close()
		return
	}

	var reg muxproto.RegisterMsg
	if err := muxproto.Unmarshal(f.Payload, &reg); err != nil || reg.NodeID == "" {
		logger.Warn("edge: bad REGISTER payload", "err", err)
		conn.Close()
		return
	}
	logger = logger.With("node_id", reg.NodeID)

	sess := newEdgeSession(reg.NodeID, conn, s, logger)
	sess.capabilities = reg.Capabilities

	ack := muxproto.RegisterAckMsg{OK: true}
	if err := conn.WriteFrame(&muxproto.Frame{
		StreamID: muxproto.ControlStreamID,
		Type:     muxproto.TypeRegisterAck,
		Payload:  muxproto.Marshal(ack),
	}); err != nil {
		logger.Warn("edge: write REGISTER_ACK failed", "err", err)
		conn.Close()
		return
	}

	if old, loaded := s.nodes.LoadAndDelete(reg.NodeID); loaded {
		old.(*EdgeSession).closeWithReason("replaced by new connection")
	}
	s.nodes.Store(reg.NodeID, sess)
	logger.Info("edge registered", "caps", reg.Capabilities)

	sess.readLoop()

	s.nodes.CompareAndDelete(reg.NodeID, sess)
	logger.Info("edge session closed")
}

func (s *Server) getSession(nodeID string) *EdgeSession {
	v, ok := s.nodes.Load(nodeID)
	if !ok {
		return nil
	}
	return v.(*EdgeSession)
}

func (s *Server) nodeIDs() []string {
	var ids []string
	s.nodes.Range(func(k, v any) bool {
		ids = append(ids, k.(string))
		return true
	})
	return ids
}

func (s *Server) requestableNodeIDs() []string {
	var ids []string
	s.nodes.Range(func(k, v any) bool {
		sess := v.(*EdgeSession)
		if sess.hasCapability("reverse-http") {
			ids = append(ids, k.(string))
		}
		return true
	})
	return ids
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP API
//
//   POST|GET|PUT|DELETE|PATCH /dispatch/{node_id}/{path}
//       — reverse proxy: forward this HTTP request to the edge node
//   GET  /nodes               — list all connected nodes
//   GET  /nodes/requestable   — list nodes that accept reverse-proxy requests
//   GET  /health              — liveness
// ─────────────────────────────────────────────────────────────────────────────

func (s *Server) runHTTPAPI() error {
	mux := http.NewServeMux()

	// /dispatch/{node_id}/{path...}
	// The path after the node_id is the URL to forward to the edge's local backend.
	//
	// Example:
	//   POST /dispatch/edge-bj-01/api/v1/data?key=val
	//   → edge receives: POST /api/v1/data?key=val
	//   → edge proxies to its local backend
	//   → response flows back
	mux.HandleFunc("/dispatch/", func(w http.ResponseWriter, r *http.Request) {
		if s.pushAPIKey != "" {
			if r.Header.Get("Authorization") != "Bearer "+s.pushAPIKey {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}

		// Parse: /dispatch/{node_id}/{path...}
		rest := r.URL.Path[len("/dispatch/"):]
		if rest == "" {
			http.Error(w, "path must be /dispatch/{node_id}/{path}", http.StatusBadRequest)
			return
		}
		var nodeID, targetPath string
		if idx := strings.IndexByte(rest, '/'); idx >= 0 {
			nodeID = rest[:idx]
			targetPath = rest[idx:] // includes leading /
		} else {
			nodeID = rest
			targetPath = "/"
		}
		if r.URL.RawQuery != "" {
			targetPath += "?" + r.URL.RawQuery
		}

		sess := s.getSession(nodeID)
		if sess == nil {
			http.Error(w, fmt.Sprintf("node %q not connected", nodeID), http.StatusBadGateway)
			return
		}
		if !sess.hasCapability("reverse-http") {
			http.Error(w, fmt.Sprintf("node %q does not accept requests", nodeID), http.StatusBadGateway)
			return
		}

		// Build request headers (forward all except internal ones).
		fwdHeaders := make(map[string][]string, len(r.Header))
		for k, v := range r.Header {
			if strings.EqualFold(k, "Authorization") {
				continue
			}
			fwdHeaders[k] = v
		}

		reqMeta := &muxproto.RequestMeta{
			Method:  r.Method,
			URL:     targetPath,
			Host:    r.Host,
			Headers: fwdHeaders,
		}

		respMeta, respBody, err := sess.DoRequest(r.Context(), s.allocStreamID(), reqMeta, r.Body)
		if err != nil {
			s.logger.Error("dispatch failed", "node", nodeID, "path", targetPath, "err", err)
			http.Error(w, fmt.Sprintf("dispatch failed: %v", err), http.StatusBadGateway)
			return
		}

		for k, vals := range respMeta.Headers {
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(respMeta.StatusCode)
		if respBody != nil {
			io.Copy(w, respBody) //nolint:errcheck
		}
	})

	mux.HandleFunc("/nodes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.nodeIDs()) //nolint:errcheck
	})

	mux.HandleFunc("/nodes/requestable", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.requestableNodeIDs()) //nolint:errcheck
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	})

	s.httpSrv = &http.Server{Addr: s.httpAddr, Handler: mux}
	s.logger.Info("HTTP API started", "addr", s.httpAddr)
	return s.httpSrv.ListenAndServe()
}
