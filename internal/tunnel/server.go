package tunnel

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
)

const certPollInterval = 30 * time.Second

// ConnHandler is called for each accepted tunnel connection.
type ConnHandler func(req *HandshakeRequest, tun *Transport)

// Server is a WebSocket server that accepts inbound tunnel connections
// from access or relay nodes.
type Server struct {
	cfg      *config.TunnelConfig
	nodeID   string
	handler  ConnHandler
	upgrader websocket.Upgrader
	logger   *slog.Logger

	httpServer   *http.Server
	certReloader *CertReloader
}

// NewServer creates a tunnel Server.
func NewServer(cfg *config.TunnelConfig, nodeID string, handler ConnHandler, logger *slog.Logger) *Server {
	path := cfg.Path
	if path == "" {
		path = "/tunnel"
	}

	s := &Server{
		cfg:    cfg,
		nodeID: nodeID,
		handler: handler,
		upgrader: websocket.Upgrader{
			HandshakeTimeout: 10 * time.Second,
			ReadBufferSize:   int(cfg.MaxFrameSize) + 512,
			WriteBufferSize:  int(cfg.MaxFrameSize) + 512,
			CheckOrigin: func(r *http.Request) bool {
				return true // trust internal network; PSK auth covers authentication
			},
		},
		logger: logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc(path, s.handleWebSocket)
	mux.HandleFunc("/health", handleHealth)

	s.httpServer = &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return s
}

// Start begins listening for tunnel connections (blocking).
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("tunnel server listen %q: %w", s.httpServer.Addr, err)
	}

	s.logger.Info("tunnel server listening",
		"addr", s.httpServer.Addr,
		"path", s.cfg.Path,
		"tls", s.cfg.TLS.Enabled,
		"auth", s.cfg.PSK != "",
	)

	if s.cfg.TLS.Enabled {
		reloader, err := NewCertReloader(
			s.cfg.TLS.CertFile,
			s.cfg.TLS.KeyFile,
			certPollInterval,
			s.logger,
		)
		if err != nil {
			return fmt.Errorf("init cert reloader: %w", err)
		}
		s.certReloader = reloader

		info := reloader.CertInfo()
		s.logger.Info("TLS certificate loaded",
			"subject", info.Subject,
			"issuer", info.Issuer,
			"expires", info.NotAfter.Format(time.RFC3339),
			"expires_in", time.Until(info.NotAfter).Round(time.Hour).String(),
			"dns_names", info.DNSNames,
		)

		tlsCfg := &tls.Config{
			GetCertificate: reloader.GetCertificate,
			MinVersion:     tls.VersionTLS12,
			NextProtos:     []string{"http/1.1"},
		}
		ln = tls.NewListener(ln, tlsCfg)
	}

	return s.httpServer.Serve(ln)
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	if s.certReloader != nil {
		s.certReloader.Stop()
	}
	return s.httpServer.Shutdown(ctx)
}

// ListenAddr returns the configured TCP listen address.
func (s *Server) ListenAddr() string { return s.httpServer.Addr }

// ReloadCert triggers an immediate certificate reload from disk.
func (s *Server) ReloadCert() error {
	if s.certReloader == nil {
		return fmt.Errorf("TLS is not enabled on this server")
	}
	return s.certReloader.Reload()
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Warn("WS upgrade failed", "remote", r.RemoteAddr, "err", err)
		return
	}

	req, err := ReceiveHandshake(conn)
	if err != nil {
		s.logger.Warn("handshake failed", "remote", r.RemoteAddr, "err", err)
		if sendErr := SendResponse(conn, ErrResponse(s.nodeID, err.Error())); sendErr != nil {
			s.logger.Debug("send error response failed", "remote", r.RemoteAddr, "err", sendErr)
		}
		conn.Close()
		return
	}

	// ── PSK authentication ───────────────────────────────────────────────
	if err := VerifyRequest(req, s.cfg.PSK); err != nil {
		s.logger.Warn("tunnel auth failed", "remote", r.RemoteAddr, "err", err)
		_ = SendResponse(conn, ErrResponse(s.nodeID, "authentication failed"))
		conn.Close()
		return
	}

	if err := SendResponse(conn, OKResponse(s.nodeID)); err != nil {
		s.logger.Warn("handshake response failed", "remote", r.RemoteAddr, "err", err)
		conn.Close()
		return
	}

	s.logger.Info("tunnel accepted",
		"remote", r.RemoteAddr,
		"service", req.ServiceID,
		"target_idc", req.TargetIDC,
		"client_ip", req.ClientIP,
		"hops", req.HopCount,
	)

	tun := NewTransport(conn, s.cfg.MaxFrameSize, s.logger)
	s.handler(req, tun)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("ok")); err != nil {
		_ = err
	}
}
