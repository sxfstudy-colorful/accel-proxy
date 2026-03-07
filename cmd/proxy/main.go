package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sxfstudy-colorful/accel-proxy/internal/config"
	"github.com/sxfstudy-colorful/accel-proxy/internal/node"
)

// Node is a common interface for all proxy node types.
type Node interface {
	Start() error
	Stop()
}

// CertReloader is optionally implemented by nodes that support hot cert reload.
type CertReloader interface {
	ReloadCert() error
}

func main() {
	var (
		configFile = flag.String("config", "config.yaml", "path to config file")
		logLevel   = flag.String("log-level", "", "override log level (debug|info|warn|error)")
	)
	flag.Parse()

	cfg, err := config.Load(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: load config: %v\n", err)
		os.Exit(1)
	}

	if *logLevel != "" {
		cfg.Log.Level = *logLevel
	}

	logger := buildLogger(cfg.Log)
	slog.SetDefault(logger)

	logger.Info("starting accel-proxy",
		"node_id", cfg.Node.ID,
		"node_type", cfg.Node.Type,
	)

	n, err := buildNode(cfg, logger)
	if err != nil {
		logger.Error("init node failed", "err", err)
		os.Exit(1)
	}

	if err := n.Start(); err != nil {
		logger.Error("start node failed", "err", err)
		os.Exit(1)
	}

	// Wait for termination or reload signal.
	quit := make(chan os.Signal, 1)
	reload := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(reload, syscall.SIGHUP)

	for {
		select {
		case sig := <-quit:
			logger.Info("shutdown signal received", "signal", sig)
			n.Stop()
			logger.Info("shutdown complete")
			return

		case <-reload:
			logger.Info("SIGHUP received: reloading TLS certificate")
			if cr, ok := n.(CertReloader); ok {
				if err := cr.ReloadCert(); err != nil {
					logger.Error("cert reload failed", "err", err)
				} else {
					logger.Info("cert reload succeeded")
				}
			} else {
				logger.Warn("this node type does not support cert reload")
			}
		}
	}
}

// buildNode selects and constructs the correct node type from config.
func buildNode(cfg *config.Config, logger *slog.Logger) (Node, error) {
	switch cfg.Node.Type {
	case config.NodeTypeAccess:
		return node.NewAccessNode(cfg, logger)

	case config.NodeTypeRelay:
		return node.NewRelayNode(cfg, logger)

	case config.NodeTypeEgress:
		return node.NewEgressNode(cfg, logger), nil

	default:
		return nil, fmt.Errorf("unknown node type %q", cfg.Node.Type)
	}
}

// buildLogger constructs a slog.Logger from the log config.
func buildLogger(cfg config.LogConfig) *slog.Logger {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	return slog.New(handler)
}
