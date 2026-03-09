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

	quit := make(chan os.Signal, 1)
	hup := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(hup, syscall.SIGHUP)

	liveCfg := cfg

	for {
		select {
		case sig := <-quit:
			logger.Info("shutdown signal received", "signal", sig)
			n.Stop()
			logger.Info("shutdown complete")
			return

		case <-hup:
			logger.Info("SIGHUP: reloading config", "file", *configFile)
			logger = handleReload(n, *configFile, &liveCfg, logger)
		}
	}
}

func handleReload(n node.Node, configFile string, liveCfg **config.Config, logger *slog.Logger) *slog.Logger {
	newCfg, err := config.Load(configFile)
	if err != nil {
		logger.Error("reload: failed to parse config, keeping current", "err", err)
		return logger
	}

	if cr, ok := n.(node.CertReloader); ok {
		if err := cr.ReloadCert(); err != nil {
			logger.Error("reload: cert reload failed", "err", err)
		} else {
			logger.Info("reload: TLS cert reloaded")
		}
	}

	rc := buildReloadableConfig(newCfg)
	if err := n.Reload(rc); err != nil {
		logger.Error("reload: config reload failed", "err", err)
		return logger
	}

	newLogger := logger
	if newCfg.Log.Level != (*liveCfg).Log.Level {
		newLogger = buildLogger(newCfg.Log)
		slog.SetDefault(newLogger)
		newLogger.Info("reload: log level changed",
			"old", (*liveCfg).Log.Level, "new", newCfg.Log.Level)
	}

	*liveCfg = newCfg

	newLogger.Info("reload: complete")
	return newLogger
}

func buildReloadableConfig(cfg *config.Config) node.ReloadableConfig {
	rc := node.ReloadableConfig{
		Groups:   make(map[string][]config.RouteGroup),
		Routes:   make(map[string][]config.IDCRoute),
		Origins:  make(map[string]node.OriginUpdate),
		LogLevel: cfg.Log.Level,
	}
	for _, svc := range cfg.Services {
		if len(svc.Groups) > 0 {
			rc.Groups[svc.ID] = svc.Groups // access nodes
		}
		if len(svc.Routes) > 0 {
			rc.Routes[svc.ID] = svc.Routes // relay nodes
		}
		if svc.Origin.Host != "" {
			rc.Origins[svc.ID] = node.OriginUpdate{
				Host:        svc.Origin.Host,
				Port:        svc.Origin.Port,
				TLS:         svc.Origin.TLS,
				DialTimeout: svc.Origin.DialTimeout,
			}
		}
	}
	return rc
}

func buildNode(cfg *config.Config, logger *slog.Logger) (node.Node, error) {
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
