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

	roles := activeRoles(cfg)
	logger.Info("starting accel-proxy", "node_id", cfg.Node.ID, "roles", roles)

	srv, err := node.NewProxyServer(cfg, logger)
	if err != nil {
		logger.Error("init failed", "err", err)
		os.Exit(1)
	}

	if err := srv.Start(); err != nil {
		logger.Error("start failed", "err", err)
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
			srv.Stop()
			logger.Info("shutdown complete")
			return

		case <-hup:
			logger.Info("SIGHUP: reloading config", "file", *configFile)
			logger = handleReload(srv, *configFile, &liveCfg, logger)
		}
	}
}

func handleReload(srv *node.ProxyServer, configFile string, liveCfg **config.Config, logger *slog.Logger) *slog.Logger {
	newCfg, err := config.Load(configFile)
	if err != nil {
		logger.Error("reload: failed to parse config, keeping current", "err", err)
		return logger
	}

	// ── 1. TLS certificate reload ─────────────────────────────────────────
	if err := srv.ReloadCert(); err != nil {
		logger.Error("reload: cert reload failed", "err", err)
	} else {
		logger.Info("reload: TLS cert reloaded")
	}

	// ── 2. Routes / origins hot-reload ────────────────────────────────────
	rc := buildReloadableConfig(newCfg)
	if err := srv.Reload(rc); err != nil {
		logger.Error("reload: config reload failed", "err", err)
		return logger
	}

	// ── 3. Log level ──────────────────────────────────────────────────────
	newLogger := logger
	if newCfg.Log.Level != (*liveCfg).Log.Level {
		newLogger = buildLogger(newCfg.Log)
		slog.SetDefault(newLogger)
		newLogger.Info("reload: log level changed",
			"old", (*liveCfg).Log.Level, "new", newCfg.Log.Level)
	}

	// ── 4. Advance baseline for next reload ───────────────────────────────
	*liveCfg = newCfg
	newLogger.Info("reload: complete")
	return newLogger
}

// buildReloadableConfig extracts the hot-reloadable subset of a config.
// Each role only consumes the fields relevant to it.
func buildReloadableConfig(cfg *config.Config) node.ReloadableConfig {
	rc := node.ReloadableConfig{
		Groups:  make(map[string][]config.RouteGroup),
		Routes:  make(map[string][]config.IDCRoute),
		Origins: make(map[string]node.OriginUpdate),
		LogLevel: cfg.Log.Level,
	}

	if cfg.Access != nil {
		rc.AccessServices = cfg.Access.Services
		for _, svc := range cfg.Access.Services {
			if len(svc.Groups) > 0 {
				rc.Groups[svc.ID] = svc.Groups
			}
		}
	}

	if cfg.Relay != nil {
		for _, svc := range cfg.Relay.Services {
			if len(svc.Routes) > 0 {
				rc.Routes[svc.ID] = svc.Routes
			}
		}
	}

	if cfg.Egress != nil {
		for _, svc := range cfg.Egress.Services {
			if svc.Origin.Host != "" {
				rc.Origins[svc.ID] = node.OriginUpdate{
					Host:        svc.Origin.Host,
					Port:        svc.Origin.Port,
					TLS:         svc.Origin.TLS,
					DialTimeout: svc.Origin.DialTimeout,
				}
			}
		}
	}

	return rc
}

// activeRoles returns a string slice of configured role names for logging.
func activeRoles(cfg *config.Config) []string {
	var roles []string
	if cfg.Access != nil {
		roles = append(roles, "access")
	}
	if cfg.Relay != nil {
		roles = append(roles, "relay")
	}
	if cfg.Egress != nil {
		roles = append(roles, "egress")
	}
	return roles
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
