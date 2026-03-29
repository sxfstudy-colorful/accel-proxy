// mux-server runs on the central node (behind accel-proxy egress).
//
// Usage:
//
//	mux-server [flags]
//
//	-edge-addr    string   listen address for edge connections   (default "0.0.0.0:8700")
//	-http-addr    string   listen address for HTTP API           (default "0.0.0.0:8701")
//	-push-api-key string   API key for POST /push (empty = no auth)
//	-log-level    string   log level: debug|info|warn|error      (default "info")
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux_http/server"
)

func main() {
	edgeAddr := flag.String("edge-addr", "0.0.0.0:8700", "listen address for edge node connections")
	httpAddr := flag.String("http-addr", "0.0.0.0:8701", "listen address for HTTP API")
	pushAPIKey := flag.String("push-api-key", "", "API key for POST /push authentication (empty = no auth)")
	logLevel := flag.String("log-level", "info", "log level: debug|info|warn|error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	srv := server.NewServer(*edgeAddr, *httpAddr, *pushAPIKey, logger)

	logger.Info("mux-server starting",
		"edge_addr", *edgeAddr,
		"http_addr", *httpAddr,
		"push_auth", *pushAPIKey != "",
	)

	if err := srv.Run(); err != nil {
		logger.Error("mux-server exited", "err", err)
		os.Exit(1)
	}
}
