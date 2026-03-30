// mux-server-v2 runs on the central node (behind accel-proxy egress).
//
// Capabilities:
//   - Push streams: origin POSTs data → server pushes to subscribed edge nodes
//   - Reverse proxy: server sends HTTP request → edge proxies to local backend → response returns
//
// Usage:
//
//	mux-server-v2 [flags]
//
//	-edge-addr    string   listen for edge connections  (default "0.0.0.0:8700")
//	-http-addr    string   HTTP API listen address      (default "0.0.0.0:8701")
//	-push-api-key string   API key for auth (empty = no auth)
//	-log-level    string   debug|info|warn|error        (default "info")
//
// HTTP API:
//
//	POST /push/{stream_name}         — push data to named stream
//	POST /dispatch/{node_id}/{path}  — reverse proxy request to edge node
//	GET  /nodes                      — list all connected nodes
//	GET  /nodes/requestable          — list nodes accepting requests
//	GET  /streams                    — list push streams
//	GET  /health                     — liveness
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux/server"
)

func main() {
	edgeAddr := flag.String("edge-addr", "0.0.0.0:8700", "listen for edge connections")
	httpAddr := flag.String("http-addr", "0.0.0.0:8701", "HTTP API listen address")
	pushAPIKey := flag.String("push-api-key", "", "API key for auth")
	logLevel := flag.String("log-level", "info", "log level")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	srv := server.NewServer(*edgeAddr, *httpAddr, *pushAPIKey, logger)
	logger.Info("mux-server-v2 starting",
		"edge_addr", *edgeAddr,
		"http_addr", *httpAddr,
		"push_auth", *pushAPIKey != "",
	)

	if err := srv.Run(); err != nil {
		logger.Error("mux-server-v2 exited", "err", err)
		os.Exit(1)
	}
}
