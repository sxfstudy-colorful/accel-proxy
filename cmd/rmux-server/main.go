// rmux-server runs on the central node (behind accel-proxy egress).
//
// It listens for edge node connections (through accel-proxy tunnel) and
// exposes an HTTP API that lets callers push HTTP requests to specific
// edge nodes and receive the responses.
//
// Usage:
//
//	rmux-server [flags]
//
//	-edge-addr    string   listen address for edge connections    (default "0.0.0.0:8700")
//	-http-addr    string   listen address for HTTP API            (default "0.0.0.0:8701")
//	-push-api-key string   API key for request authentication     (empty = no auth)
//	-log-level    string   log level: debug|info|warn|error       (default "info")
//
// HTTP API:
//
//	POST /request/{node_id}   Push HTTP request to edge node, receive response
//	  Headers:
//	    Authorization: Bearer <push-api-key>  (if configured)
//	    X-Target-URL:  /api/v1/data           (path to forward on the edge)
//	    X-Target-Host: backend.local:8080     (optional Host header override)
//	  Body: request body to forward
//	  Response: the edge node's backend response
//
//	GET  /nodes                List connected edge nodes
//	GET  /health               Liveness probe
//
// Example:
//
//	# Push a GET request to edge node "edge-bj-01", targeting /api/status
//	curl -X GET http://localhost:8701/request/edge-bj-01 \
//	  -H "X-Target-URL: /api/status"
//
//	# Push a POST request with body
//	curl -X POST http://localhost:8701/request/edge-bj-01 \
//	  -H "X-Target-URL: /api/data" \
//	  -H "Content-Type: application/json" \
//	  -d '{"key":"value"}'
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux/server"
)

func main() {
	edgeAddr := flag.String("edge-addr", "0.0.0.0:8700", "listen address for edge connections")
	httpAddr := flag.String("http-addr", "0.0.0.0:8701", "listen address for HTTP API")
	pushAPIKey := flag.String("push-api-key", "", "API key for authentication (empty = no auth)")
	logLevel := flag.String("log-level", "info", "log level: debug|info|warn|error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	srv := server.NewServer(*edgeAddr, *httpAddr, *pushAPIKey, logger)

	logger.Info("rmux-server starting",
		"edge_addr", *edgeAddr,
		"http_addr", *httpAddr,
		"auth", *pushAPIKey != "",
	)

	if err := srv.Run(); err != nil {
		logger.Error("rmux-server exited", "err", err)
		os.Exit(1)
	}
}
