// rmux-client runs on the edge node.
//
// It connects to rmux-server through the accel-proxy tunnel and waits for
// server-pushed HTTP requests. Each request is forwarded to a local backend
// and the response is sent back through the tunnel.
//
// Usage:
//
//	rmux-client [flags]
//
//	-access-addr   string   accel-proxy access node address       (required)
//	-node-id       string   unique identifier for this edge node   (required)
//	-backend       string   local backend URL to forward to        (required)
//	-log-level     string   log level: debug|info|warn|error       (default "info")
//
// Example:
//
//	# Connect through proxy, forward requests to local backend on :8080
//	rmux-client \
//	  -access-addr 10.0.0.1:7000 \
//	  -node-id edge-bj-01 \
//	  -backend http://127.0.0.1:8080
//
// Data flow:
//
//	rmux-server HTTP API
//	  → accel-proxy tunnel (access → relay → egress)
//	  → rmux-client
//	  → local backend (http://127.0.0.1:8080)
//	  → response flows back the same path
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux/client"
)

func main() {
	accessAddr := flag.String("access-addr", "", "accel-proxy access node address (required)")
	nodeID := flag.String("node-id", "", "unique edge node identifier (required)")
	backend := flag.String("backend", "", "local backend URL to forward requests to (required)")
	logLevel := flag.String("log-level", "info", "log level: debug|info|warn|error")
	flag.Parse()

	if *accessAddr == "" || *nodeID == "" || *backend == "" {
		slog.Error("all of -access-addr, -node-id, -backend are required")
		flag.Usage()
		os.Exit(1)
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	// Create an HTTP client for forwarding to the local backend.
	httpClient := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 50,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	handler := client.NewHTTPForwardHandler(*backend, httpClient)
	sess := client.NewSession(*nodeID, *accessAddr, handler, logger)

	logger.Info("rmux-client starting",
		"node_id", *nodeID,
		"access_addr", *accessAddr,
		"backend", *backend,
	)

	// Blocks until Close() is called or process is killed.
	sess.Run()
}
