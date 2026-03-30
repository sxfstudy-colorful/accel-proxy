// mux-client-v2 runs on the edge node.
//
// Capabilities:
//   - Receives push streams from server, exposes as local HTTP GET endpoints
//   - Accepts reverse-proxied HTTP requests from server, proxies to local backend
//
// Usage:
//
//	mux-client-v2 [flags]
//
//	-access-addr   string   accel-proxy access node address       (required)
//	-node-id       string   unique edge node identifier           (required)
//	-streams       string   comma-separated push stream names     (optional)
//	-backend       string   local backend for reverse proxy       (optional, e.g. "http://127.0.0.1:8080")
//	-local-addr    string   local HTTP listen address             (default "127.0.0.1:8800")
//	-log-level     string   log level                             (default "info")
//
// Examples:
//
//	# Subscribe to push streams + accept reverse-proxied requests
//	mux-client-v2 \
//	  -access-addr 10.0.0.1:7000 \
//	  -node-id edge-bj-01 \
//	  -streams video-feed,telemetry \
//	  -backend http://127.0.0.1:8080 \
//	  -local-addr 0.0.0.0:8800
//
//	# Reverse proxy only (no push streams)
//	mux-client-v2 \
//	  -access-addr 10.0.0.1:7000 \
//	  -node-id edge-bj-01 \
//	  -backend http://127.0.0.1:9090
//
//	# Server sends request to edge:
//	curl -X POST http://mux-server:8701/dispatch/edge-bj-01/api/status
//	# → server sends request through tunnel → edge proxies to 127.0.0.1:8080/api/status
//	# → response flows back through tunnel → curl receives the response
package main

import (
	"flag"
	"log/slog"
	"os"
	"strings"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux/client"
)

func main() {
	accessAddr := flag.String("access-addr", "", "accel-proxy access node address (required)")
	nodeID := flag.String("node-id", "", "unique edge node identifier (required)")
	streamsRaw := flag.String("streams", "", "comma-separated push stream names")
	backend := flag.String("backend", "", "local backend URL for reverse proxy (e.g. http://127.0.0.1:8080)")
	localAddr := flag.String("local-addr", "127.0.0.1:8800", "local HTTP listen address")
	logLevel := flag.String("log-level", "info", "log level")
	flag.Parse()

	if *accessAddr == "" || *nodeID == "" {
		slog.Error("-access-addr and -node-id are required")
		flag.Usage()
		os.Exit(1)
	}

	// Parse push stream names.
	var pushSubs []string
	if *streamsRaw != "" {
		for _, s := range strings.Split(*streamsRaw, ",") {
			if t := strings.TrimSpace(s); t != "" {
				pushSubs = append(pushSubs, t)
			}
		}
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	// Build request handler if backend is specified.
	var reqHandler client.RequestHandler
	var reqTargets []string
	if *backend != "" {
		reqHandler = client.NewHTTPProxyHandler(*backend, logger)
		reqTargets = []string{*backend}
	}

	if len(pushSubs) == 0 && reqHandler == nil {
		slog.Error("at least one of -streams or -backend is required")
		flag.Usage()
		os.Exit(1)
	}

	logger.Info("mux-client-v2 starting",
		"node_id", *nodeID,
		"access_addr", *accessAddr,
		"push_streams", pushSubs,
		"backend", *backend,
		"local_addr", *localAddr,
	)

	sess := client.NewEdgeClientSession(
		*nodeID, *accessAddr, pushSubs,
		reqHandler, reqTargets, logger,
	)

	// Start local HTTP server for push stream consumers.
	if len(pushSubs) > 0 {
		httpSrv := client.NewLocalHTTPServer(sess, *localAddr, logger)
		go func() {
			if err := httpSrv.Run(); err != nil {
				logger.Error("local HTTP server error", "err", err)
				os.Exit(1)
			}
		}()
	}

	// Run the session (connects, registers, handles requests, reconnects).
	sess.Run()
}
