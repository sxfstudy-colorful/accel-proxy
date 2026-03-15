// mux-client runs on the edge node.
//
// It connects to mux-server through the accel-proxy tunnel, subscribes to one
// or more named push streams, and exposes each stream as a local HTTP endpoint
// so on-box consumers can read data with a plain HTTP GET.
//
// Usage:
//
//	mux-client [flags]
//
//	-access-addr   string   accel-proxy access node address        (required)
//	-node-id       string   unique identifier for this edge node    (required)
//	-streams       string   comma-separated stream names to subscribe (required)
//	-local-addr    string   local HTTP listen address               (default "127.0.0.1:8800")
//	-log-level     string   log level: debug|info|warn|error        (default "info")
//
// Examples:
//
//	# Subscribe to two streams through the proxy, expose on :8800
//	mux-client \
//	  -access-addr 10.0.0.1:7000 \
//	  -node-id edge-bj-01 \
//	  -streams video-feed,telemetry \
//	  -local-addr 0.0.0.0:8800
//
//	# Consume stream on the same machine:
//	curl http://localhost:8800/stream/video-feed | tee output.bin | wc -c
package main

import (
	"flag"
	"log/slog"
	"os"
	"strings"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux_http/client"
)

func main() {
	accessAddr := flag.String("access-addr", "", "accel-proxy access node address host:port (required)")
	nodeID := flag.String("node-id", "", "unique edge node identifier (required)")
	streamsRaw := flag.String("streams", "", "comma-separated stream names to subscribe (required)")
	localAddr := flag.String("local-addr", "127.0.0.1:8800", "local HTTP server listen address")
	logLevel := flag.String("log-level", "info", "log level: debug|info|warn|error")
	flag.Parse()

	// ── Validation ────────────────────────────────────────────────────────
	if *accessAddr == "" {
		slog.Error("-access-addr is required")
		flag.Usage()
		os.Exit(1)
	}
	if *nodeID == "" {
		slog.Error("-node-id is required")
		flag.Usage()
		os.Exit(1)
	}
	if *streamsRaw == "" {
		slog.Error("-streams is required")
		flag.Usage()
		os.Exit(1)
	}

	subs := make([]string, 0)
	for _, s := range strings.Split(*streamsRaw, ",") {
		if t := strings.TrimSpace(s); t != "" {
			subs = append(subs, t)
		}
	}
	if len(subs) == 0 {
		slog.Error("-streams must contain at least one name")
		os.Exit(1)
	}

	// ── Logger ────────────────────────────────────────────────────────────
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	logger.Info("mux-client starting",
		"node_id", *nodeID,
		"access_addr", *accessAddr,
		"streams", subs,
		"local_addr", *localAddr,
	)

	// ── Build and run session ─────────────────────────────────────────────
	sess := client.NewEdgeClientSession(*nodeID, *accessAddr, subs, logger)

	// Start local HTTP server in a goroutine; it exposes the stream readers.
	httpSrv := client.NewLocalHTTPServer(sess, *localAddr, logger)
	go func() {
		if err := httpSrv.Run(); err != nil {
			logger.Error("local HTTP server error", "err", err)
			os.Exit(1)
		}
	}()

	// Run the session (connects, registers, reconnects on failure).
	// Blocks until the process is killed.
	sess.Run()
}
