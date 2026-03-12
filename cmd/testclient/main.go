// testclient is a standalone proxy test client for accel-proxy.
//
// Each "mode" exercises a different aspect of the proxy chain.
// -addr points to the accel-proxy access node; the client never talks to
// testserver directly.
//
// Usage:
//
//	testclient -mode <mode> [flags]
//
// Global flags:
//
//	-addr    string     proxy access address host:port (required)
//	-mode    string     test mode (see below)
//	-n       int        number of requests / connections / messages (default 1)
//	-conc    int        parallel workers (default 1)
//	-timeout duration   per-operation timeout (default 10s)
//	-v                  verbose output
//
// Mode-specific flags:
//
//	-msg     string      payload for tcp-echo (default "hello accel-proxy\n")
//	-size    int         bytes for large mode (default 1048576)
//	-delay   int         backend sleep ms for slow mode (default 500)
//	-wsmsgs  int         messages per WS connection in ws-echo/ws-latency (default 5)
//
// Modes:
//
//	tcp-echo      dial TCP, send -msg, verify echo
//	http-get      GET /hello, check 200 + body
//	http-post     POST /body with payload, verify echo
//	http-hdr      GET /headers, print proxy-injected headers
//	large         download /large?n=N bytes, verify content
//	slow          GET /slow?ms=N, report proxy overhead
//	ws-echo       WebSocket: open -n connections, send -wsmsgs text frames each,
//	              verify every frame is echoed back unchanged
//	ws-latency    WebSocket: measure round-trip latency per message via /ws-latency,
//	              reports p50/p95/p99
//	bench         HTTP throughput: -n GET /hello with -conc workers, report req/s
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ─────────────────────────────────────────────────────────────────────────────
// CLI
// ─────────────────────────────────────────────────────────────────────────────

type globalFlags struct {
	addr    string
	mode    string
	n       int
	conc    int
	timeout time.Duration
	verbose bool
}

func main() {
	var g globalFlags
	flag.StringVar(&g.addr, "addr", "", "proxy access address host:port (required)")
	flag.StringVar(&g.mode, "mode", "", "test mode")
	flag.IntVar(&g.n, "n", 1, "number of requests / connections")
	flag.IntVar(&g.conc, "conc", 1, "parallel workers")
	flag.DurationVar(&g.timeout, "timeout", 10*time.Second, "per-operation timeout")
	flag.BoolVar(&g.verbose, "v", false, "verbose output")

	msg    := flag.String("msg",    "hello accel-proxy\n", "payload for tcp-echo")
	size   := flag.Int64("size",   1024*1024,              "bytes for large mode")
	delay  := flag.Int("delay",    500,                    "backend sleep ms for slow mode")
	wsmsgs := flag.Int("wsmsgs",   5,                      "messages per WS connection")

	flag.Parse()

	level := slog.LevelInfo
	if g.verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	if g.addr == "" {
		fmt.Fprintln(os.Stderr, "ERROR: -addr is required")
		flag.Usage()
		os.Exit(1)
	}
	if g.mode == "" {
		fmt.Fprintln(os.Stderr, "ERROR: -mode is required")
		flag.Usage()
		os.Exit(1)
	}
	if g.conc < 1 {
		g.conc = 1
	}

	var err error
	switch g.mode {
	case "tcp-echo":
		err = runTCPEcho(g, *msg, logger)
	case "http-get":
		err = runHTTPGet(g, logger)
	case "http-post":
		err = runHTTPPost(g, logger)
	case "http-hdr":
		err = runHTTPHeaders(g, logger)
	case "large":
		err = runLarge(g, *size, logger)
	case "slow":
		err = runSlow(g, *delay, logger)
	case "ws-echo":
		err = runWSEcho(g, *wsmsgs, logger)
	case "ws-latency":
		err = runWSLatency(g, *wsmsgs, logger)
	case "bench":
		err = runBench(g, logger)
	default:
		fmt.Fprintf(os.Stderr, "ERROR: unknown mode %q\n", g.mode)
		flag.Usage()
		os.Exit(1)
	}

	if err != nil {
		logger.Error("test failed", "mode", g.mode, "err", err)
		os.Exit(1)
	}
	logger.Info("test passed", "mode", g.mode)
}

// ─────────────────────────────────────────────────────────────────────────────
// tcp-echo
// ─────────────────────────────────────────────────────────────────────────────

func runTCPEcho(g globalFlags, msg string, logger *slog.Logger) error {
	logger.Info("tcp-echo", "addr", g.addr, "n", g.n, "conc", g.conc,
		"msg", strings.TrimRight(msg, "\n"))

	type result struct {
		id  int
		err error
		dur time.Duration
	}
	jobs    := make(chan int, g.n)
	results := make(chan result, g.n)
	for i := 0; i < g.n; i++ {
		jobs <- i
	}
	close(jobs)

	for w := 0; w < g.conc; w++ {
		go func() {
			for id := range jobs {
				start := time.Now()
				err := doTCPEcho(g.addr, msg, g.timeout)
				results <- result{id: id, err: err, dur: time.Since(start)}
			}
		}()
	}

	var failed int
	var total time.Duration
	for i := 0; i < g.n; i++ {
		r := <-results
		total += r.dur
		if r.err != nil {
			logger.Error("tcp-echo failed", "conn", r.id, "err", r.err)
			failed++
		} else {
			logger.Debug("tcp-echo ok", "conn", r.id, "dur", r.dur)
		}
	}
	logger.Info("tcp-echo summary", "total", g.n, "failed", failed,
		"avg_latency", total/time.Duration(g.n))
	if failed > 0 {
		return fmt.Errorf("%d / %d connections failed", failed, g.n)
	}
	return nil
}

func doTCPEcho(addr, msg string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	if _, err := io.WriteString(conn, msg); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.CloseWrite()
	}
	got, err := io.ReadAll(conn)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if string(got) != msg {
		return fmt.Errorf("echo mismatch: sent %q got %q", msg, string(got))
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// http-get / http-post / http-hdr
// ─────────────────────────────────────────────────────────────────────────────

func runHTTPGet(g globalFlags, logger *slog.Logger) error {
	client := httpClient(g.timeout)
	url := fmt.Sprintf("http://%s/hello", g.addr)
	logger.Info("http-get", "url", url, "n", g.n, "conc", g.conc)
	return runHTTPParallel(g, logger, func(id int) error {
		resp, err := client.Get(url)
		if err != nil {
			return fmt.Errorf("GET: %w", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		if !strings.Contains(string(body), "Hello from origin") {
			return fmt.Errorf("unexpected body: %q", string(body))
		}
		logger.Debug("http-get ok", "req", id)
		return nil
	})
}

func runHTTPPost(g globalFlags, logger *slog.Logger) error {
	client := httpClient(g.timeout)
	url     := fmt.Sprintf("http://%s/body", g.addr)
	payload := "test payload from testclient"
	logger.Info("http-post", "url", url, "n", g.n, "conc", g.conc)
	return runHTTPParallel(g, logger, func(id int) error {
		resp, err := client.Post(url, "text/plain", strings.NewReader(payload))
		if err != nil {
			return fmt.Errorf("POST: %w", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		if string(body) != payload {
			return fmt.Errorf("body mismatch: got %q", string(body))
		}
		logger.Debug("http-post ok", "req", id)
		return nil
	})
}

func runHTTPHeaders(g globalFlags, logger *slog.Logger) error {
	client := httpClient(g.timeout)
	url    := fmt.Sprintf("http://%s/headers", g.addr)
	logger.Info("http-hdr", "url", url)
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("GET /headers: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("── Headers seen by origin (%s) ──\n%s\n", g.addr, string(body))
	for _, h := range []string{"X-Forwarded-For", "Via"} {
		if !strings.Contains(string(body), h) {
			return fmt.Errorf("expected header %q not found", h)
		}
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// large
// ─────────────────────────────────────────────────────────────────────────────

func runLarge(g globalFlags, size int64, logger *slog.Logger) error {
	client := httpClient(g.timeout + 60*time.Second)
	url    := fmt.Sprintf("http://%s/large?n=%d", g.addr, size)
	logger.Info("large", "url", url, "size_bytes", size)

	start := time.Now()
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("GET: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	var totalRead int64
	buf := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(buf)
		for i := 0; i < n; i++ {
			if buf[i] != 'A' {
				return fmt.Errorf("corruption at offset %d: got %q", totalRead+int64(i), buf[i])
			}
		}
		totalRead += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read at offset %d: %w", totalRead, err)
		}
	}
	dur  := time.Since(start)
	mbps := float64(totalRead) / dur.Seconds() / (1024 * 1024)
	if totalRead != size {
		return fmt.Errorf("size mismatch: expected %d got %d", size, totalRead)
	}
	logger.Info("large ok",
		"bytes", totalRead,
		"duration", dur.Round(time.Millisecond),
		"throughput_mbps", fmt.Sprintf("%.2f", mbps),
	)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// slow
// ─────────────────────────────────────────────────────────────────────────────

func runSlow(g globalFlags, delay int, logger *slog.Logger) error {
	client := httpClient(g.timeout + time.Duration(delay)*time.Millisecond + 5*time.Second)
	url    := fmt.Sprintf("http://%s/slow?ms=%d", g.addr, delay)
	logger.Info("slow", "url", url, "backend_delay_ms", delay)

	start := time.Now()
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("GET: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	dur      := time.Since(start)
	overhead := dur - time.Duration(delay)*time.Millisecond

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	logger.Info("slow result",
		"total_latency",  dur.Round(time.Millisecond),
		"backend_delay",  time.Duration(delay)*time.Millisecond,
		"proxy_overhead", overhead.Round(time.Millisecond),
	)
	const maxOverhead = 500 * time.Millisecond
	if overhead > maxOverhead {
		return fmt.Errorf("proxy overhead %v exceeds threshold %v", overhead, maxOverhead)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// ws-echo
//
// Opens -n WebSocket connections (with -conc workers) to /ws through the
// HTTP-mode access node. Over each connection it sends -wsmsgs text frames
// and verifies every frame is echoed back unchanged.
//
// This exercises the proxy's handleWebSocket path end-to-end:
//   client ──WS upgrade──► access (HTTP mode) ──tunnel──► relay ──tunnel──► egress ──TCP──► origin /ws
// ─────────────────────────────────────────────────────────────────────────────

func runWSEcho(g globalFlags, msgsPerConn int, logger *slog.Logger) error {
	logger.Info("ws-echo",
		"addr", g.addr, "connections", g.n, "conc", g.conc, "msgs_per_conn", msgsPerConn)

	type result struct {
		id  int
		err error
		dur time.Duration
	}
	jobs    := make(chan int, g.n)
	results := make(chan result, g.n)
	for i := 0; i < g.n; i++ {
		jobs <- i
	}
	close(jobs)

	for w := 0; w < g.conc; w++ {
		go func() {
			for id := range jobs {
				start := time.Now()
				err := doWSEcho(g.addr, id, msgsPerConn, g.timeout, logger)
				results <- result{id: id, err: err, dur: time.Since(start)}
			}
		}()
	}

	var failed int
	var total time.Duration
	for i := 0; i < g.n; i++ {
		r := <-results
		total += r.dur
		if r.err != nil {
			logger.Error("ws-echo failed", "conn", r.id, "err", r.err)
			failed++
		} else {
			logger.Debug("ws-echo ok", "conn", r.id, "dur", r.dur)
		}
	}

	msgs := g.n * msgsPerConn
	logger.Info("ws-echo summary",
		"connections", g.n, "messages", msgs, "failed_conns", failed,
		"avg_conn_dur", total/time.Duration(g.n))
	if failed > 0 {
		return fmt.Errorf("%d / %d WebSocket connections failed", failed, g.n)
	}
	return nil
}

func doWSEcho(accessAddr string, id, msgsPerConn int, timeout time.Duration, logger *slog.Logger) error {
	// The access node is an HTTP proxy: connect via plain ws://.
	// The proxy transparently upgrades and forwards through the tunnel chain.
	url := fmt.Sprintf("ws://%s/ws", accessAddr)

	dialer := websocket.Dialer{
		HandshakeTimeout: timeout,
		NetDial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, accessAddr, timeout)
		},
	}

	conn, resp, err := dialer.Dial(url, nil)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("dial (HTTP %d): %w", resp.StatusCode, err)
		}
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(timeout))
	conn.SetWriteDeadline(time.Now().Add(timeout))

	for i := 0; i < msgsPerConn; i++ {
		sent := fmt.Sprintf("conn=%d msg=%d payload=accel-proxy-ws-test", id, i)
		if err := conn.WriteMessage(websocket.TextMessage, []byte(sent)); err != nil {
			return fmt.Errorf("msg %d write: %w", i, err)
		}
		msgType, got, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("msg %d read: %w", i, err)
		}
		if msgType != websocket.TextMessage {
			return fmt.Errorf("msg %d: expected text frame, got type %d", i, msgType)
		}
		if string(got) != sent {
			return fmt.Errorf("msg %d echo mismatch:\n  sent: %q\n  got:  %q", i, sent, string(got))
		}
		logger.Debug("ws-echo msg ok", "conn", id, "msg", i)
		// Refresh deadlines for the next round.
		conn.SetReadDeadline(time.Now().Add(timeout))
		conn.SetWriteDeadline(time.Now().Add(timeout))
	}

	// Clean close handshake.
	conn.WriteMessage(websocket.CloseMessage, //nolint:errcheck
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// ws-latency
//
// Opens one WebSocket connection to /ws-latency, sends -n messages, records
// round-trip time for each, and prints p50/p95/p99.
//
// The server wraps each reply in JSON with a recv_at timestamp. The client
// records send time and computes RTT = recv time - send time.
// ─────────────────────────────────────────────────────────────────────────────

func runWSLatency(g globalFlags, msgsPerConn int, logger *slog.Logger) error {
	// For latency measurement a single serial connection is clearest.
	total := g.n * msgsPerConn
	logger.Info("ws-latency",
		"addr", g.addr, "connections", g.n, "msgs_per_conn", msgsPerConn, "total_msgs", total)

	url := fmt.Sprintf("ws://%s/ws-latency", g.addr)
	dialer := websocket.Dialer{HandshakeTimeout: g.timeout}

	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	latencies := make([]time.Duration, 0, total)

	for c := 0; c < g.n; c++ {
		for m := 0; m < msgsPerConn; m++ {
			payload := fmt.Sprintf("conn=%d msg=%d", c, m)
			sendAt  := time.Now()
			conn.SetWriteDeadline(sendAt.Add(g.timeout))
			if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
				return fmt.Errorf("write c=%d m=%d: %w", c, m, err)
			}

			conn.SetReadDeadline(time.Now().Add(g.timeout))
			_, raw, err := conn.ReadMessage()
			recvAt := time.Now()
			if err != nil {
				return fmt.Errorf("read c=%d m=%d: %w", c, m, err)
			}

			// Parse server's JSON envelope.
			var envelope struct {
				RecvAt  string `json:"recv_at"`
				Payload string `json:"payload"`
			}
			if err := json.Unmarshal(raw, &envelope); err != nil {
				return fmt.Errorf("parse reply c=%d m=%d: %w", c, m, err)
			}
			if envelope.Payload != payload {
				return fmt.Errorf("payload mismatch c=%d m=%d: got %q", c, m, envelope.Payload)
			}

			rtt := recvAt.Sub(sendAt)
			latencies = append(latencies, rtt)
			logger.Debug("ws-latency msg", "c", c, "m", m, "rtt", rtt)
		}
	}

	sortDurations(latencies)
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)

	fmt.Printf("\n── WebSocket latency (access → relay → egress → origin) ───\n")
	fmt.Printf("  Messages:    %d\n", len(latencies))
	fmt.Printf("  RTT p50:     %v\n", p50.Round(time.Microsecond*100))
	fmt.Printf("  RTT p95:     %v\n", p95.Round(time.Microsecond*100))
	fmt.Printf("  RTT p99:     %v\n", p99.Round(time.Microsecond*100))
	fmt.Printf("  RTT min:     %v\n", latencies[0].Round(time.Microsecond*100))
	fmt.Printf("  RTT max:     %v\n", latencies[len(latencies)-1].Round(time.Microsecond*100))
	fmt.Printf("───────────────────────────────────────────────────────────\n\n")
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// bench
// ─────────────────────────────────────────────────────────────────────────────

func runBench(g globalFlags, logger *slog.Logger) error {
	client := httpClient(g.timeout)
	url    := fmt.Sprintf("http://%s/hello", g.addr)
	logger.Info("bench", "url", url, "n", g.n, "conc", g.conc)

	latencies := make([]time.Duration, g.n)
	var idx      atomic.Int64
	var errCount atomic.Int64

	jobs := make(chan int, g.n)
	for i := 0; i < g.n; i++ {
		jobs <- i
	}
	close(jobs)

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < g.conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				t0 := time.Now()
				resp, err := client.Get(url)
				lat := time.Since(t0)
				if err != nil || resp.StatusCode != http.StatusOK {
					errCount.Add(1)
					if resp != nil {
						resp.Body.Close()
					}
					continue
				}
				io.Copy(io.Discard, resp.Body) //nolint:errcheck
				resp.Body.Close()
				if i := idx.Add(1) - 1; int(i) < len(latencies) {
					latencies[i] = lat
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := int(idx.Load())
	rps   := float64(total) / elapsed.Seconds()

	collected := latencies[:total]
	sortDurations(collected)
	p50 := percentile(collected, 50)
	p95 := percentile(collected, 95)
	p99 := percentile(collected, 99)

	fmt.Printf("\n── Benchmark results ──────────────────────────────\n")
	fmt.Printf("  Target:        %s\n", url)
	fmt.Printf("  Requests:      %d  (concurrency=%d)\n", g.n, g.conc)
	fmt.Printf("  Errors:        %d\n", errCount.Load())
	fmt.Printf("  Total time:    %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Throughput:    %.1f req/s\n", rps)
	fmt.Printf("  Latency p50:   %v\n", p50.Round(time.Microsecond*100))
	fmt.Printf("  Latency p95:   %v\n", p95.Round(time.Microsecond*100))
	fmt.Printf("  Latency p99:   %v\n", p99.Round(time.Microsecond*100))
	fmt.Printf("───────────────────────────────────────────────────\n\n")

	if errCount.Load() > int64(g.n/10) {
		return fmt.Errorf("too many errors: %d / %d", errCount.Load(), g.n)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func runHTTPParallel(g globalFlags, logger *slog.Logger, fn func(id int) error) error {
	jobs := make(chan int, g.n)
	for i := 0; i < g.n; i++ {
		jobs <- i
	}
	close(jobs)

	errs := make(chan error, g.n)
	var wg sync.WaitGroup
	for w := 0; w < g.conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				errs <- fn(id)
			}
		}()
	}
	go func() { wg.Wait(); close(errs) }()

	var failed int
	for err := range errs {
		if err != nil {
			logger.Error("request failed", "err", err)
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d / %d requests failed", failed, g.n)
	}
	return nil
}

func httpClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j] < d[j-1]; j-- {
			d[j], d[j-1] = d[j-1], d[j]
		}
	}
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[(len(sorted)-1)*p/100]
}
