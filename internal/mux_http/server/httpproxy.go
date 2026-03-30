package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux_http/muxproto"
)

// nextRequestID allocates globally unique request IDs for HTTP reverse proxy.
// Starts at 0x80000000 to avoid collision with push stream IDs (which start at 1).
var nextRequestID atomic.Uint32

func init() { nextRequestID.Store(0x80000000) }

func allocRequestID() uint32 {
	for {
		id := nextRequestID.Add(1)
		if id != 0 {
			return id
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// pendingRequest — tracks one in-flight HTTP request awaiting response
// ─────────────────────────────────────────────────────────────────────────────

type pendingRequest struct {
	id     uint32
	nodeID string

	// respCh receives the reconstructed HTTP response or an error.
	// Buffered(1) so the receiver doesn't block the readLoop.
	respCh chan httpResult

	createdAt time.Time
}

type httpResult struct {
	statusCode int
	headers    map[string]string
	body       []byte
	err        error
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTPProxyBroker — coordinates HTTP request dispatch and response collection
// ─────────────────────────────────────────────────────────────────────────────

// HTTPProxyBroker manages sending HTTP requests to edge nodes and collecting
// their responses. It is embedded in PushBroker and shares the same NodeRegistry.
//
// Flow:
//  1. Origin calls POST /proxy/{node_id} with an HTTP request body
//  2. Broker serializes the request into a TypeHTTPRequest frame and sends it
//     to the target edge node's MuxConn
//  3. Edge node executes the request locally and streams back:
//     TypeHTTPResponseHead → TypeHTTPResponseBody* → TypeHTTPResponseEnd
//  4. Broker reassembles the response and returns it to the waiting HTTP handler
type HTTPProxyBroker struct {
	logger *slog.Logger
	nodes  *NodeRegistry

	mu       sync.Mutex
	pending  map[uint32]*pendingRequest // requestID → pending

	timeout time.Duration
}

func NewHTTPProxyBroker(nodes *NodeRegistry, timeout time.Duration, logger *slog.Logger) *HTTPProxyBroker {
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &HTTPProxyBroker{
		logger:  logger,
		nodes:   nodes,
		pending: make(map[uint32]*pendingRequest),
		timeout: timeout,
	}
}

// SendRequest sends an HTTP request to the specified edge node and waits for
// the response. Blocks until the response is fully received or timeout.
func (b *HTTPProxyBroker) SendRequest(ctx context.Context, nodeID string, req *muxproto.HTTPRequestMsg) (*httpResult, error) {
	sess := b.nodes.Get(nodeID)
	if sess == nil {
		return nil, fmt.Errorf("node %q not connected", nodeID)
	}

	reqID := allocRequestID()

	pr := &pendingRequest{
		id:        reqID,
		nodeID:    nodeID,
		respCh:    make(chan httpResult, 1),
		createdAt: time.Now(),
	}

	b.mu.Lock()
	b.pending[reqID] = pr
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.pending, reqID)
		b.mu.Unlock()
	}()

	// Serialize and send the request frame.
	payload := muxproto.Marshal(req)
	if err := sess.conn.WriteFrame(&muxproto.Frame{
		StreamID: reqID,
		Type:     muxproto.TypeHTTPRequest,
		Payload:  payload,
	}); err != nil {
		return nil, fmt.Errorf("write HTTP request frame: %w", err)
	}

	b.logger.Info("http proxy: request sent",
		"request_id", reqID,
		"node", nodeID,
		"method", req.Method,
		"url", req.URL,
	)

	// Wait for response or timeout.
	timeoutCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	select {
	case result := <-pr.respCh:
		return &result, result.err
	case <-timeoutCtx.Done():
		return nil, fmt.Errorf("http proxy: timeout waiting for response from node %q (request_id=%d)", nodeID, reqID)
	}
}

// OnHTTPResponseHead is called by the session readLoop when a TypeHTTPResponseHead
// frame arrives from the edge.
func (b *HTTPProxyBroker) OnHTTPResponseHead(reqID uint32, msg *muxproto.HTTPResponseHeadMsg) {
	b.mu.Lock()
	pr, ok := b.pending[reqID]
	b.mu.Unlock()
	if !ok {
		b.logger.Warn("http proxy: response head for unknown request", "request_id", reqID)
		return
	}
	// Store status+headers; wait for body+end.
	// We use a simple approach: accumulate in the pendingRequest and deliver
	// the complete response on TypeHTTPResponseEnd.
	pr.respCh <- httpResult{
		statusCode: msg.StatusCode,
		headers:    msg.Headers,
	}
}

// OnHTTPResponseComplete is called when we receive a simplified single-frame
// response (head + body combined, used when body is small enough).
func (b *HTTPProxyBroker) OnHTTPResponseComplete(reqID uint32, head *muxproto.HTTPResponseHeadMsg, body []byte) {
	b.mu.Lock()
	pr, ok := b.pending[reqID]
	b.mu.Unlock()
	if !ok {
		b.logger.Warn("http proxy: complete response for unknown request", "request_id", reqID)
		return
	}
	pr.respCh <- httpResult{
		statusCode: head.StatusCode,
		headers:    head.Headers,
		body:       body,
	}
}

// OnHTTPError is called when the edge reports it cannot execute the request.
func (b *HTTPProxyBroker) OnHTTPError(reqID uint32, msg *muxproto.HTTPErrorMsg) {
	b.mu.Lock()
	pr, ok := b.pending[reqID]
	b.mu.Unlock()
	if !ok {
		b.logger.Warn("http proxy: error for unknown request", "request_id", reqID)
		return
	}
	pr.respCh <- httpResult{
		statusCode: msg.Code,
		err:        fmt.Errorf("edge error: %s", msg.Message),
	}
}

// WriteProxyHTTPResponse writes the httpResult as an HTTP response to w.
func WriteProxyHTTPResponse(w http.ResponseWriter, result *httpResult) {
	for k, v := range result.headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(result.statusCode)
	if len(result.body) > 0 {
		w.Write(result.body) //nolint:errcheck
	}
}

// BuildHTTPRequestMsg converts an http.Request into an HTTPRequestMsg for
// transmission over the mux protocol.
func BuildHTTPRequestMsg(r *http.Request) (*muxproto.HTTPRequestMsg, error) {
	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(io.LimitReader(r.Body, int64(muxproto.MaxPayloadSize)))
		if err != nil {
			return nil, fmt.Errorf("read request body: %w", err)
		}
	}

	headers := make(map[string]string, len(r.Header))
	for k, vs := range r.Header {
		headers[k] = vs[0] // flatten to single value
	}

	u := r.URL.String()
	if r.URL.Host == "" {
		u = r.URL.Path
		if r.URL.RawQuery != "" {
			u += "?" + r.URL.RawQuery
		}
	}

	return &muxproto.HTTPRequestMsg{
		Method:  r.Method,
		URL:     u,
		Headers: headers,
		Body:    body,
	}, nil
}

// BuildHTTPResponse reconstructs an *http.Response from an httpResult.
// This is useful for programmatic callers that want a standard http.Response.
func BuildHTTPResponse(result *httpResult) *http.Response {
	resp := &http.Response{
		StatusCode: result.statusCode,
		Status:     fmt.Sprintf("%d %s", result.statusCode, http.StatusText(result.statusCode)),
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(result.body)),
	}
	for k, v := range result.headers {
		resp.Header.Set(k, v)
	}
	return resp
}
