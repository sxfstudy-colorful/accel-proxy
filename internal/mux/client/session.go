package client

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux/muxproto"
)

const (
	recvBufCap    = 64 * 1024 * 1024
	ackEveryBytes = 1 * 1024 * 1024
	ackInterval   = 200 * time.Millisecond

	reconnectBase = 1 * time.Second
	reconnectMax  = 30 * time.Second

	clientPingInterval = 20 * time.Second
	clientPingTimeout  = 15 * time.Second
)

// RequestHandler is called for each reverse-proxy request pushed by the server.
type RequestHandler func(ctx context.Context, req *muxproto.RequestMeta, body io.Reader) (*muxproto.ResponseMeta, io.Reader, error)

// ─────────────────────────────────────────────────────────────────────────────
// Push stream receive state
// ─────────────────────────────────────────────────────────────────────────────

type pushRecvState struct {
	name        string
	id          uint32
	recvOffset  atomic.Int64
	ackedOffset atomic.Int64
	buf         *RecvBuffer
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeClientSession — unified client supporting push streams + reverse proxy
// ─────────────────────────────────────────────────────────────────────────────

type EdgeClientSession struct {
	nodeID     string
	accessAddr string
	pushSubs   []string
	reqHandler RequestHandler
	logger     *slog.Logger

	// Push stream state (keyed by name, rebuilt on connect)
	pushStreams    map[string]*pushRecvState
	pushByID      map[uint32]*pushRecvState
	pushIDMu      sync.RWMutex

	// Request-response streams (keyed by stream ID)
	reqStreams   map[uint32]*muxproto.Stream
	reqStreamMu sync.RWMutex

	conn   *muxproto.MuxConn
	connMu sync.Mutex

	pongCh chan struct{}
	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc
}

// NewEdgeClientSession creates a session that supports:
//   - push stream subscriptions (pushSubs)
//   - reverse-proxied HTTP requests (reqHandler)
//
// At least one of pushSubs or reqHandler must be provided.
func NewEdgeClientSession(
	nodeID, accessAddr string,
	pushSubs []string,
	reqHandler RequestHandler,
	reqTargets []string, // unused, kept for API compat
	logger *slog.Logger,
) *EdgeClientSession {
	ctx, cancel := context.WithCancel(context.Background())
	s := &EdgeClientSession{
		nodeID:     nodeID,
		accessAddr: accessAddr,
		pushSubs:   pushSubs,
		reqHandler: reqHandler,
		logger:     logger.With("node_id", nodeID),
		pushStreams: make(map[string]*pushRecvState, len(pushSubs)),
		reqStreams:  make(map[uint32]*muxproto.Stream),
		ctx:        ctx,
		cancel:     cancel,
	}
	for _, name := range pushSubs {
		s.pushStreams[name] = &pushRecvState{
			name: name,
			buf:  NewRecvBuffer(recvBufCap),
		}
	}
	return s
}

func (s *EdgeClientSession) Close() {
	s.closed.Store(true)
	s.cancel()
	s.connMu.Lock()
	if s.conn != nil {
		s.conn.Close()
	}
	s.connMu.Unlock()
}

// StreamReader returns an io.Reader for the named push stream.
func (s *EdgeClientSession) StreamReader(name string) io.Reader {
	if st, ok := s.pushStreams[name]; ok {
		return st.buf
	}
	return nil
}

// ResumeOffset returns the byte offset a push stream has received so far.
func (s *EdgeClientSession) ResumeOffset(name string) int64 {
	if st, ok := s.pushStreams[name]; ok {
		return st.recvOffset.Load()
	}
	return 0
}

// PushSubs returns the list of push stream names.
func (s *EdgeClientSession) PushSubs() []string { return s.pushSubs }

// Run connects, registers, and reconnects with exponential back-off.
func (s *EdgeClientSession) Run() {
	backoff := reconnectBase
	for !s.closed.Load() {
		if err := s.runOnce(); err != nil {
			if s.closed.Load() {
				return
			}
			s.logger.Warn("session error, reconnecting", "err", err, "backoff", backoff)
			select {
			case <-time.After(backoff):
			case <-s.ctx.Done():
				return
			}
			if backoff < reconnectMax {
				backoff *= 2
			}
		} else {
			backoff = reconnectBase
		}
	}
}

func (s *EdgeClientSession) runOnce() error {
	// Reopen any closed push buffers from previous cycle.
	for _, st := range s.pushStreams {
		if st.buf.IsClosed() {
			st.buf.Reopen()
		}
	}

	conn, err := net.DialTimeout("tcp", s.accessAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s: %w", s.accessAddr, err)
	}
	mc := muxproto.NewMuxConn(conn)

	s.connMu.Lock()
	s.conn = mc
	s.connMu.Unlock()

	defer func() {
		mc.Close()
		s.connMu.Lock()
		if s.conn == mc {
			s.conn = nil
		}
		s.connMu.Unlock()
	}()

	// 1. Build and send REGISTER
	var caps []string
	if s.reqHandler != nil {
		caps = append(caps, "reverse-http")
	}
	if len(s.pushSubs) > 0 {
		caps = append(caps, "push")
	}
	resume := make(map[string]int64)
	for name, st := range s.pushStreams {
		if off := st.recvOffset.Load(); off > 0 {
			resume[name] = off
		}
	}

	regPayload := muxproto.Marshal(muxproto.RegisterMsg{
		NodeID:       s.nodeID,
		PushSubs:     s.pushSubs,
		PushResume:   resume,
		Capabilities: caps,
	})
	if err := mc.WriteFrame(&muxproto.Frame{
		StreamID: muxproto.ControlStreamID,
		Type:     muxproto.TypeRegister,
		Payload:  regPayload,
	}); err != nil {
		return fmt.Errorf("write REGISTER: %w", err)
	}

	// 2. Read REGISTER_ACK
	f, err := mc.ReadFrame()
	if err != nil {
		return fmt.Errorf("read REGISTER_ACK: %w", err)
	}
	if f.Type != muxproto.TypeRegisterAck {
		return fmt.Errorf("expected REGISTER_ACK, got type %d", f.Type)
	}
	var ack muxproto.RegisterAckMsg
	if err := muxproto.Unmarshal(f.Payload, &ack); err != nil || !ack.OK {
		return fmt.Errorf("registration rejected: %s", ack.Message)
	}

	// Rebuild push stream ID mappings.
	byID := make(map[uint32]*pushRecvState, len(ack.PushStreamIDs))
	for name, id := range ack.PushStreamIDs {
		if st, ok := s.pushStreams[name]; ok {
			st.id = id
			byID[id] = st
		}
	}
	s.pushIDMu.Lock()
	s.pushByID = byID
	s.pushIDMu.Unlock()

	s.logger.Info("registered", "push_sids", ack.PushStreamIDs, "caps", caps)

	// 3. Start background goroutines
	ackStop := make(chan struct{})
	if len(s.pushSubs) > 0 {
		go s.ackLoop(mc, ackStop)
	}
	defer close(ackStop)

	pongCh := make(chan struct{}, 4)
	s.pongCh = pongCh
	pingStop := make(chan struct{})
	go s.pingLoop(mc, pingStop, pongCh)
	defer close(pingStop)

	// 4. Read loop
	return s.readLoop(mc)
}

// ─────────────────────────────────────────────────────────────────────────────
// Frame dispatch
// ─────────────────────────────────────────────────────────────────────────────

func (s *EdgeClientSession) readLoop(mc *muxproto.MuxConn) error {
	for {
		if s.ctx.Err() != nil {
			return nil
		}
		f, err := mc.ReadFrame()
		if err != nil {
			if s.ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read frame: %w", err)
		}

		switch {
		// Control frames
		case f.StreamID == muxproto.ControlStreamID:
			s.handleControl(f)

		// Push stream frames
		case f.Type == muxproto.TypePushData:
			s.handlePushData(f)
		case f.Type == muxproto.TypePushRST:
			s.handlePushRST(f)

		// Request-response stream: first HEADERS = new request from server
		case f.Type == muxproto.TypeHeaders && !s.hasReqStream(f.StreamID):
			s.handleNewRequest(mc, f)

		// Request-response stream: continuation frames
		case f.Type == muxproto.TypeHeaders || f.Type == muxproto.TypeData || f.Type == muxproto.TypeRST:
			s.dispatchReqStream(f)

		default:
			s.logger.Debug("unhandled frame", "type", f.Type, "sid", f.StreamID)
		}
	}
}

func (s *EdgeClientSession) handleControl(f *muxproto.Frame) {
	switch f.Type {
	case muxproto.TypePing:
		s.connMu.Lock()
		mc := s.conn
		s.connMu.Unlock()
		if mc != nil {
			mc.WriteFrame(&muxproto.Frame{Type: muxproto.TypePong}) //nolint:errcheck
		}
	case muxproto.TypePong:
		select {
		case s.pongCh <- struct{}{}:
		default:
		}
	case muxproto.TypeGoAway:
		var msg muxproto.GoAwayMsg
		muxproto.Unmarshal(f.Payload, &msg) //nolint:errcheck
		s.logger.Info("received GOAWAY", "reason", msg.Reason)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Push stream handling
// ─────────────────────────────────────────────────────────────────────────────

func (s *EdgeClientSession) handlePushData(f *muxproto.Frame) {
	s.pushIDMu.RLock()
	st := s.pushByID[f.StreamID]
	s.pushIDMu.RUnlock()
	if st == nil {
		s.logger.Warn("PUSH_DATA for unknown stream", "sid", f.StreamID)
		return
	}

	if err := st.buf.Write(s.ctx, f.Payload); err != nil {
		s.logger.Warn("push buffer write error", "stream", st.name, "err", err)
		return
	}
	st.recvOffset.Add(int64(len(f.Payload)))

	if st.recvOffset.Load()-st.ackedOffset.Load() >= ackEveryBytes {
		s.sendPushAck(st)
	}
}

func (s *EdgeClientSession) handlePushRST(f *muxproto.Frame) {
	s.pushIDMu.RLock()
	st := s.pushByID[f.StreamID]
	s.pushIDMu.RUnlock()
	if st == nil {
		return
	}
	var msg muxproto.RSTMsg
	muxproto.Unmarshal(f.Payload, &msg) //nolint:errcheck
	var closeErr error
	if msg.Reason != "EOF" {
		closeErr = fmt.Errorf("push RST: %s", msg.Reason)
	}
	st.buf.Close(closeErr)
	s.logger.Info("push stream closed", "stream", st.name, "reason", msg.Reason)
}

func (s *EdgeClientSession) sendPushAck(st *pushRecvState) {
	s.connMu.Lock()
	mc := s.conn
	s.connMu.Unlock()
	if mc == nil {
		return
	}
	offset := st.recvOffset.Load()
	mc.WriteFrame(&muxproto.Frame{ //nolint:errcheck
		StreamID: muxproto.ControlStreamID,
		Type:     muxproto.TypePushAck,
		Payload:  muxproto.Marshal(muxproto.PushAckMsg{StreamID: st.id, Offset: offset}),
	})
	st.ackedOffset.Store(offset)
}

func (s *EdgeClientSession) ackLoop(mc *muxproto.MuxConn, stop <-chan struct{}) {
	ticker := time.NewTicker(ackInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.pushIDMu.RLock()
			for _, st := range s.pushByID {
				if st.recvOffset.Load() > st.ackedOffset.Load() {
					s.sendPushAck(st)
				}
			}
			s.pushIDMu.RUnlock()
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Request-response stream handling
// ─────────────────────────────────────────────────────────────────────────────

func (s *EdgeClientSession) hasReqStream(id uint32) bool {
	s.reqStreamMu.RLock()
	_, ok := s.reqStreams[id]
	s.reqStreamMu.RUnlock()
	return ok
}

func (s *EdgeClientSession) handleNewRequest(mc *muxproto.MuxConn, f *muxproto.Frame) {
	if s.reqHandler == nil {
		s.logger.Warn("received request but no handler configured", "sid", f.StreamID)
		mc.WriteFrame(&muxproto.Frame{ //nolint:errcheck
			StreamID: f.StreamID,
			Type:     muxproto.TypeRST,
			Payload:  muxproto.Marshal(muxproto.RSTMsg{Reason: "no handler"}),
		})
		return
	}

	stream := muxproto.NewStream(f.StreamID, mc)
	s.reqStreamMu.Lock()
	s.reqStreams[f.StreamID] = stream
	s.reqStreamMu.Unlock()

	// Parse first HEADERS frame.
	if err := stream.OnHeaders(f); err != nil {
		s.logger.Warn("bad request headers", "sid", f.StreamID, "err", err)
		stream.SendRST("bad headers") //nolint:errcheck
		s.removeReqStream(f.StreamID)
		return
	}

	go s.serveRequest(stream)
}

func (s *EdgeClientSession) dispatchReqStream(f *muxproto.Frame) {
	s.reqStreamMu.RLock()
	stream := s.reqStreams[f.StreamID]
	s.reqStreamMu.RUnlock()
	if stream == nil {
		s.logger.Warn("frame for unknown req stream", "sid", f.StreamID)
		return
	}

	switch f.Type {
	case muxproto.TypeHeaders:
		stream.OnHeaders(f) //nolint:errcheck
	case muxproto.TypeData:
		stream.OnData(f) //nolint:errcheck
	case muxproto.TypeRST:
		var msg muxproto.RSTMsg
		muxproto.Unmarshal(f.Payload, &msg) //nolint:errcheck
		stream.OnRST(msg.Reason)
		s.removeReqStream(f.StreamID)
	}
}

func (s *EdgeClientSession) serveRequest(stream *muxproto.Stream) {
	defer func() {
		s.removeReqStream(stream.ID)
		stream.Close()
	}()

	reqMeta, err := stream.RecvRequestHeaders()
	if err != nil {
		s.logger.Warn("recv request headers failed", "sid", stream.ID, "err", err)
		return
	}

	s.logger.Info("reverse request",
		"sid", stream.ID, "method", reqMeta.Method,
		"url", reqMeta.URL, "host", reqMeta.Host)

	respMeta, respBody, err := s.reqHandler(s.ctx, reqMeta, stream.ReqBody())
	if err != nil {
		s.logger.Error("handler error", "sid", stream.ID, "err", err)
		stream.SendRST(fmt.Sprintf("handler error: %v", err)) //nolint:errcheck
		return
	}

	// Send response headers.
	if err := stream.SendHeaders(respMeta, 0); err != nil {
		s.logger.Error("send resp headers", "sid", stream.ID, "err", err)
		return
	}

	// Send response body.
	if respBody != nil {
		if closer, ok := respBody.(io.Closer); ok {
			defer closer.Close()
		}
		buf := make([]byte, muxproto.DataChunkSize)
		for {
			n, readErr := respBody.Read(buf)
			if n > 0 {
				var flags muxproto.Flags
				if readErr != nil {
					flags = muxproto.FlagEndStream
				}
				if sendErr := stream.SendData(buf[:n], flags); sendErr != nil {
					s.logger.Error("send resp body", "sid", stream.ID, "err", sendErr)
					return
				}
			}
			if readErr != nil {
				if n == 0 {
					stream.SendData(nil, muxproto.FlagEndStream) //nolint:errcheck
				}
				break
			}
		}
	} else {
		stream.SendData(nil, muxproto.FlagEndStream) //nolint:errcheck
	}
}

func (s *EdgeClientSession) removeReqStream(id uint32) {
	s.reqStreamMu.Lock()
	delete(s.reqStreams, id)
	s.reqStreamMu.Unlock()
}

// ─────────────────────────────────────────────────────────────────────────────
// Keepalive
// ─────────────────────────────────────────────────────────────────────────────

func (s *EdgeClientSession) pingLoop(mc *muxproto.MuxConn, stop <-chan struct{}, pongCh <-chan struct{}) {
	ticker := time.NewTicker(clientPingInterval)
	defer ticker.Stop()
	lastPong := time.Now()

	for {
		select {
		case <-stop:
			return
		case <-s.ctx.Done():
			return
		case <-pongCh:
			lastPong = time.Now()
		case <-ticker.C:
			if time.Since(lastPong) > clientPingInterval+clientPingTimeout {
				s.logger.Warn("ping timeout")
				mc.Close()
				return
			}
			mc.WriteFrame(&muxproto.Frame{Type: muxproto.TypePing}) //nolint:errcheck
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Default request handler: forward to local HTTP backend
// ─────────────────────────────────────────────────────────────────────────────

// NewHTTPProxyHandler returns a RequestHandler that forwards requests to the
// given backend URL (e.g. "http://127.0.0.1:8080").
func NewHTTPProxyHandler(backendBaseURL string, logger *slog.Logger) RequestHandler {
	httpClient := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 50,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	return func(ctx context.Context, reqMeta *muxproto.RequestMeta, body io.Reader) (*muxproto.ResponseMeta, io.Reader, error) {
		url := strings.TrimRight(backendBaseURL, "/") + reqMeta.URL

		httpReq, err := http.NewRequestWithContext(ctx, reqMeta.Method, url, body)
		if err != nil {
			return &muxproto.ResponseMeta{StatusCode: http.StatusBadGateway}, nil, nil
		}
		if reqMeta.Host != "" {
			httpReq.Host = reqMeta.Host
		}
		for k, vals := range reqMeta.Headers {
			for _, v := range vals {
				httpReq.Header.Add(k, v)
			}
		}

		resp, err := httpClient.Do(httpReq)
		if err != nil {
			logger.Warn("backend request failed", "url", url, "err", err)
			return &muxproto.ResponseMeta{StatusCode: http.StatusBadGateway}, nil, nil
		}

		respHeaders := make(map[string][]string, len(resp.Header))
		for k, v := range resp.Header {
			respHeaders[k] = v
		}

		return &muxproto.ResponseMeta{
			StatusCode: resp.StatusCode,
			Headers:    respHeaders,
		}, resp.Body, nil
	}
}
