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

// ─── Push stream receive state ───────────────────────────────────────────────

type pushRecvState struct {
	name        string
	id          uint32
	recvOffset  atomic.Int64
	ackedOffset atomic.Int64
	buf         *RecvBuffer
}

// ─── EdgeClientSession ──────────────────────────────────────────────────────

type EdgeClientSession struct {
	nodeID     string
	accessAddr string
	pushSubs   []string
	reqHandler RequestHandler
	logger     *slog.Logger

	pushStreams map[string]*pushRecvState
	pushByID    map[uint32]*pushRecvState
	pushIDMu    sync.RWMutex

	reqStreams  map[uint32]*muxproto.Stream
	reqStreamMu sync.RWMutex

	conn   *muxproto.MuxConn
	connMu sync.Mutex

	pongCh chan struct{}
	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc
}

func NewEdgeClientSession(
	nodeID, accessAddr string,
	pushSubs []string,
	reqHandler RequestHandler,
	reqTargets []string, // unused, kept for API compat
	logger *slog.Logger,
) *EdgeClientSession {
	ctx, cancel := context.WithCancel(context.Background())
	s := &EdgeClientSession{
		nodeID:      nodeID,
		accessAddr:  accessAddr,
		pushSubs:    pushSubs,
		reqHandler:  reqHandler,
		logger:      logger.With("node_id", nodeID),
		pushStreams: make(map[string]*pushRecvState, len(pushSubs)),
		reqStreams:  make(map[uint32]*muxproto.Stream),
		ctx:         ctx,
		cancel:      cancel,
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
	s.closeConn()
}

func (s *EdgeClientSession) StreamReader(name string) io.Reader {
	if st, ok := s.pushStreams[name]; ok {
		return st.buf
	}
	return nil
}

func (s *EdgeClientSession) ResumeOffset(name string) int64 {
	if st, ok := s.pushStreams[name]; ok {
		return st.recvOffset.Load()
	}
	return 0
}

func (s *EdgeClientSession) PushSubs() []string { return s.pushSubs }

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

func (s *EdgeClientSession) getConn() *muxproto.MuxConn {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.conn
}

func (s *EdgeClientSession) setConn(mc *muxproto.MuxConn) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	s.conn = mc
}

func (s *EdgeClientSession) closeConn() {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
}

func (s *EdgeClientSession) runOnce() error {
	for _, st := range s.pushStreams {
		if st.buf.IsClosed() {
			st.buf.Reopen()
		}
	}

	conn, err := net.DialTimeout("tcp", s.accessAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s: %w", s.accessAddr, err)
	}

	s.setConn(muxproto.NewMuxConn(conn))

	defer func() {
		s.closeConn()
	}()

	// Build capabilities.
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

	if err := s.getConn().WriteFrame(&muxproto.Frame{
		StreamID: muxproto.ControlStreamID,
		Type:     muxproto.TypeRegister,
		Payload:  regPayload,
	}); err != nil {
		return fmt.Errorf("write REGISTER: %w", err)
	}

	f, err := s.getConn().ReadFrame()
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

	ackStop := make(chan struct{})
	if len(s.pushSubs) > 0 {
		go s.ackLoop(ackStop)
	}
	defer close(ackStop)

	pongCh := make(chan struct{}, 4)
	s.pongCh = pongCh
	pingStop := make(chan struct{})
	go s.pingLoop(pingStop, pongCh)
	defer close(pingStop)

	return s.dispatchLoop()
}

func (s *EdgeClientSession) dispatchLoop() error {
	for {
		if s.ctx.Err() != nil {
			return nil
		}
		f, err := s.getConn().ReadFrame()
		if err != nil {
			if s.ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read frame: %w", err)
		}

		switch {
		case f.StreamID == muxproto.ControlStreamID:
			if err := s.handleControl(f); err != nil {
				return fmt.Errorf("handle control: %w", err)
			}

		case f.Type == muxproto.TypePushData:
			s.handlePushData(f)

		case f.Type == muxproto.TypePushRST:
			s.handlePushRST(f)

		case f.Type == muxproto.TypeHeaders && !s.hasReqStream(f.StreamID):
			if err := s.handleNewRequest(f); err != nil {
				return fmt.Errorf("handle new request: %w", err)
			}

		case f.Type == muxproto.TypeHeaders || f.Type == muxproto.TypeData ||
			f.Type == muxproto.TypeRST || f.Type == muxproto.TypeWindowUpdate:
			if err := s.dispatchReqStream(f); err != nil {
				return fmt.Errorf("dispatch req stream: %w", err)
			}

		default:
			s.logger.Debug("unhandled frame", "type", f.Type, "sid", f.StreamID)
		}
	}
}

func (s *EdgeClientSession) handleControl(f *muxproto.Frame) error {
	switch f.Type {
	case muxproto.TypePing:
		if err := s.getConn().WriteFrame(&muxproto.Frame{Type: muxproto.TypePong}); err != nil {
			s.logger.Error("ping error", "err", err)
			return fmt.Errorf("ping error: %w", err)
		}

	case muxproto.TypePong:
		select {
		case s.pongCh <- struct{}{}:
		default:
		}

	case muxproto.TypeGoAway:
		var msg muxproto.GoAwayMsg
		if err := muxproto.Unmarshal(f.Payload, &msg); err != nil {
			s.logger.Error("goaway error", "err", err)
			return fmt.Errorf("goaway error: %w", err)
		}

		s.logger.Info("received GOAWAY", "reason", msg.Reason)
	}

	return nil
}

// ─── Push stream handling ────────────────────────────────────────────────────

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

func (s *EdgeClientSession) ackLoop(stop <-chan struct{}) {
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

func (s *EdgeClientSession) hasReqStream(id uint32) bool {
	s.reqStreamMu.RLock()
	_, ok := s.reqStreams[id]
	s.reqStreamMu.RUnlock()
	return ok
}

func (s *EdgeClientSession) removeReqStream(id uint32) {
	s.reqStreamMu.Lock()
	delete(s.reqStreams, id)
	s.reqStreamMu.Unlock()
}

func (s *EdgeClientSession) dispatchReqStream(f *muxproto.Frame) error {
	s.reqStreamMu.RLock()
	stream := s.reqStreams[f.StreamID]
	s.reqStreamMu.RUnlock()
	if stream == nil {
		s.logger.Warn("frame for unknown req stream, ignore this condition", "sid", f.StreamID)
		return nil
	}

	switch f.Type {
	case muxproto.TypeHeaders:
		if err := stream.OnHeaders(f); err != nil {
			s.logger.Warn("bad request headers", "sid", f.StreamID, "err", err)
			_ = stream.SendRST("bad headers")
			stream.Close()
		}

	case muxproto.TypeData:
		if err := stream.OnData(f); err != nil {
			s.logger.Warn("stream data error — flow control violation",
				"sid", f.StreamID, "err", err)
			mc := s.getConn()
			if mc != nil {
				if err := mc.WriteFrame(&muxproto.Frame{ //nolint:errcheck
					Type: muxproto.TypeGoAway,
					Payload: muxproto.Marshal(muxproto.GoAwayMsg{
						Reason: fmt.Sprintf("flow control violation on stream %d", f.StreamID),
					}),
				}); err != nil {
					s.logger.Warn("flow control violation", "sid", f.StreamID, "err", err)
					return fmt.Errorf("flow control violation on stream %d", f.StreamID)
				}
			}
		}

	case muxproto.TypeWindowUpdate:
		stream.OnWindowUpdate(f.Payload)

	case muxproto.TypeRST:
		var msg muxproto.RSTMsg
		if err := muxproto.Unmarshal(f.Payload, &msg); err != nil {
			s.logger.Warn("bad request rst", "sid", f.StreamID, "err", err)
		}
		stream.OnRST(msg.Reason)
		s.removeReqStream(f.StreamID)
	}

	return nil
}

// ─── Keepalive ───────────────────────────────────────────────────────────────

func (s *EdgeClientSession) pingLoop(stop <-chan struct{}, pongCh <-chan struct{}) {
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
				s.logger.Error("ping timeout")
				s.closeConn()
				return
			}

			if err := s.getConn().WriteFrame(&muxproto.Frame{Type: muxproto.TypePing}); err != nil {
				s.logger.Error("write frame err %+v", err.Error())
			}
		}
	}
}

// ─── Default request handler ─────────────────────────────────────────────────

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
