package client

import (
	"fmt"
	"io"

	"github.com/sxfstudy-colorful/accel-proxy/internal/mux/muxproto"
)

func (s *EdgeClientSession) handleNewRequest(f *muxproto.Frame) error {
	if s.reqHandler == nil {
		s.logger.Warn("received request but no handler", "sid", f.StreamID)
		if err := s.getConn().WriteFrame(&muxproto.Frame{
			StreamID: f.StreamID,
			Type:     muxproto.TypeRST,
			Payload:  muxproto.Marshal(muxproto.RSTMsg{Reason: "no handler"}),
		}); err != nil {
			s.logger.Error("no handler, handle request error", "err", err)
			return err
		}

		return fmt.Errorf("received request but no handler")
	}

	stream := muxproto.NewStream(f.StreamID, s.getConn())
	s.reqStreamMu.Lock()
	s.reqStreams[f.StreamID] = stream
	s.reqStreamMu.Unlock()

	if err := stream.OnHeaders(f); err != nil {
		s.logger.Warn("bad request headers", "sid", f.StreamID, "err", err)
		_ = stream.SendRST("bad headers")
		s.removeReqStream(f.StreamID)
		return err
	}

	go s.serveRequest(stream)
	return nil
}

// serveRequest handles one server-pushed HTTP request.
// FIX: panic recovery to prevent handler panics from leaking streams.
func (s *EdgeClientSession) serveRequest(stream *muxproto.Stream) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("serveRequest panic recovered",
				"sid", stream.ID, "panic", fmt.Sprintf("%v", r))
			_ = stream.SendRST("internal error") //nolint:errcheck
		}
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

	// ReqBodyReader auto-sends WINDOW_UPDATE(WinDirRequest) as handler reads.
	respMeta, respBody, err := s.reqHandler(s.ctx, reqMeta, stream.ReqBodyReader())
	if err != nil {
		s.logger.Error("handler error", "sid", stream.ID, "err", err)
		stream.SendRST(fmt.Sprintf("handler error: %v", err)) //nolint:errcheck
		return
	}

	if err := stream.SendHeaders(respMeta, 0); err != nil {
		s.logger.Error("send resp headers", "sid", stream.ID, "err", err)
		return
	}

	// Send response body with flow control (Direction B).
	if respBody != nil {
		if closer, ok := respBody.(io.Closer); ok {
			defer closer.Close()
		}
		if err := stream.SendBodyWithFlowControl(s.ctx, respBody, muxproto.WinDirResponse); err != nil {
			s.logger.Error("send resp body", "sid", stream.ID, "err", err)
			return
		}
	} else {
		stream.SendData(nil, muxproto.FlagEndStream) //nolint:errcheck
	}
}
