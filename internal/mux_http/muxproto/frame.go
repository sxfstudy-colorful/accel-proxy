// Package muxproto defines the binary frame protocol used between mux-client
// and mux-server over a single TCP connection (carried by accel-proxy tunnel).
//
// The protocol supports two interaction patterns on the same connection:
//
//  1. Stream push (server → client): server pushes named byte streams to
//     subscribed clients. Clients ACK consumed offsets for flow control.
//
//  2. HTTP reverse proxy (server → client → server): server sends an HTTP
//     request to a client, client forwards it to a local service, and sends
//     the HTTP response back. Each request/response pair shares a unique
//     request ID allocated by the server.
//
// Frame header (9 bytes, big-endian):
//
//	[0:4]  stream_id  — 0 = control channel; for push: stream ID; for HTTP: request ID
//	[4]    type       — FrameType constant
//	[5:9]  length     — payload byte count
package muxproto

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	FrameHeaderSize = 9
	MaxPayloadSize  = 8 * 1024 * 1024 // 8 MiB hard cap per frame
	DataChunkSize   = 32 * 1024       // 32 KiB default DATA frame size
	ControlStreamID = uint32(0)
)

var ErrOffsetEvicted = errors.New("requested offset has been evicted from buffer")

// FrameType identifies the purpose of a frame.
type FrameType uint8

const (
	// ── Control (stream_id == 0) ─────────────────────────────────────────
	TypeRegister    FrameType = 0x01 // edge → server : subscribe + capabilities
	TypeRegisterAck FrameType = 0x02 // server → edge : confirmation

	// ── Stream push (server → edge) ─────────────────────────────────────
	TypeData         FrameType = 0x03 // server → edge : push payload chunk
	TypeAck          FrameType = 0x04 // edge → server : cumulative byte ACK
	TypeWindowUpdate FrameType = 0x05 // server → edge : new send window
	TypeRST          FrameType = 0x06 // either side   : reset a stream

	// ── Keepalive ────────────────────────────────────────────────────────
	TypePing FrameType = 0x07 // either side : keepalive probe
	TypePong FrameType = 0x08 // either side : keepalive reply

	// ── HTTP reverse proxy (server → edge → server) ─────────────────────
	// stream_id field carries the unique request ID for correlation.
	TypeHTTPRequest      FrameType = 0x10 // server → edge : serialized HTTP request
	TypeHTTPResponseHead FrameType = 0x11 // edge → server : HTTP status + headers
	TypeHTTPResponseBody FrameType = 0x12 // edge → server : response body chunk
	TypeHTTPResponseEnd  FrameType = 0x13 // edge → server : end of response (may carry trailer)
	TypeHTTPError        FrameType = 0x14 // edge → server : execution error (target unreachable, etc.)
)

// Frame is the unit of exchange between mux-client and mux-server.
type Frame struct {
	StreamID uint32 // stream push: stream ID; HTTP reverse: request ID
	Type     FrameType
	Payload  []byte
}

func (f *Frame) WriteTo(w io.Writer) error {
	if len(f.Payload) > MaxPayloadSize {
		return fmt.Errorf("frame payload too large to write: %d > %d", len(f.Payload), MaxPayloadSize)
	}
	var hdr [FrameHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], f.StreamID)
	hdr[4] = byte(f.Type)
	binary.BigEndian.PutUint32(hdr[5:9], uint32(len(f.Payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(f.Payload) > 0 {
		_, err := w.Write(f.Payload)
		return err
	}
	return nil
}

func ReadFrame(r io.Reader) (*Frame, error) {
	var hdr [FrameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	streamID := binary.BigEndian.Uint32(hdr[0:4])
	typ := FrameType(hdr[4])
	length := binary.BigEndian.Uint32(hdr[5:9])

	if length > MaxPayloadSize {
		return nil, fmt.Errorf("frame payload too large: %d > %d", length, MaxPayloadSize)
	}
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}
	return &Frame{StreamID: streamID, Type: typ, Payload: payload}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Control message payloads (JSON, stream_id == 0)
// ─────────────────────────────────────────────────────────────────────────────

// RegisterMsg is sent by the edge on connect.
// Caps declares which features the edge supports.
type RegisterMsg struct {
	NodeID string           `json:"node_id"`
	Subs   []string         `json:"subs"`             // stream names to subscribe (push mode)
	Resume map[string]int64 `json:"resume,omitempty"` // push resume offsets
	Caps   []string         `json:"caps,omitempty"`   // capabilities: "push", "http_proxy"
}

type RegisterAckMsg struct {
	OK        bool              `json:"ok"`
	Message   string            `json:"message,omitempty"`
	StreamIDs map[string]uint32 `json:"stream_ids,omitempty"` // push stream IDs
}

// ── Stream push payloads ─────────────────────────────────────────────────────

type AckMsg struct {
	StreamID uint32 `json:"sid"`
	Offset   int64  `json:"off"`
}

type WindowUpdateMsg struct {
	StreamID uint32 `json:"sid"`
	Window   int64  `json:"window"`
}

type RSTMsg struct {
	StreamID uint32 `json:"sid"`
	Reason   string `json:"reason"`
}

// ── HTTP reverse proxy payloads ──────────────────────────────────────────────

// HTTPRequestMsg is the TypeHTTPRequest payload. The server serializes an
// inbound HTTP request and sends it to the edge node for local execution.
// StreamID (in the frame header) carries the unique request ID.
type HTTPRequestMsg struct {
	Method  string            `json:"method"`
	URL     string            `json:"url"`     // full URL including scheme+host or just path
	Headers map[string]string `json:"headers"` // flattened headers (single value per key)
	Body    []byte            `json:"body,omitempty"`
}

// HTTPResponseHeadMsg is the TypeHTTPResponseHead payload.
// Sent by the edge once the local HTTP response headers are available.
type HTTPResponseHeadMsg struct {
	StatusCode int               `json:"status"`
	Headers    map[string]string `json:"headers"`
}

// HTTPResponseEndMsg is the TypeHTTPResponseEnd payload.
// Signals the end of the response body. Optional error field for partial failures.
type HTTPResponseEndMsg struct {
	Error string `json:"error,omitempty"`
}

// HTTPErrorMsg is the TypeHTTPError payload.
// Sent instead of response when the edge cannot execute the request at all.
type HTTPErrorMsg struct {
	Code    int    `json:"code"`    // suggested HTTP status (502, 504, etc.)
	Message string `json:"message"`
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON helpers
// ─────────────────────────────────────────────────────────────────────────────

func Marshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
