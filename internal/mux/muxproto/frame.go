// Package muxproto defines the binary frame protocol for bidirectional
// multiplexed streams over a single TCP connection (carried by accel-proxy tunnel).
//
// Two stream modes:
//
//	Push stream     — server → client unidirectional data push
//	Request stream  — server pushes HTTP request to client, client returns HTTP response
//
// Frame header (13 bytes, big-endian):
//
//	[0:4]   stream_id   — 0 = control, odd = server-initiated, even = reserved
//	[4]     type        — FrameType
//	[5]     flags       — per-type flags (END_STREAM, END_HEADERS)
//	[6:10]  length      — payload byte count
//	[10:13] reserved    — must be zero
//
// Request stream lifecycle:
//
//	Server                              Client
//	  ├─ HEADERS(sid, req meta) ────────►│
//	  ├─ DATA(sid, chunk) ──────────────►│
//	  ├─ DATA(sid, chunk, END_STREAM) ──►│  client forwards to local backend
//	  │◄── HEADERS(sid, resp meta) ──────┤
//	  │◄── DATA(sid, resp chunk) ────────┤
//	  │◄── DATA(sid, chunk, END_STREAM) ─┤  server reads response
//
// Stream IDs: odd = server-initiated, even = reserved for client-initiated, 0 = control.
package muxproto

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	FrameHeaderSize = 13
	MaxPayloadSize  = 8 * 1024 * 1024
	DataChunkSize   = 32 * 1024
	ControlStreamID = uint32(0)
)

// FrameType identifies the purpose of a frame.
type FrameType uint8

const (
	// Control (stream_id == 0)
	TypeRegister    FrameType = 0x01
	TypeRegisterAck FrameType = 0x02
	TypePing        FrameType = 0x07
	TypePong        FrameType = 0x08
	TypeGoAway      FrameType = 0x09

	// Push streams (server → client, unidirectional data)
	TypePushData FrameType = 0x10
	TypePushAck  FrameType = 0x11
	TypePushRST  FrameType = 0x12

	// Request-response streams (bidirectional)
	TypeHeaders FrameType = 0x20 // HTTP headers (JSON payload)
	TypeData    FrameType = 0x21 // body chunk
	TypeRST     FrameType = 0x22 // abort stream
)

// Flags byte.
type Flags uint8

const (
	FlagEndStream Flags = 0x01 // last frame in this direction
)

func (f Flags) Has(flag Flags) bool { return f&flag != 0 }

// ─────────────────────────────────────────────────────────────────────────────
// Frame
// ─────────────────────────────────────────────────────────────────────────────

type Frame struct {
	StreamID uint32
	Type     FrameType
	Flags    Flags
	Payload  []byte
}

func (f *Frame) HasFlag(flag Flags) bool { return f.Flags.Has(flag) }

func (f *Frame) WriteTo(w io.Writer) error {
	if len(f.Payload) > MaxPayloadSize {
		return fmt.Errorf("payload too large: %d", len(f.Payload))
	}
	var hdr [FrameHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], f.StreamID)
	hdr[4] = byte(f.Type)
	hdr[5] = byte(f.Flags)
	binary.BigEndian.PutUint32(hdr[6:10], uint32(len(f.Payload)))
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
	f := &Frame{
		StreamID: binary.BigEndian.Uint32(hdr[0:4]),
		Type:     FrameType(hdr[4]),
		Flags:    Flags(hdr[5]),
	}
	length := binary.BigEndian.Uint32(hdr[6:10])
	if length > MaxPayloadSize {
		return nil, fmt.Errorf("payload too large: %d", length)
	}
	if length > 0 {
		f.Payload = make([]byte, length)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Control messages (JSON, stream_id == 0)
// ─────────────────────────────────────────────────────────────────────────────

type RegisterMsg struct {
	NodeID       string            `json:"node_id"`
	PushSubs     []string          `json:"push_subs,omitempty"`
	PushResume   map[string]int64  `json:"push_resume,omitempty"`
	Capabilities []string          `json:"caps,omitempty"` // ["reverse-http","push"]
	Metadata     map[string]string `json:"meta,omitempty"`
}

type RegisterAckMsg struct {
	OK            bool              `json:"ok"`
	Message       string            `json:"message,omitempty"`
	PushStreamIDs map[string]uint32 `json:"push_sids,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Request-response metadata (JSON inside HEADERS frame payload)
// ─────────────────────────────────────────────────────────────────────────────

// RequestMeta is sent by server in a HEADERS frame to push an HTTP request to client.
type RequestMeta struct {
	Method  string              `json:"method"`
	URL     string              `json:"url"`
	Host    string              `json:"host,omitempty"`
	Headers map[string][]string `json:"headers,omitempty"`
}

// ResponseMeta is sent by client in a HEADERS frame to return the HTTP response.
type ResponseMeta struct {
	StatusCode int                 `json:"status"`
	Headers    map[string][]string `json:"headers,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Push stream messages
// ─────────────────────────────────────────────────────────────────────────────

type PushAckMsg struct {
	StreamID uint32 `json:"sid"`
	Offset   int64  `json:"off"`
}

type RSTMsg struct {
	Reason string `json:"reason"`
}

type GoAwayMsg struct {
	Reason string `json:"reason"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func Marshal(v any) []byte   { b, _ := json.Marshal(v); return b }
func Unmarshal(d []byte, v any) error { return json.Unmarshal(d, v) }

var (
	ErrStreamReset   = errors.New("stream reset by peer")
	ErrOffsetEvicted = errors.New("offset evicted from buffer")
)

// TypeWindowUpdate is added for per-stream flow control.
// Either side sends this to grant the remote sender more send credits.
const TypeWindowUpdate FrameType = 0x30

// WinDir identifies which direction of a stream the WINDOW_UPDATE applies to.
type WinDir uint8

const (
	// WinDirRequest: credits for server→client (request body DATA frames).
	// Sent by the client (receiver) to allow the server (sender) to send more.
	WinDirRequest WinDir = 0
	// WinDirResponse: credits for client→server (response body DATA frames).
	// Sent by the server (receiver) to allow the client (sender) to send more.
	WinDirResponse WinDir = 1
)

// WindowUpdateMsg is the payload of a TypeWindowUpdate frame.
type WindowUpdateMsg struct {
	StreamID  uint32 `json:"sid"`
	Increment int32  `json:"inc"` // bytes to add to the sender's window
	Dir       WinDir `json:"dir"`
}
