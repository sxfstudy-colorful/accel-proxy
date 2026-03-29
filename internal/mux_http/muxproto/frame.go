// Package muxproto defines the binary frame protocol used between mux-client
// and mux-server over a single TCP connection (carried by accel-proxy tunnel).
//
// Frame header (9 bytes, big-endian):
//
//	[0:4]  stream_id  — 0 = control channel, 1+ = data streams (assigned by server)
//	[4]    type       — FrameType constant
//	[5:9]  length     — payload byte count
//
// Control frames (stream_id == 0) carry JSON payloads.
// DATA frames carry raw bytes; stream_id identifies which push stream.
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

// ErrOffsetEvicted is returned when a requested read offset has already been
// trimmed from the StreamBuffer (subscriber fell too far behind).
var ErrOffsetEvicted = errors.New("requested offset has been evicted from buffer")

// FrameType identifies the purpose of a frame.
type FrameType uint8

const (
	TypeRegister     FrameType = 0x01 // edge → server : subscribe + optional resume
	TypeRegisterAck  FrameType = 0x02 // server → edge : stream ID assignments
	TypeData         FrameType = 0x03 // server → edge : push payload
	TypeAck          FrameType = 0x04 // edge → server : cumulative byte ACK
	TypeWindowUpdate FrameType = 0x05 // server → edge : new send window
	TypeRST          FrameType = 0x06 // either side   : reset a stream
	TypePing         FrameType = 0x07 // either side   : keepalive probe
	TypePong         FrameType = 0x08 // either side   : keepalive reply
)

// Frame is the unit of exchange between mux-client and mux-server.
type Frame struct {
	StreamID uint32
	Type     FrameType
	Payload  []byte
}

// WriteTo serialises the frame into w.
// Returns an error if the payload exceeds MaxPayloadSize (defense-in-depth:
// prevents a bug on the sending side from wasting bandwidth on frames that
// the receiver will reject anyway).
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

// ReadFrame reads exactly one frame from r.
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
// Control message payloads (JSON, only on stream_id == 0)
// ─────────────────────────────────────────────────────────────────────────────

type RegisterMsg struct {
	NodeID string           `json:"node_id"`
	Subs   []string         `json:"subs"`
	Resume map[string]int64 `json:"resume,omitempty"`
}

type RegisterAckMsg struct {
	OK        bool              `json:"ok"`
	Message   string            `json:"message,omitempty"`
	StreamIDs map[string]uint32 `json:"stream_ids"`
}

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
