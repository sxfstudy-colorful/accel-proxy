// Package tunnel implements the internal WebSocket-based tunnel protocol
// used between proxy nodes (access → relay → egress).
//
// Wire format
// ───────────
//  1. Client dials WS to the next hop's /tunnel endpoint.
//  2. Client sends a JSON HandshakeRequest as a Text frame.
//  3. Server replies with a JSON HandshakeResponse as a Text frame.
//  4. Both sides exchange raw Binary frames as an opaque byte stream.
package tunnel

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

// Version is the current tunnel protocol version.
const Version = 1

// HandshakeRequest is sent by the dialing side right after the WebSocket
// handshake completes.
type HandshakeRequest struct {
	Version   int               `json:"version"`
	ServiceID string            `json:"service_id"`
	TargetIDC string            `json:"target_idc"`
	Protocol  string            `json:"protocol"`
	ClientIP  string            `json:"client_ip"`
	HopCount  int               `json:"hop_count"`
	Metadata  map[string]string `json:"metadata,omitempty"`

	// Authentication fields — set by SignRequest, verified by VerifyRequest.
	// Empty when PSK is not configured (backward-compatible unsigned mode).
	Timestamp int64  `json:"ts,omitempty"`
	Signature string `json:"sig,omitempty"`
}

// HandshakeResponse is sent back by the receiving side.
type HandshakeResponse struct {
	Status  string `json:"status"`            // "ok" or "error"
	Message string `json:"message,omitempty"` // error description
	NodeID  string `json:"node_id"`           // responder's node ID for tracing
}

// MaxHopCount is the maximum number of relay hops allowed (loop guard).
const MaxHopCount = 16

// SendHandshake writes a HandshakeRequest and waits for a HandshakeResponse.
func SendHandshake(conn *websocket.Conn, req *HandshakeRequest) (*HandshakeResponse, error) {
	if err := writeHandshakeRequest(conn, req); err != nil {
		return nil, err
	}
	return readHandshakeResponse(conn)
}

func writeHandshakeRequest(conn *websocket.Conn, req *HandshakeRequest) error {
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteJSON(req); err != nil {
		return fmt.Errorf("write handshake request: %w", err)
	}
	conn.SetWriteDeadline(time.Time{})
	return nil
}

func readHandshakeResponse(conn *websocket.Conn) (*HandshakeResponse, error) {
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("read handshake response: %w", err)
	}
	conn.SetReadDeadline(time.Time{})

	var resp HandshakeResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return nil, fmt.Errorf("parse handshake response: %w", err)
	}
	return &resp, nil
}

// ReceiveHandshake reads a HandshakeRequest from the WebSocket connection.
func ReceiveHandshake(conn *websocket.Conn) (*HandshakeRequest, error) {
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("read handshake request: %w", err)
	}
	conn.SetReadDeadline(time.Time{})

	var req HandshakeRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		return nil, fmt.Errorf("parse handshake request: %w", err)
	}
	if req.Version != Version {
		return nil, fmt.Errorf("unsupported tunnel version %d (want %d)", req.Version, Version)
	}
	if req.HopCount > MaxHopCount {
		return nil, fmt.Errorf("hop count %d exceeds max %d (loop?)", req.HopCount, MaxHopCount)
	}
	if req.TargetIDC == "" {
		return nil, fmt.Errorf("target_idc is empty")
	}
	return &req, nil
}

// SendResponse writes a HandshakeResponse back to the dialing side.
func SendResponse(conn *websocket.Conn, resp *HandshakeResponse) error {
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	err := conn.WriteJSON(resp)
	conn.SetWriteDeadline(time.Time{})
	return err
}

func OKResponse(nodeID string) *HandshakeResponse {
	return &HandshakeResponse{Status: "ok", NodeID: nodeID}
}

func ErrResponse(nodeID, msg string) *HandshakeResponse {
	return &HandshakeResponse{Status: "error", NodeID: nodeID, Message: msg}
}
