// Package tunnel implements the internal WebSocket-based tunnel protocol
// used between proxy nodes (access → relay → egress).
//
// Wire format
// ───────────
//  1. Client dials WS to the next hop's /tunnel endpoint.
//  2. Client sends a JSON HandshakeRequest as a Text frame.
//  3. Server replies with a JSON HandshakeResponse as a Text frame.
//  4. Both sides exchange raw Binary frames as an opaque byte stream.
//
// This design lets the tunnel traverse HTTP reverse proxies and CDNs because
// it looks like a normal WebSocket upgrade.
package tunnel

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

// Version is the current tunnel protocol version.
const Version = 1

// HandshakeRequest is sent by the dialing side (access/relay node) right after
// the WebSocket handshake completes.
type HandshakeRequest struct {
	// Version must equal tunnel.Version.
	Version int `json:"version"`

	// ServiceID identifies the business service (matches config service ID).
	ServiceID string `json:"service_id"`

	// TargetHost / TargetPort is the final origin address.
	// Relay nodes forward this unchanged so the egress knows where to connect.
	TargetHost string `json:"target_host"`
	TargetPort int    `json:"target_port"`

	// Protocol is the L4/L7 protocol: "tcp", "http", "https".
	Protocol string `json:"protocol"`

	// ClientIP is the original client's remote address, propagated for logging
	// and X-Forwarded-For insertion at the egress.
	ClientIP string `json:"client_ip"`

	// HopCount tracks how many relay hops have been traversed (for TTL-style
	// loop protection).
	HopCount int `json:"hop_count"`

	// Metadata carries arbitrary key-value pairs (e.g. SNI hostname).
	Metadata map[string]string `json:"metadata,omitempty"`
}

// HandshakeResponse is sent back by the receiving side.
type HandshakeResponse struct {
	// Status is "ok" on success or "error" on failure.
	Status string `json:"status"`
	// Message carries an error description when Status == "error".
	Message string `json:"message,omitempty"`
	// NodeID is the responder's node ID, useful for tracing.
	NodeID string `json:"node_id"`
}

// MaxHopCount is the maximum number of relay hops allowed (loop guard).
const MaxHopCount = 16

// SendHandshake writes a HandshakeRequest to the WebSocket connection and
// waits for a HandshakeResponse, applying a generous timeout.
func SendHandshake(conn *websocket.Conn, req *HandshakeRequest) (*HandshakeResponse, error) {
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteJSON(req); err != nil {
		return nil, fmt.Errorf("write handshake request: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("read handshake response: %w", err)
	}

	var resp HandshakeResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return nil, fmt.Errorf("parse handshake response: %w", err)
	}

	if resp.Status != "ok" {
		return &resp, fmt.Errorf("handshake rejected by %q: %s", resp.NodeID, resp.Message)
	}

	// Clear deadlines — caller manages them from here.
	conn.SetReadDeadline(time.Time{})
	conn.SetWriteDeadline(time.Time{})

	return &resp, nil
}

// ReceiveHandshake reads a HandshakeRequest from the WebSocket connection and
// returns it.  The caller must send a HandshakeResponse via SendResponse.
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
		return nil, fmt.Errorf("hop count %d exceeds max %d (possible loop)", req.HopCount, MaxHopCount)
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

// OKResponse returns a successful HandshakeResponse.
func OKResponse(nodeID string) *HandshakeResponse {
	return &HandshakeResponse{Status: "ok", NodeID: nodeID}
}

// ErrResponse returns a failed HandshakeResponse.
func ErrResponse(nodeID, msg string) *HandshakeResponse {
	return &HandshakeResponse{Status: "error", NodeID: nodeID, Message: msg}
}
