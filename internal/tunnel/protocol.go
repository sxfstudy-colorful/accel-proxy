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
//
// Routing is IDC-based:
//   - Access node sets TargetIDC once, derived from the service config.
//   - Relay nodes forward this unchanged; they select the next hop purely
//     by looking up TargetIDC in their own route table.
//   - Egress node uses ServiceID to find the local origin address.
//     It does NOT receive a host/port from upstream — origin addresses are
//     a local concern of each IDC and are never transmitted over the wire.
type HandshakeRequest struct {
	// Version must equal tunnel.Version.
	Version int `json:"version"`

	// ServiceID identifies the business service.
	// Used by relay nodes for route table lookup and by egress to find
	// the local origin address.
	ServiceID string `json:"service_id"`

	// TargetIDC is the datacenter that holds the origin for this service.
	// This is the sole routing key carried through the entire hop chain.
	// Set by the access node; never modified by relay nodes.
	TargetIDC string `json:"target_idc"`

	// Protocol is the L4/L7 protocol: "tcp", "http", "https".
	// Carried to egress so it knows whether to dial plain TCP or TLS.
	Protocol string `json:"protocol"`

	// ClientIP is the original client's remote address, propagated for
	// logging and X-Forwarded-For insertion at the access node.
	ClientIP string `json:"client_ip"`

	// HopCount tracks relay hops traversed (loop guard, max = MaxHopCount).
	HopCount int `json:"hop_count"`

	// Metadata carries arbitrary key-value pairs (e.g. SNI hostname, trace ID).
	Metadata map[string]string `json:"metadata,omitempty"`
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
// Used by relay/egress servers that accept inbound tunnels; the dialer uses
// writeHandshakeRequest + readHandshakeResponse separately to enforce the
// retry-safety boundary (see dialer.go).
func SendHandshake(conn *websocket.Conn, req *HandshakeRequest) (*HandshakeResponse, error) {
	if err := writeHandshakeRequest(conn, req); err != nil {
		return nil, err
	}
	return readHandshakeResponse(conn)
}

// writeHandshakeRequest sends the HandshakeRequest frame.
// A failure here means the remote never received the request — safe to retry.
func writeHandshakeRequest(conn *websocket.Conn, req *HandshakeRequest) error {
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteJSON(req); err != nil {
		return fmt.Errorf("write handshake request: %w", err)
	}
	conn.SetWriteDeadline(time.Time{})
	return nil
}

// readHandshakeResponse reads the HandshakeResponse frame.
// By the time this is called the remote has already received the request and
// started building its downstream chain — a failure here must NOT be retried.
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
