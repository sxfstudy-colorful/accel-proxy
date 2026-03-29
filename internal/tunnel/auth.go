package tunnel

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"time"
)

// maxClockSkew is the maximum allowed clock difference (in seconds) between
// the signing node and the verifying node. Requests older than this are
// rejected to prevent replay attacks.
const maxClockSkew = 60

// SignRequest computes an HMAC-SHA256 signature over routing-critical fields
// and sets the Timestamp + Signature fields on req.
//
// Call this in the Dialer immediately before SendHandshake.
// If psk is empty the function is a no-op (unsigned mode).
func SignRequest(req *HandshakeRequest, psk string) {
	if psk == "" {
		return
	}
	req.Timestamp = time.Now().Unix()
	req.Signature = computeHMAC(req, psk)
}

// VerifyRequest checks that the request's HMAC signature matches the expected
// value and that the timestamp is within maxClockSkew of the current time.
//
// Call this in the tunnel Server after ReceiveHandshake.
// If psk is empty, verification is skipped (backward-compatible unsigned mode).
func VerifyRequest(req *HandshakeRequest, psk string) error {
	if psk == "" {
		return nil // unsigned mode — no PSK configured
	}
	drift := math.Abs(float64(time.Now().Unix() - req.Timestamp))
	if drift > maxClockSkew {
		return fmt.Errorf("timestamp drift %.0fs exceeds max %ds", drift, maxClockSkew)
	}
	expected := computeHMAC(req, psk)
	if !hmac.Equal([]byte(req.Signature), []byte(expected)) {
		return fmt.Errorf("HMAC signature mismatch")
	}
	return nil
}

func computeHMAC(req *HandshakeRequest, psk string) string {
	mac := hmac.New(sha256.New, []byte(psk))
	fmt.Fprintf(mac, "%d:%s:%s:%d", req.Version, req.ServiceID, req.TargetIDC, req.Timestamp)
	return hex.EncodeToString(mac.Sum(nil))
}
