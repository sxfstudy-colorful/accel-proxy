package tunnel

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// CertReloader holds a TLS certificate in memory and reloads it from disk
// whenever the files change (detected by mtime) or when Reload() is called
// explicitly (e.g. triggered by SIGHUP).
//
// It is safe for concurrent use: the hot path (GetCertificate / GetConfigForClient)
// only takes an RLock, so ongoing TLS handshakes are never blocked by a reload.
type CertReloader struct {
	certFile string
	keyFile  string
	logger   *slog.Logger

	mu          sync.RWMutex
	cert        *tls.Certificate // current certificate, guarded by mu
	certModTime time.Time        // mtime of cert file at last load
	keyModTime  time.Time        // mtime of key  file at last load

	stopCh chan struct{}
}

// NewCertReloader creates a CertReloader and performs the initial load.
// It starts a background goroutine that polls for file changes every interval.
// Call Stop() to shut it down.
func NewCertReloader(certFile, keyFile string, pollInterval time.Duration, logger *slog.Logger) (*CertReloader, error) {
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}

	r := &CertReloader{
		certFile: certFile,
		keyFile:  keyFile,
		logger:   logger,
		stopCh:   make(chan struct{}),
	}

	// Initial load — fail fast if the files are invalid.
	if err := r.load(); err != nil {
		return nil, fmt.Errorf("initial cert load: %w", err)
	}

	go r.pollLoop(pollInterval)
	return r, nil
}

// GetCertificate satisfies tls.Config.GetCertificate.
// Called by the TLS stack on every inbound handshake.
func (r *CertReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cert, nil
}

// GetConfigForClient satisfies tls.Config.GetConfigForClient.
// Useful when you need to return a per-client tls.Config (e.g. mTLS).
func (r *CertReloader) GetConfigForClient(hello *tls.ClientHelloInfo) (*tls.Config, error) {
	r.mu.RLock()
	cert := r.cert
	r.mu.RUnlock()

	return &tls.Config{
		Certificates: []tls.Certificate{*cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// Reload forces an immediate reload from disk regardless of mtime.
// Intended to be called from a SIGHUP handler.
func (r *CertReloader) Reload() error {
	return r.load()
}

// Stop terminates the background poll goroutine.
func (r *CertReloader) Stop() {
	close(r.stopCh)
}

// CertInfo returns metadata about the currently loaded certificate for
// monitoring / logging purposes.
func (r *CertReloader) CertInfo() CertInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info := CertInfo{CertFile: r.certFile, KeyFile: r.keyFile}
	if r.cert != nil && len(r.cert.Certificate) > 0 {
		if parsed, err := x509.ParseCertificate(r.cert.Certificate[0]); err == nil {
			info.Subject    = parsed.Subject.CommonName
			info.NotBefore  = parsed.NotBefore
			info.NotAfter   = parsed.NotAfter
			info.DNSNames   = parsed.DNSNames
			info.Issuer     = parsed.Issuer.CommonName
		}
	}
	return info
}

// CertInfo carries human-readable certificate metadata.
type CertInfo struct {
	CertFile  string
	KeyFile   string
	Subject   string
	Issuer    string
	NotBefore time.Time
	NotAfter  time.Time
	DNSNames  []string
}

// ─── private ──────────────────────────────────────────────────────────────────

// load reads the cert/key pair from disk and atomically replaces the in-memory
// certificate if either file has changed since the last load.
func (r *CertReloader) load() error {
	certStat, err := os.Stat(r.certFile)
	if err != nil {
		return fmt.Errorf("stat cert file %q: %w", r.certFile, err)
	}
	keyStat, err := os.Stat(r.keyFile)
	if err != nil {
		return fmt.Errorf("stat key file %q: %w", r.keyFile, err)
	}

	// Fast path: skip if nothing has changed (mtime check).
	r.mu.RLock()
	unchanged := certStat.ModTime().Equal(r.certModTime) &&
		keyStat.ModTime().Equal(r.keyModTime)
	r.mu.RUnlock()

	if unchanged {
		return nil
	}

	// Slow path: parse and validate the new certificate outside the lock.
	newCert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("load x509 key pair: %w", err)
	}

	// Parse leaf for logging.
	var expiry time.Time
	var subject string
	if len(newCert.Certificate) > 0 {
		if leaf, err := x509.ParseCertificate(newCert.Certificate[0]); err == nil {
			newCert.Leaf = leaf // cache parsed leaf
			expiry = leaf.NotAfter
			subject = leaf.Subject.CommonName
		}
	}

	// Atomic swap.
	r.mu.Lock()
	r.cert = &newCert
	r.certModTime = certStat.ModTime()
	r.keyModTime = keyStat.ModTime()
	r.mu.Unlock()

	r.logger.Info("TLS certificate reloaded",
		"cert_file", r.certFile,
		"subject", subject,
		"expires", expiry.Format(time.RFC3339),
		"expires_in", time.Until(expiry).Round(time.Hour).String(),
	)
	return nil
}

// pollLoop checks the cert files for changes at the given interval.
func (r *CertReloader) pollLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := r.load(); err != nil {
				// Log but keep serving the old certificate — better than crashing.
				r.logger.Error("TLS certificate reload failed (keeping current cert)",
					"cert_file", r.certFile,
					"err", err,
				)
			}
		case <-r.stopCh:
			return
		}
	}
}
