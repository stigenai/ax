package main

import (
	"crypto/tls"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"
)

// certReloader serves a certificate from two files. It re-reads them when
// either file changes, so a cert-manager renewal needs no restart.
type certReloader struct {
	certFile, keyFile string

	mu      sync.Mutex
	cert    *tls.Certificate
	certMod time.Time
	keyMod  time.Time
}

// newTLSConfig returns nil (plaintext) when both files are empty. It returns
// an error when only one is set or the first load fails.
func newTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	if certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, errors.New("--tls-cert-file and --tls-key-file must be set together")
	}
	r := &certReloader{certFile: certFile, keyFile: keyFile}
	if _, err := r.get(nil); err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: r.get,
		NextProtos:     []string{"h2", "http/1.1"},
	}, nil
}

func (r *certReloader) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cs, cerr := os.Stat(r.certFile)
	ks, kerr := os.Stat(r.keyFile)
	if cerr == nil && kerr == nil && r.cert != nil && cs.ModTime().Equal(r.certMod) && ks.ModTime().Equal(r.keyMod) {
		return r.cert, nil
	}
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		if r.cert != nil {
			slog.Error("reload TLS certificate failed; keeping the old one", "error", err)
			return r.cert, nil
		}
		return nil, err
	}
	r.cert = &cert
	if cerr == nil && kerr == nil {
		r.certMod, r.keyMod = cs.ModTime(), ks.ModTime()
	}
	return r.cert, nil
}
