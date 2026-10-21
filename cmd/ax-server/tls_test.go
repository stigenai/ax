package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeCert(t *testing.T, dir, cn string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile = filepath.Join(dir, "tls.crt")
	keyFile = filepath.Join(dir, "tls.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	trust, err := os.OpenFile(filepath.Join(dir, "trust.pem"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer trust.Close()
	if _, err := trust.Write(certPEM); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// servedCN dials the server, trusting every cert written in dir, and returns
// the CN of the certificate it sends.
func servedCN(t *testing.T, addr, dir string) string {
	t.Helper()
	pemBytes, err := os.ReadFile(filepath.Join(dir, "trust.pem"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pemBytes)
	conn, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool, ServerName: "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return conn.ConnectionState().PeerCertificates[0].Subject.CommonName
}

func TestNewTLSConfigOff(t *testing.T) {
	cfg, err := newTLSConfig("", "")
	if err != nil || cfg != nil {
		t.Fatalf("got cfg=%v err=%v, want nil, nil", cfg, err)
	}
}

func TestNewTLSConfigOneFlagOnly(t *testing.T) {
	certFile, keyFile := writeCert(t, t.TempDir(), "a")
	if _, err := newTLSConfig(certFile, ""); err == nil {
		t.Error("cert without key: want error")
	}
	if _, err := newTLSConfig("", keyFile); err == nil {
		t.Error("key without cert: want error")
	}
}

func TestNewTLSConfigBadFiles(t *testing.T) {
	if _, err := newTLSConfig("/nonexistent.crt", "/nonexistent.key"); err == nil {
		t.Error("missing files: want error")
	}
}

func TestNewTLSConfigServesAndReloads(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeCert(t, dir, "first")
	cfg, err := newTLSConfig(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_ = c.(*tls.Conn).Handshake()
			}()
		}
	}()
	addr := ln.Addr().String()
	if got := servedCN(t, addr, dir); got != "first" {
		t.Fatalf("CN = %q, want first", got)
	}

	// Renew: replace both files, with a later mtime.
	writeCert(t, dir, "second")
	later := time.Now().Add(time.Minute)
	for _, f := range []string{certFile, keyFile} {
		if err := os.Chtimes(f, later, later); err != nil {
			t.Fatal(err)
		}
	}
	if got := servedCN(t, addr, dir); got != "second" {
		t.Fatalf("after renewal CN = %q, want second", got)
	}

	// A broken renewal keeps the last good certificate.
	if err := os.WriteFile(certFile, []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := servedCN(t, addr, dir); got != "second" {
		t.Fatalf("after bad renewal CN = %q, want second", got)
	}
}
