package guest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func writeCA(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ca.pem")
	b := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ts.Certificate().Raw})
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func writeUnrelatedCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "unrelated"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "other.pem")
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func newHealthGRPC() *grpc.Server {
	s := grpc.NewServer()
	healthpb.RegisterHealthServer(s, health.NewServer())
	return s
}

func startTLSServer(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewUnstartedServer(http.HandlerFunc(newHealthGRPC().ServeHTTP))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)
	return ts
}

func startPlainServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := newHealthGRPC()
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func check(t *testing.T, addr string) error {
	t.Helper()
	target, creds, err := TransportCredentials(addr)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

func TestTLSWithCAFile(t *testing.T) {
	ts := startTLSServer(t)
	t.Setenv("AX_CA_FILE", writeCA(t, ts))
	if err := check(t, "https://"+ts.Listener.Addr().String()); err != nil {
		t.Fatalf("https with trusted CA: %v", err)
	}
}

func TestTLSRejectsUntrustedCert(t *testing.T) {
	ts := startTLSServer(t)
	t.Setenv("AX_CA_FILE", writeUnrelatedCA(t))
	err := check(t, "https://"+ts.Listener.Addr().String())
	if err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("want certificate error, got %v", err)
	}
}

func TestTLSToPlaintextServerFails(t *testing.T) {
	addr := startPlainServer(t)
	if err := check(t, "https://"+addr); err == nil {
		t.Fatal("https to a plaintext server must fail")
	}
}

func TestPlaintextAddresses(t *testing.T) {
	addr := startPlainServer(t)
	for _, a := range []string{addr, "http://" + addr} {
		if err := check(t, a); err != nil {
			t.Fatalf("%s: %v", a, err)
		}
	}
}

func TestBadCAFile(t *testing.T) {
	notPEM := filepath.Join(t.TempDir(), "x.pem")
	if err := os.WriteFile(notPEM, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{filepath.Join(t.TempDir(), "missing.pem"), notPEM} {
		t.Setenv("AX_CA_FILE", f)
		_, _, err := TransportCredentials("https://h.example")
		if err == nil || !strings.Contains(err.Error(), f) {
			t.Fatalf("want error naming %s, got %v", f, err)
		}
	}
}

func TestTransportCredentialsTable(t *testing.T) {
	tests := []struct {
		addr       string
		target     string
		tls        bool
		serverName string
	}{
		{"https://h.example", "h.example:443", true, "h.example"},
		{"https://h.example:8443", "h.example:8443", true, "h.example"},
		{"h.example:443", "h.example:443", true, "h.example"},
		{"h.example:80", "h.example:80", false, ""},
		{"http://h.example:443", "h.example:443", false, ""},
		{"http://h.example:80", "h.example:80", false, ""},
		{"127.0.0.1:0", "127.0.0.1:0", false, ""},
		{"dns:///h.example", "dns:///h.example", false, ""},
	}
	for _, tt := range tests {
		target, creds, err := TransportCredentials(tt.addr)
		if err != nil {
			t.Fatalf("%s: %v", tt.addr, err)
		}
		info := creds.Info()
		if target != tt.target || (info.SecurityProtocol == "tls") != tt.tls || info.ServerName != tt.serverName {
			t.Errorf("%s: got %q %q %q, want %q tls=%v %q", tt.addr, target, info.SecurityProtocol, info.ServerName, tt.target, tt.tls, tt.serverName)
		}
	}
}
