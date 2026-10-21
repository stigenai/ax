package main

import (
	"context"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func listTasks(t *testing.T, addr string) error {
	t.Helper()
	client, conn, err := getAXClient(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = client.ListTasks(ctx, &v1alpha1.ListTasksRequest{Atespace: "default"})
	return err
}

func TestGetAXClientTLS(t *testing.T) {
	srv := server.NewServer(memory.NewStore()).GRPCServer()
	ts := httptest.NewUnstartedServer(http.HandlerFunc(srv.ServeHTTP))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ts.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := listTasks(t, "https://"+ts.Listener.Addr().String()); err == nil {
		t.Fatal("https without AX_CA_FILE must not trust the test CA")
	}
	t.Setenv("AX_CA_FILE", ca)
	if err := listTasks(t, "https://"+ts.Listener.Addr().String()); err != nil {
		t.Fatalf("https with AX_CA_FILE: %v", err)
	}
}

func TestGetAXClientPlaintext(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server.NewServer(memory.NewStore()).GRPCServer()
	t.Cleanup(srv.Stop)
	go func() { _ = srv.Serve(lis) }()
	for _, a := range []string{lis.Addr().String(), "http://" + lis.Addr().String()} {
		if err := listTasks(t, a); err != nil {
			t.Fatalf("%s: %v", a, err)
		}
	}
}
