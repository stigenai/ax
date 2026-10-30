// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const testAuthToken = "0123456789abcdef0123456789abcdef"

func portOf(t *testing.T, a net.Addr) string {
	t.Helper()
	_, p, err := net.SplitHostPort(a.String())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestAuthTokenSent: the server has auth on, so a call succeeds only if the
// CLI sends "Bearer <token>" over https and over each loopback form.
func TestAuthTokenSent(t *testing.T) {
	t.Setenv("AX_AUTH_TOKEN", testAuthToken)
	srv := server.NewServer(memory.NewStore())
	srv.AuthToken = testAuthToken

	ts := httptest.NewUnstartedServer(http.HandlerFunc(srv.GRPCServer().ServeHTTP))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ts.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AX_CA_FILE", ca)
	if err := listTasks(t, "https://127.0.0.1:"+portOf(t, ts.Listener.Addr())); err != nil {
		t.Fatalf("https: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := srv.GRPCServer()
	t.Cleanup(g.Stop)
	go func() { _ = g.Serve(lis) }()
	port := portOf(t, lis.Addr())
	for _, a := range []string{"http://127.0.0.1:" + port, "http://localhost:" + port, "127.0.0.1:" + port} {
		if err := listTasks(t, a); err != nil {
			t.Fatalf("%s: %v", a, err)
		}
	}
}

// TestAuthTokenRefusedPlaintext: the CLI must not put the token on a
// plaintext link to a non-loopback host.
func TestAuthTokenRefusedPlaintext(t *testing.T) {
	t.Setenv("AX_AUTH_TOKEN", testAuthToken)
	for _, a := range []string{"http://ax.example:8080", "10.0.0.1:8080", "http://10.0.0.1:8080", "ax.example:8080"} {
		_, conn, err := getAXClient(a)
		if err == nil {
			conn.Close()
			t.Fatalf("%s: want an error, got a client", a)
		}
	}
}

// TestAuthTokenUnset: without AX_AUTH_TOKEN the CLI sends no authorization.
func TestAuthTokenUnset(t *testing.T) {
	t.Setenv("AX_AUTH_TOKEN", "")
	got := make(chan []string, 1)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		got <- md.Get("authorization")
		return h(ctx, req)
	}))
	v1alpha1.RegisterAXServer(g, &v1alpha1.UnimplementedAXServer{})
	t.Cleanup(g.Stop)
	go func() { _ = g.Serve(lis) }()
	_ = listTasks(t, lis.Addr().String())
	if h := <-got; len(h) != 0 {
		t.Fatalf("authorization sent without AX_AUTH_TOKEN: %v", h)
	}
}
