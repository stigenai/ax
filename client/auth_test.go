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

package client

import (
	"context"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const testAuthToken = "test-auth-token-for-unit-tests-only"

// recordingServer returns a gRPC server that sends the authorization values of
// each call on the returned channel.
func recordingServer() (*grpc.Server, <-chan []string) {
	got := make(chan []string, 1)
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		got <- md.Get("authorization")
		return next(ctx, req)
	}))
	v1alpha1.RegisterAXServer(srv, v1alpha1.UnimplementedAXServer{})
	return srv, got
}

// callWith dials serverURL with token and makes one call. The call itself may
// fail (the stub server is unimplemented); only the Dial error is returned.
func callWith(t *testing.T, serverURL, token string) error {
	t.Helper()
	client, conn, err := Dial(context.Background(), Options{ServerURL: serverURL, AuthToken: token})
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = client.GetModel(ctx, &v1alpha1.GetModelRequest{})
	return nil
}

func TestDialAuthTokenSentOverTLSAndLoopback(t *testing.T) {
	isolate(t)
	want := []string{"Bearer " + testAuthToken}

	srv, got := recordingServer()
	ts := httptest.NewUnstartedServer(http.HandlerFunc(srv.ServeHTTP))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ts.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AX_CA_FILE", ca)
	if err := callWith(t, "https://"+ts.Listener.Addr().String(), testAuthToken); err != nil {
		t.Fatalf("https: %v", err)
	}
	if h := <-got; !reflect.DeepEqual(h, want) {
		t.Errorf("https: authorization = %v, want %v", h, want)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	plain, plainGot := recordingServer()
	t.Cleanup(plain.Stop)
	go func() { _ = plain.Serve(lis) }()
	_, port, _ := net.SplitHostPort(lis.Addr().String())
	for _, addr := range []string{"http://127.0.0.1:" + port, "http://localhost:" + port, "127.0.0.1:" + port} {
		if err := callWith(t, addr, testAuthToken); err != nil {
			t.Fatalf("%s: %v", addr, err)
		}
		if h := <-plainGot; !reflect.DeepEqual(h, want) {
			t.Errorf("%s: authorization = %v, want %v", addr, h, want)
		}
	}
}

func TestDialAuthTokenRefusedOverRemotePlaintext(t *testing.T) {
	isolate(t)
	for _, addr := range []string{"http://ax.example:8080", "10.0.0.1:8080", "http://10.0.0.1:8080", "ax.example:8080"} {
		if err := callWith(t, addr, testAuthToken); err == nil {
			t.Errorf("%s: Dial() error = nil, want an error", addr)
		}
	}
}

func TestDialWithoutAuthTokenSendsNothing(t *testing.T) {
	isolate(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv, got := recordingServer()
	t.Cleanup(srv.Stop)
	go func() { _ = srv.Serve(lis) }()
	if err := callWith(t, lis.Addr().String(), ""); err != nil {
		t.Fatal(err)
	}
	if h := <-got; len(h) != 0 {
		t.Errorf("authorization = %v, want none", h)
	}
}
