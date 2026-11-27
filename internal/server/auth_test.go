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

package server_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const testAuthToken = "test-auth-token-for-unit-tests-only"

// startAuthServer serves the AX API, plus the gRPC health service for its
// streaming Watch RPC, which the AX API does not have.
func startAuthServer(t *testing.T, token string) *grpc.ClientConn {
	t.Helper()
	srv := server.NewServer(memory.NewStore(), newTestTaskManager(substratetest.NewServer(t)), server.Options{AuthToken: token})
	healthpb.RegisterHealthServer(srv.GRPCServer(), health.NewServer())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: srv.Handler()}
	hs.Protocols = new(http.Protocols)
	hs.Protocols.SetHTTP1(true)
	hs.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = hs.Serve(ln) }()
	t.Cleanup(func() { _ = hs.Close() })
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// withAuth adds one authorization metadata value for each argument.
func withAuth(ctx context.Context, values ...string) context.Context {
	for _, v := range values {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", v)
	}
	return ctx
}

var authCases = []struct {
	name   string
	header []string
	ok     bool
}{
	{"valid", []string{"Bearer " + testAuthToken}, true},
	{"none", nil, false},
	{"wrong", []string{"Bearer wrong"}, false},
	{"suffix", []string{"Bearer " + testAuthToken + "x"}, false},
	{"no scheme", []string{testAuthToken}, false},
	{"lowercase scheme", []string{"bearer " + testAuthToken}, false},
	{"two spaces", []string{"Bearer  " + testAuthToken}, false},
	{"two values", []string{"Bearer " + testAuthToken, "Bearer wrong"}, false},
}

func TestAuthUnary(t *testing.T) {
	client := v1alpha1.NewAXClient(startAuthServer(t, testAuthToken))
	for _, tc := range authCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(withAuth(context.Background(), tc.header...), 5*time.Second)
			defer cancel()
			_, err := client.ListTasks(ctx, &v1alpha1.ListTasksRequest{})
			if tc.ok != (err == nil) || (!tc.ok && status.Code(err) != codes.Unauthenticated) {
				t.Fatalf("ListTasks() error = %v, want success %v, or Unauthenticated", err, tc.ok)
			}
		})
	}
}

func TestAuthStream(t *testing.T) {
	client := healthpb.NewHealthClient(startAuthServer(t, testAuthToken))
	for _, tc := range authCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(withAuth(context.Background(), tc.header...), 5*time.Second)
			defer cancel()
			stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = stream.Recv()
			if tc.ok != (err == nil) || (!tc.ok && status.Code(err) != codes.Unauthenticated) {
				t.Fatalf("Recv() error = %v, want success %v, or Unauthenticated", err, tc.ok)
			}
		})
	}
}

func TestAuthHealthzOpen(t *testing.T) {
	srv := server.NewServer(memory.NewStore(), newTestTaskManager(substratetest.NewServer(t)), server.Options{AuthToken: testAuthToken})
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz status = %d, want 200", rec.Code)
	}
}

func TestAuthOff(t *testing.T) {
	client := v1alpha1.NewAXClient(startAuthServer(t, ""))
	for _, header := range [][]string{nil, {"Bearer anything"}} {
		ctx, cancel := context.WithTimeout(withAuth(context.Background(), header...), 5*time.Second)
		_, err := client.ListTasks(ctx, &v1alpha1.ListTasksRequest{})
		cancel()
		if err != nil {
			t.Fatalf("auth off, header %v: %v", header, err)
		}
	}
}
