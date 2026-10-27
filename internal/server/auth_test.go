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
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const authToken = "0123456789abcdef0123456789abcdef"

func startAuthServer(t *testing.T, token string) v1alpha1.AXClient {
	t.Helper()
	srv := server.NewServer(memory.NewStore())
	srv.AuthToken = token
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
	return v1alpha1.NewAXClient(conn)
}

// withAuth adds one authorization metadata value for each argument.
func withAuth(ctx context.Context, values ...string) context.Context {
	for _, v := range values {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", v)
	}
	return ctx
}

func TestAuthUnary(t *testing.T) {
	client := startAuthServer(t, authToken)
	cases := []struct {
		name   string
		header []string
		ok     bool
	}{
		{"valid", []string{"Bearer " + authToken}, true},
		{"none", nil, false},
		{"wrong", []string{"Bearer wrong"}, false},
		{"suffix", []string{"Bearer " + authToken + "x"}, false},
		{"no scheme", []string{authToken}, false},
		{"lowercase scheme", []string{"bearer " + authToken}, false},
		{"two spaces", []string{"Bearer  " + authToken}, false},
		{"two values", []string{"Bearer " + authToken, "Bearer wrong"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(withAuth(context.Background(), tc.header...), 5*time.Second)
			defer cancel()
			_, err := client.ListTasks(ctx, &v1alpha1.ListTasksRequest{})
			if tc.ok {
				if err != nil {
					t.Fatalf("want success, got %v", err)
				}
				return
			}
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("want Unauthenticated, got %v", err)
			}
		})
	}
}

func TestAuthStream(t *testing.T) {
	client := startAuthServer(t, authToken)
	for _, tc := range []struct {
		name   string
		header []string
		ok     bool
	}{
		{"none", nil, false},
		{"wrong", []string{"Bearer wrong"}, false},
		{"valid", []string{"Bearer " + authToken}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(withAuth(context.Background(), tc.header...), 2*time.Second)
			defer cancel()
			stream, err := client.WatchTask(ctx, &v1alpha1.WatchTaskRequest{Name: "missing"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = stream.Recv()
			if tc.ok {
				// An open stream waits for events; the deadline ends it.
				if status.Code(err) == codes.Unauthenticated {
					t.Fatalf("valid header rejected: %v", err)
				}
				return
			}
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("want Unauthenticated, got %v", err)
			}
		})
	}
}

func TestAuthHealthzOpen(t *testing.T) {
	srv := server.NewServer(memory.NewStore())
	srv.AuthToken = authToken
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", rec.Code)
	}
}

func TestAuthOff(t *testing.T) {
	client := startAuthServer(t, "")
	for _, header := range [][]string{nil, {"Bearer anything"}} {
		ctx, cancel := context.WithTimeout(withAuth(context.Background(), header...), 5*time.Second)
		_, err := client.ListTasks(ctx, &v1alpha1.ListTasksRequest{})
		cancel()
		if err != nil {
			t.Fatalf("auth off, header %v: %v", header, err)
		}
	}
}
