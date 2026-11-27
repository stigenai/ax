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

package guest_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/ax/internal/guest"
)

// TestExecMisdirectedRequest runs Exec against a router that answers every
// request with HTTP 421, as atenet-router does for an actor its worker no
// longer holds. The error must name the failed start, so a caller can retry,
// and carry the text that cmd/ax matches on.
func TestExecMisdirectedRequest(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	router := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusMisdirectedRequest)
	})}
	router.Protocols = new(http.Protocols)
	router.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = router.Serve(lis) }()
	t.Cleanup(func() { _ = router.Close() })

	client, err := guest.Dial(lis.Addr().String(), "ns/actor")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Exec(ctx, guest.ExecOptions{Command: []string{"true"}})
	if !errors.Is(err, guest.ErrStartProcess) {
		t.Fatalf("Exec() error = %v, want guest.ErrStartProcess", err)
	}
	if !strings.Contains(err.Error(), "421 (Misdirected Request)") {
		t.Errorf("Exec() error = %q, want it to contain %q", err, "421 (Misdirected Request)")
	}
}
