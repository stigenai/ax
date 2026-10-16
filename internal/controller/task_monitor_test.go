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

package controller

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// startReadyzServer runs an HTTP server whose only route, /readyz, always
// answers with statusCode and body. It returns the server's address, in the
// workerIP shape probeReadyz expects. The server stops when the test ends.
func startReadyzServer(t *testing.T, statusCode int, body string) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(body))
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(lis)
	t.Cleanup(func() { _ = srv.Close() })
	return lis.Addr().String()
}

func TestClassifyActorLoss(t *testing.T) {
	crashed := func(msg string) *ateapipb.Actor {
		return &ateapipb.Actor{
			Status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_CRASHED,
				Crash: &ateapipb.ActorCrash{Message: msg},
			},
		}
	}
	running := &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING}}

	cases := []struct {
		name       string
		actor      *ateapipb.Actor
		err        error
		wantLost   bool
		wantReason string
	}{
		{
			name:       "a NotFound error means Substrate no longer knows the actor",
			err:        status.Error(codes.NotFound, "actor not found"),
			wantLost:   true,
			wantReason: "ActorMissing",
		},
		{
			name:       "a crash message naming OOM is OOMKilled",
			actor:      crashed("sandbox killed: Out of Memory"),
			wantLost:   true,
			wantReason: "OOMKilled",
		},
		{
			name:       "a crash message naming oom as a whole word is OOMKilled",
			actor:      crashed("container exited: oom-kill"),
			wantLost:   true,
			wantReason: "OOMKilled",
		},
		{
			name:       "a crash message that does not name OOM is ActorCrashed",
			actor:      crashed("worker pod evicted"),
			wantLost:   true,
			wantReason: "ActorCrashed",
		},
		{
			name:     "a crash message naming a word that merely contains oom is not OOMKilled",
			actor:    crashed("workroom unavailable"),
			wantLost: true,
			// "workroom" contains "oom" but not as a whole word, so this must
			// not match oomPattern.
			wantReason: "ActorCrashed",
		},
		{
			name:     "a RUNNING actor is not lost",
			actor:    running,
			wantLost: false,
		},
		{
			name:     "a transient non-NotFound error is not lost",
			err:      status.Error(codes.Unavailable, "substrate unreachable"),
			wantLost: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			reason, message, lost := classifyActorLoss("task1", tt.actor, tt.err)
			if lost != tt.wantLost {
				t.Fatalf("classifyActorLoss() lost = %v, want %v", lost, tt.wantLost)
			}
			if !tt.wantLost {
				return
			}
			if reason != tt.wantReason {
				t.Fatalf("classifyActorLoss() reason = %q, want %q", reason, tt.wantReason)
			}
			if message == "" {
				t.Fatalf("classifyActorLoss() message is empty, want the crash or error text")
			}
		})
	}
}

// TestClassifyActorLossTruncatesMessage proves the 512-byte bound: an
// oversized crash message must not reach the task's status whole.
func TestClassifyActorLossTruncatesMessage(t *testing.T) {
	huge := strings.Repeat("a", maxCrashMessageBytes*2)
	actor := &ateapipb.Actor{
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			Crash: &ateapipb.ActorCrash{Message: huge},
		},
	}
	_, message, lost := classifyActorLoss("task1", actor, nil)
	if !lost {
		t.Fatal("want lost=true for a crashed actor")
	}
	if len(message) != maxCrashMessageBytes {
		t.Fatalf("message length = %d, want %d", len(message), maxCrashMessageBytes)
	}
}

// TestTruncateMessageCutsOnRuneBoundary covers ax-9qu.12: a naive byte slice
// at maxCrashMessageBytes can land inside a multi-byte UTF-8 rune.
// truncateMessage must back up to a whole rune instead, so the result stays
// valid UTF-8. An invalid result fails protojson's write of the Condition
// (reproduced: "Condition.message contains invalid UTF-8"), which would
// leave the task stuck Running instead of moving to Failed.
func TestTruncateMessageCutsOnRuneBoundary(t *testing.T) {
	// "世" is a 3-byte rune. maxCrashMessageBytes (512) is not a multiple of
	// 3, so a naive msg[:512] on a long run of this rune cuts mid-rune.
	huge := strings.Repeat("世", maxCrashMessageBytes)
	got := truncateMessage(huge)
	if !utf8.ValidString(got) {
		t.Fatalf("truncateMessage returned invalid UTF-8: %q", got)
	}
	if len(got) > maxCrashMessageBytes {
		t.Fatalf("truncateMessage length = %d, want <= %d", len(got), maxCrashMessageBytes)
	}
	if len(got) == 0 {
		t.Fatal("truncateMessage returned empty for a huge input")
	}
}

func TestProbeReadyz(t *testing.T) {
	r := &TaskReconciler{httpClient: &http.Client{Timeout: 2 * time.Second}}

	t.Run("a 200 is alive under either predicate", func(t *testing.T) {
		lis := startReadyzServer(t, http.StatusOK, "ok")
		_, ok := r.probeReadyz(context.Background(), lis, "default", "actor", readyzStatusOK)
		if !ok {
			t.Fatal("want ok=true for a 200 response")
		}
		_, ok = r.probeReadyz(context.Background(), lis, "default", "actor", readyzAlive)
		if !ok {
			t.Fatal("want ok=true for a 200 response under readyzAlive")
		}
	})

	t.Run("a 503 workspace initializing is alive only under readyzAlive", func(t *testing.T) {
		lis := startReadyzServer(t, http.StatusServiceUnavailable, "workspace initializing")
		_, ok := r.probeReadyz(context.Background(), lis, "default", "actor", readyzStatusOK)
		if ok {
			t.Fatal("want ok=false for a 503 under readyzStatusOK")
		}
		_, ok = r.probeReadyz(context.Background(), lis, "default", "actor", readyzAlive)
		if !ok {
			t.Fatal("want ok=true for a 503 workspace-initializing response under readyzAlive")
		}
	})

	t.Run("a 503 with another body is dead under readyzAlive", func(t *testing.T) {
		lis := startReadyzServer(t, http.StatusServiceUnavailable, "some other failure")
		_, ok := r.probeReadyz(context.Background(), lis, "default", "actor", readyzAlive)
		if ok {
			t.Fatal("want ok=false for a 503 that does not name workspace setup")
		}
	})

	t.Run("a closed connection is dead", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := lis.Addr().String()
		lis.Close()
		_, ok := r.probeReadyz(context.Background(), addr, "default", "actor", readyzAlive)
		if ok {
			t.Fatal("want ok=false when the runner is unreachable")
		}
	})
}
