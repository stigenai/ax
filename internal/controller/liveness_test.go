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

package controller_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// livenessRouter is a fake atenet-router. Each probe carries the header
// ate-target-actor: <atespace>/<actor>; set() picks that exact value's
// reply mode for the next probes, until changed again.
type livenessRouter struct {
	mu    sync.Mutex
	mode  map[string]string // "ok" | "stale" | "stale-no-header" | "502" | "hang"; default "ok"
	calls int
	srv   *httptest.Server
}

func newLivenessRouter(t *testing.T) *livenessRouter {
	t.Helper()
	r := &livenessRouter{mode: map[string]string{}}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		actor := req.Header.Get("ate-target-actor")
		r.mu.Lock()
		r.calls++
		mode := r.mode[actor]
		r.mu.Unlock()
		switch mode {
		case "stale":
			w.Header().Set("X-Ate-Assignment-Stale", "true")
			w.WriteHeader(http.StatusMisdirectedRequest)
		case "stale-no-header":
			w.WriteHeader(http.StatusMisdirectedRequest)
		case "502":
			w.WriteHeader(http.StatusBadGateway)
		case "hang":
			<-req.Context().Done()
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *livenessRouter) set(atespace, actor, mode string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mode[atespace+"/"+actor] = mode
}

func (r *livenessRouter) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *livenessRouter) addr() string {
	return strings.TrimPrefix(r.srv.URL, "http://")
}

// newLivenessSubstrateClient serves mockSrv over gRPC and returns a client
// bound to it, the same pattern newTestReconciler uses.
func newLivenessSubstrateClient(t *testing.T, mockSrv *mockControlServer) *substrate.Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func newLivenessChecker(t *testing.T, s store.Store, mockSrv *mockControlServer, router *livenessRouter) *controller.LivenessChecker {
	t.Helper()
	client := newLivenessSubstrateClient(t, mockSrv)
	checker := controller.NewLivenessChecker(s, client, router.addr())
	checker.HTTPClient = &http.Client{Timeout: 500 * time.Millisecond}
	return checker
}

func runningTaskWithActor(atespace, name, actor, workerIP string) *v1alpha1.Task {
	return &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: atespace},
		Status:   &v1alpha1.TaskStatus{Phase: "Running", Actor: actor, WorkerIp: workerIP},
	}
}

func readyReason(task *v1alpha1.Task) string {
	for _, c := range task.GetStatus().GetConditions() {
		if c.GetType() == "Ready" {
			return c.GetReason()
		}
	}
	return ""
}

// TestLivenessSweepFailsTaskAtThresholdNotBefore covers AC4: a Running task
// whose actor Substrate still reports RUNNING on the same worker that just
// answered 421 stale fails only once the sweep sees that threshold times in
// a row, never before.
func TestLivenessSweepFailsTaskAtThresholdNotBefore(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := runningTaskWithActor("default", "task1", "task1", "10.244.3.47")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	mockSrv := &mockControlServer{existingActor: &ateapipb.Actor{
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.244.3.47"},
		},
	}}
	router := newLivenessRouter(t)
	router.set("default", "task1", "stale")
	checker := newLivenessChecker(t, s, mockSrv, router)

	for i := 1; i < controller.DefaultLivenessThreshold; i++ {
		checker.SweepOnce(ctx)
		got, err := s.GetTask(ctx, "default", "task1")
		if err != nil {
			t.Fatal(err)
		}
		if got.GetStatus().GetPhase() != "Running" {
			t.Fatalf("sweep %d: phase is %q, want Running (threshold is %d)", i, got.GetStatus().GetPhase(), controller.DefaultLivenessThreshold)
		}
	}

	checker.SweepOnce(ctx)
	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Failed" {
		t.Fatalf("phase is %q, want Failed at the threshold sweep", got.GetStatus().GetPhase())
	}
	if got.GetStatus().GetWorkerIp() != "" {
		t.Fatalf("WorkerIp is %q, want cleared", got.GetStatus().GetWorkerIp())
	}
	if reason := readyReason(got); reason != "ActorLost" {
		t.Fatalf("Ready reason is %q, want ActorLost", reason)
	}
}

// TestLivenessSweepActorMissingGetsActorMissingReason covers AC4's NotFound
// branch: a stale probe plus a Substrate GetActor NotFound fails the task
// with reason ActorMissing, not ActorLost.
func TestLivenessSweepActorMissingGetsActorMissingReason(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := runningTaskWithActor("default", "task1", "task1", "10.244.3.47")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	mockSrv := &mockControlServer{getActorErr: status.Error(codes.NotFound, "actor not found")}
	router := newLivenessRouter(t)
	router.set("default", "task1", "stale")
	checker := newLivenessChecker(t, s, mockSrv, router)

	for i := 0; i < controller.DefaultLivenessThreshold; i++ {
		checker.SweepOnce(ctx)
	}

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Failed" {
		t.Fatalf("phase is %q, want Failed", got.GetStatus().GetPhase())
	}
	if reason := readyReason(got); reason != "ActorMissing" {
		t.Fatalf("Ready reason is %q, want ActorMissing", reason)
	}
}

// TestLivenessSweepIgnoresInconclusiveResults covers AC5: a 421 without the
// stale header, a 502, and a probe that never answers (a timeout) never
// fail a task, no matter how many sweeps see them.
func TestLivenessSweepIgnoresInconclusiveResults(t *testing.T) {
	for _, mode := range []string{"stale-no-header", "502", "hang"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s := memory.NewStore()
			task := runningTaskWithActor("default", "task1", "task1", "10.244.3.47")
			if err := s.SaveTask(ctx, task); err != nil {
				t.Fatal(err)
			}

			mockSrv := &mockControlServer{existingActor: &ateapipb.Actor{
				Status: &ateapipb.ActorStatus{
					State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
					WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.244.3.47"},
				},
			}}
			router := newLivenessRouter(t)
			router.set("default", "task1", mode)
			checker := newLivenessChecker(t, s, mockSrv, router)

			for i := 0; i < controller.DefaultLivenessThreshold+2; i++ {
				checker.SweepOnce(ctx)
			}

			got, err := s.GetTask(ctx, "default", "task1")
			if err != nil {
				t.Fatal(err)
			}
			if got.GetStatus().GetPhase() != "Running" {
				t.Fatalf("mode %q: phase is %q after %d sweeps, want Running (never fails on an inconclusive result)",
					mode, got.GetStatus().GetPhase(), controller.DefaultLivenessThreshold+2)
			}
		})
	}
}

// TestLivenessSweepStaleOkStaleDoesNotFail covers AC5's reset case: a
// healthy probe in between two stale ones restarts the miss count, so the
// task never reaches the threshold.
func TestLivenessSweepStaleOkStaleDoesNotFail(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := runningTaskWithActor("default", "task1", "task1", "10.244.3.47")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	mockSrv := &mockControlServer{existingActor: &ateapipb.Actor{
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.244.3.47"},
		},
	}}
	router := newLivenessRouter(t)
	checker := newLivenessChecker(t, s, mockSrv, router)

	// stale, ok, stale, ok, ... never 2 stale results in a row for longer
	// than the threshold allows.
	for i := 0; i < controller.DefaultLivenessThreshold*2; i++ {
		if i%2 == 0 {
			router.set("default", "task1", "stale")
		} else {
			router.set("default", "task1", "ok")
		}
		checker.SweepOnce(ctx)
	}

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase is %q, want Running (a healthy probe must reset the miss count)", got.GetStatus().GetPhase())
	}
}

// TestLivenessSweepSkipsNonRunningTasks covers AC5's probe-count rule:
// Pending, Suspended, Terminating and Failed tasks get 0 probes.
func TestLivenessSweepSkipsNonRunningTasks(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	for _, phase := range []string{"Pending", "Suspended", "Terminating", "Failed"} {
		task := runningTaskWithActor("default", "task-"+phase, "task-"+phase, "10.244.3.47")
		task.Status.Phase = phase
		if err := s.SaveTask(ctx, task); err != nil {
			t.Fatal(err)
		}
	}

	mockSrv := &mockControlServer{}
	router := newLivenessRouter(t)
	for _, phase := range []string{"Pending", "Suspended", "Terminating", "Failed"} {
		router.set("default", "task-"+phase, "stale")
	}
	checker := newLivenessChecker(t, s, mockSrv, router)

	checker.SweepOnce(ctx)

	if n := router.callCount(); n != 0 {
		t.Fatalf("router got %d probes, want 0 for non-Running tasks", n)
	}
}

// TestLivenessSweepPrunesMissesOnDeleteAndRecreate covers ax-lox.8. A
// person deletes a task, then creates a new task with the same name. The
// new task must not inherit the old task's miss count. Without the fix,
// the new task fails after 1 more stale sweep, not after
// DefaultLivenessThreshold sweeps.
func TestLivenessSweepPrunesMissesOnDeleteAndRecreate(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := runningTaskWithActor("default", "task1", "task1", "10.244.3.47")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	mockSrv := &mockControlServer{existingActor: &ateapipb.Actor{
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.244.3.47"},
		},
	}}
	router := newLivenessRouter(t)
	router.set("default", "task1", "stale")
	checker := newLivenessChecker(t, s, mockSrv, router)

	// 2 stale sweeps: 1 short of DefaultLivenessThreshold (3).
	checker.SweepOnce(ctx)
	checker.SweepOnce(ctx)
	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase is %q after 2 misses, want Running", got.GetStatus().GetPhase())
	}

	// Delete the task, then re-create a same-name task fresh. A sweep with
	// nothing listed must drop the old task's miss count.
	if err := s.DeleteTask(ctx, "default", "task1"); err != nil {
		t.Fatal(err)
	}
	checker.SweepOnce(ctx)

	fresh := runningTaskWithActor("default", "task1", "task1", "10.244.3.47")
	if err := s.SaveTask(ctx, fresh); err != nil {
		t.Fatal(err)
	}

	// 1 stale sweep on the re-created task: it must stay Running. Before
	// the fix, it inherits the old task's 2 misses and fails on this sweep.
	checker.SweepOnce(ctx)
	got, err = s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase is %q after 1 miss on the re-created task, want Running: it must not inherit the deleted task's miss count", got.GetStatus().GetPhase())
	}
}

// raceStore wraps a store.Store and runs hook immediately before every
// UpdateTaskStatusIf call, so a test can simulate a concurrent write that
// lands between a liveness probe and its conditional write.
type raceStore struct {
	store.Store
	hook func()
}

func (s *raceStore) UpdateTaskStatusIf(ctx context.Context, atespace, name string, expect func(*v1alpha1.TaskStatus) bool, status *v1alpha1.TaskStatus) (bool, error) {
	if s.hook != nil {
		s.hook()
		s.hook = nil // fire once: only the write that reaches the threshold races
	}
	return s.Store.UpdateTaskStatusIf(ctx, atespace, name, expect, status)
}

// TestLivenessSweepNoWriteWhenTaskChangedBeforeWrite covers AC5's
// conditional-write case: a concurrent write (a fresh reconcile) changes
// the task's WorkerIp between this probe and the threshold write, so the
// write must not apply and the task must not fail on stale information.
func TestLivenessSweepNoWriteWhenTaskChangedBeforeWrite(t *testing.T) {
	ctx := context.Background()
	inner := memory.NewStore()
	task := runningTaskWithActor("default", "task1", "task1", "10.244.3.47")
	if err := inner.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	raced := &raceStore{Store: inner, hook: func() {
		if err := inner.UpdateTaskStatus(ctx, "default", "task1", &v1alpha1.TaskStatus{
			Phase: "Running", Actor: "task1", WorkerIp: "10.244.9.9",
		}); err != nil {
			t.Fatal(err)
		}
	}}

	mockSrv := &mockControlServer{existingActor: &ateapipb.Actor{
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.244.3.47"},
		},
	}}
	router := newLivenessRouter(t)
	router.set("default", "task1", "stale")
	checker := newLivenessChecker(t, raced, mockSrv, router)

	for i := 0; i < controller.DefaultLivenessThreshold; i++ {
		checker.SweepOnce(ctx)
	}

	got, err := inner.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase is %q, want Running: the conditional write must not apply once WorkerIp changed underneath it", got.GetStatus().GetPhase())
	}
	if got.GetStatus().GetWorkerIp() != "10.244.9.9" {
		t.Fatalf("WorkerIp is %q, want the concurrent write's %q preserved", got.GetStatus().GetWorkerIp(), "10.244.9.9")
	}
}
