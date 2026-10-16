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

// newTestReconcilerForServer serves srv over gRPC and returns a reconciler
// bound to it, like newTestReconciler, but accepting any ControlServer so a
// test can wrap mockControlServer to override one method (see
// raceGetActorServer below).
func newTestReconcilerForServer(t *testing.T, srv ateapipb.ControlServer) *controller.TaskReconciler {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond
	return reconciler
}

// deadRunnerAddr returns an address with nothing listening, so a probe
// against it always fails with a connection error.
func deadRunnerAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

// taskWithWorker is runningTaskWithActor plus an Id, so a test can check
// failTask's status.id guard.
func taskWithWorker(atespace, name, actor, workerIP, id string) *v1alpha1.Task {
	task := runningTaskWithActor(atespace, name, actor, workerIP)
	task.Status.Id = id
	return task
}

// sandboxRouter is a fake atenet-router for TaskMonitor's router-only
// /readyz probe (probeSandboxViaRouter, ax-9qu.10). A test picks the reply
// for one atespace/actor key with set(); an unset key answers "dead" (502),
// as a sandbox with no working route would. callsFor records every
// ate-target-actor header value the router saw, in order, so a test can
// prove the probe used the header instead of a direct dial.
type sandboxRouter struct {
	mu    sync.Mutex
	mode  map[string]string
	calls []string
	srv   *httptest.Server
}

func newSandboxRouter(t *testing.T) *sandboxRouter {
	t.Helper()
	r := &sandboxRouter{mode: map[string]string{}}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		actor := req.Header.Get("ate-target-actor")
		r.mu.Lock()
		r.calls = append(r.calls, actor)
		mode := r.mode[actor]
		r.mu.Unlock()
		switch mode {
		case "alive":
			w.WriteHeader(http.StatusOK)
		case "initializing":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("workspace initializing"))
		case "stale":
			w.Header().Set("X-Ate-Assignment-Stale", "true")
			w.WriteHeader(http.StatusMisdirectedRequest)
		default: // "dead" or unset: a sandbox that never answers.
			w.WriteHeader(http.StatusBadGateway)
		}
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *sandboxRouter) set(atespace, actor, mode string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mode[atespace+"/"+actor] = mode
}

func (r *sandboxRouter) headerCalls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	return out
}

func (r *sandboxRouter) addr() string {
	return strings.TrimPrefix(r.srv.URL, "http://")
}

// setRouterAddr points monitor.RouterAddr at router. main.go sets this
// field the same way, once, from ATENET_ROUTER_ADDR (ax-9qu.16). A test
// sets the field directly instead of the environment variable.
func setRouterAddr(monitor *controller.TaskMonitor, router *sandboxRouter) {
	monitor.RouterAddr = router.addr()
}

// TestTaskMonitorSweepCrashedActorFailsWithActorCrashed covers AC4's crashed
// branch: a Running task whose actor Substrate reports CRASHED, with a
// message that does not name OOM, becomes Failed/ActorCrashed.
func TestTaskMonitorSweepCrashedActorFailsWithActorCrashed(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	mockSrv := &mockControlServer{getActorActor: &ateapipb.Actor{
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			Crash: &ateapipb.ActorCrash{Message: "worker pod evicted"},
		},
	}}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)

	monitor.Sweep(ctx)

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Failed" {
		t.Fatalf("phase = %q, want Failed", got.GetStatus().GetPhase())
	}
	if reason := readyReason(got); reason != "ActorCrashed" {
		t.Fatalf("Ready reason = %q, want ActorCrashed", reason)
	}
	if got.GetStatus().GetWorkerIp() != "" {
		t.Fatalf("WorkerIp = %q, want cleared", got.GetStatus().GetWorkerIp())
	}
}

// TestTaskMonitorSweepOOMCrashFailsWithOOMKilled covers AC4's OOM branch: a
// crash message naming OOM becomes Failed/OOMKilled, not ActorCrashed.
func TestTaskMonitorSweepOOMCrashFailsWithOOMKilled(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	mockSrv := &mockControlServer{getActorActor: &ateapipb.Actor{
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			Crash: &ateapipb.ActorCrash{Message: "sandbox killed: Out of Memory"},
		},
	}}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)

	monitor.Sweep(ctx)

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Failed" {
		t.Fatalf("phase = %q, want Failed", got.GetStatus().GetPhase())
	}
	if reason := readyReason(got); reason != "OOMKilled" {
		t.Fatalf("Ready reason = %q, want OOMKilled", reason)
	}
}

// TestTaskMonitorSweepNotFoundActorFailsWithActorMissing covers AC4's
// NotFound branch and ax-9qu.7: Substrate no longer knowing the actor at
// all becomes Failed/ActorMissing, the same reason LivenessChecker uses for
// this case (liveness.go:44-45, docs/networking.md), not ActorLost.
func TestTaskMonitorSweepNotFoundActorFailsWithActorMissing(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	mockSrv := &mockControlServer{getActorErr: status.Error(codes.NotFound, "actor not found")}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)

	monitor.Sweep(ctx)

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Failed" {
		t.Fatalf("phase = %q, want Failed", got.GetStatus().GetPhase())
	}
	if reason := readyReason(got); reason != "ActorMissing" {
		t.Fatalf("Ready reason = %q, want ActorMissing", reason)
	}
}

// TestTaskMonitorGetActorBoundedByTimeout covers ax-9qu.11: a hung
// Substrate GetActor call must not stall the serial sweep. getActorDelay
// outlasts getActorTimeout (5s), so Sweep must still return promptly.
func TestTaskMonitorGetActorBoundedByTimeout(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	mockSrv := &mockControlServer{getActorDelay: 10 * time.Second}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)

	done := make(chan struct{})
	go func() {
		monitor.Sweep(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("Sweep did not return within 8s: GetActor's 10s delay was not bounded by a shorter timeout")
	}

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running: a GetActor timeout is transient, not proof of loss", got.GetStatus().GetPhase())
	}
}

// runningActor builds an ACTOR_STATE_RUNNING actor so checkTask proceeds to
// probe the runner instead of classifying the actor itself as lost.
func runningActor() *ateapipb.Actor {
	return &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING}}
}

// sweepWithClock sweeps monitor once with Now fixed at t, so a test can
// control the elapsed time between probes without a real sleep.
func sweepWithClock(ctx context.Context, monitor *controller.TaskMonitor, t time.Time) {
	monitor.Now = func() time.Time { return t }
	monitor.Sweep(ctx)
}

// TestTaskMonitorSandboxUnresponsiveNeedsThresholdAndWindow covers AC5: a
// RUNNING actor whose runner never answers /readyz fails the task only once
// 3 consecutive probes span at least 90s. Fewer probes, or the same 3 probes
// too close together, must leave the task Running.
func TestTaskMonitorSandboxUnresponsiveNeedsThresholdAndWindow(t *testing.T) {
	cases := []struct {
		name   string
		clocks []time.Duration // elapsed time (from a fixed start) at each sweep
		want   string
	}{
		{name: "2 misses never fails, no matter the elapsed time", clocks: []time.Duration{0, 200 * time.Second}, want: "Running"},
		{name: "3 misses under 90s does not fail", clocks: []time.Duration{0, 10 * time.Second, 20 * time.Second}, want: "Running"},
		{name: "3 misses spanning at least 90s fails", clocks: []time.Duration{0, 45 * time.Second, 95 * time.Second}, want: "Failed"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			s := memory.NewStore()
			task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
			if err := s.SaveTask(ctx, task); err != nil {
				t.Fatal(err)
			}
			router := newSandboxRouter(t)
			router.set("default", "task1", "dead")
			mockSrv := &mockControlServer{getActorActor: runningActor()}
			reconciler := newTestReconciler(t, mockSrv)
			monitor := controller.NewTaskMonitor(s, reconciler, 0)
			setRouterAddr(monitor, router)

			start := time.Now()
			for _, d := range tt.clocks {
				sweepWithClock(ctx, monitor, start.Add(d))
			}

			got, err := s.GetTask(ctx, "default", "task1")
			if err != nil {
				t.Fatal(err)
			}
			if got.GetStatus().GetPhase() != tt.want {
				t.Fatalf("phase = %q, want %q", got.GetStatus().GetPhase(), tt.want)
			}
			if tt.want == "Failed" {
				if reason := readyReason(got); reason != "SandboxUnresponsive" {
					t.Fatalf("Ready reason = %q, want SandboxUnresponsive", reason)
				}
			}
		})
	}
}

// TestTaskMonitorRouterTransportErrorStaysInconclusive covers ax-9qu.15.
//
// A closed listener refuses every dial. This is a transport-level failure
// to reach atenet-router itself. It is not proof the sandbox died. The
// probe must not count it as a miss.
//
// Without this fix, a router outage of 90s or more fails every RUNNING
// task with SandboxUnresponsive. This is PR #14 review finding 1.
//
// This test runs 4 sweeps spanning well over 90s. The task must still be
// Running at the end.
func TestTaskMonitorRouterTransportErrorStaysInconclusive(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)
	// A closed listener refuses every dial: a transport error, not an
	// HTTP response. This differs from a dead sandboxRouter reply (tested
	// above), which is a real response and must count as a miss.
	monitor.RouterAddr = deadRunnerAddr(t)

	start := time.Now()
	for _, d := range []time.Duration{0, 45 * time.Second, 95 * time.Second, 200 * time.Second} {
		sweepWithClock(ctx, monitor, start.Add(d))
	}

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running: a router transport error must stay inconclusive, not a miss", got.GetStatus().GetPhase())
	}
}

// TestTaskMonitorRouterConnectedButTimedOutCountsAsMiss covers ax-9qu.18,
// PR #16 review finding 1.
//
// The router accepts the probe's connection (httptrace's GotConn fires),
// but its handler never answers. The reconciler's httpClient.Timeout (2s,
// reconciler.go) cuts the probe short with a client-timeout error. This
// differs from TestTaskMonitorRouterTransportErrorStaysInconclusive: there,
// no connection ever forms. Here, the router is reachable; the runner
// behind it is not answering. The probe must count as a miss.
//
// Without this fix, every client timeout was inconclusive. A hung runner
// never grew the miss count, so the task stayed Running forever: the
// ax-9qu symptom this whole bead chain exists to fix.
//
// This test runs 3 sweeps spanning at least 90s. The task must end Failed
// with reason SandboxUnresponsive.
func TestTaskMonitorRouterConnectedButTimedOutCountsAsMiss(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)
	monitor.RouterAddr = strings.TrimPrefix(srv.URL, "http://")

	start := time.Now()
	for _, d := range []time.Duration{0, 45 * time.Second, 95 * time.Second} {
		sweepWithClock(ctx, monitor, start.Add(d))
	}

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Failed" {
		t.Fatalf("phase = %q, want Failed: a probe that connects to the router but gets no response in time must count as a miss", got.GetStatus().GetPhase())
	}
	if reason := readyReason(got); reason != "SandboxUnresponsive" {
		t.Fatalf("Ready reason = %q, want SandboxUnresponsive", reason)
	}
}

// TestTaskMonitorRouterTransportErrorThenRecoveryKeepsMissStreak covers
// ax-9qu.19, PR #16 review finding 2.
//
// A transport error (no connection to the router at all) must leave an
// already-running miss streak untouched: it must not clear it and must not
// add to it. This test interleaves 2 dead router responses, 1 transport
// error, then 1 more dead response, spanning at least 90s from the first
// dead response. Without ax-9qu.18's fix clearing the streak on a transport
// error, this sequence would reset the window and the task would stay
// Running. The fix must still end the task Failed.
func TestTaskMonitorRouterTransportErrorThenRecoveryKeepsMissStreak(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	router := newSandboxRouter(t)
	router.set("default", "task1", "dead")
	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)
	deadListener := deadRunnerAddr(t)
	routerAddr := router.addr()

	start := time.Now()
	// Miss 1: a real dead response from the router.
	setRouterAddr(monitor, router)
	sweepWithClock(ctx, monitor, start)
	// Miss 2: a real dead response from the router.
	sweepWithClock(ctx, monitor, start.Add(45*time.Second))
	// A transport error: the router itself is unreachable. This must not
	// clear the 2 misses already recorded, and must not count as a 3rd.
	monitor.RouterAddr = deadListener
	sweepWithClock(ctx, monitor, start.Add(60*time.Second))
	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running after only 2 misses plus 1 transport error", got.GetStatus().GetPhase())
	}
	// Miss 3: a real dead response from the router again, spanning >=90s
	// from the first miss. The earlier transport error must not have
	// cleared the streak.
	monitor.RouterAddr = routerAddr
	sweepWithClock(ctx, monitor, start.Add(95*time.Second))

	got, err = s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Failed" {
		t.Fatalf("phase = %q, want Failed: a transport error in the middle of a miss streak must not clear it", got.GetStatus().GetPhase())
	}
	if reason := readyReason(got); reason != "SandboxUnresponsive" {
		t.Fatalf("Ready reason = %q, want SandboxUnresponsive", reason)
	}
}

// TestTaskMonitorReadyz503WorkspaceInitializingStaysRunning covers AC5's
// alive predicate: a 503 naming workspace setup counts as alive, so the task
// stays Running even after many sweeps.
func TestTaskMonitorReadyz503WorkspaceInitializingStaysRunning(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()

	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	router := newSandboxRouter(t)
	router.set("default", "task1", "initializing")
	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)
	setRouterAddr(monitor, router)

	start := time.Now()
	for i := 0; i < 5; i++ {
		sweepWithClock(ctx, monitor, start.Add(time.Duration(i)*100*time.Second))
	}

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running: a 503 workspace-initializing probe is alive", got.GetStatus().GetPhase())
	}
}

// TestTaskMonitorFailTaskSkipsWriteWhenStatusIDChanged covers AC6. A
// concurrent write changes the task's status.id. This happens between
// failTask's re-read and its conditional write. The write must not apply.
// raceStore (see liveness_test.go) fires its hook right before the store's
// UpdateTaskStatusIf call. This lands the concurrent write at that exact
// point. By then, failTask has already re-read the task and passed its own
// id and phase check. So only the store's conditional write can still catch
// the race.
func TestTaskMonitorFailTaskSkipsWriteWhenStatusIDChanged(t *testing.T) {
	ctx := context.Background()
	inner := memory.NewStore()
	workerAddr := deadRunnerAddr(t)
	task := taskWithWorker("default", "task1", "task1", workerAddr, "id-1")
	if err := inner.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	router := newSandboxRouter(t)
	router.set("default", "task1", "dead")

	raced := &raceStore{Store: inner, hook: func() {
		if err := inner.UpdateTaskStatus(ctx, "default", "task1", &v1alpha1.TaskStatus{
			Phase: "Running", Actor: "task1", WorkerIp: workerAddr, Id: "id-2",
		}); err != nil {
			t.Fatal(err)
		}
	}}

	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(raced, reconciler, 0)
	setRouterAddr(monitor, router)

	start := time.Now()
	sweepWithClock(ctx, monitor, start)
	sweepWithClock(ctx, monitor, start.Add(45*time.Second))
	sweepWithClock(ctx, monitor, start.Add(95*time.Second))

	got, err := inner.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running: failTask must not overwrite a task whose status.id changed underneath it", got.GetStatus().GetPhase())
	}
	if got.GetStatus().GetId() != "id-2" {
		t.Fatalf("Id = %q, want the concurrent write's %q preserved", got.GetStatus().GetId(), "id-2")
	}
}

// TestTaskMonitorFailTaskSkipsWriteWhenPhaseLeftRunning covers AC6's other
// race: the task leaves Running (a suspend) between failTask's re-read and
// its conditional write, so the write must not apply.
func TestTaskMonitorFailTaskSkipsWriteWhenPhaseLeftRunning(t *testing.T) {
	ctx := context.Background()
	inner := memory.NewStore()
	workerAddr := deadRunnerAddr(t)
	task := taskWithWorker("default", "task1", "task1", workerAddr, "id-1")
	if err := inner.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	router := newSandboxRouter(t)
	router.set("default", "task1", "dead")

	raced := &raceStore{Store: inner, hook: func() {
		if err := inner.UpdateTaskStatus(ctx, "default", "task1", &v1alpha1.TaskStatus{
			Phase: "Suspended", Actor: "task1", WorkerIp: workerAddr, Id: "id-1",
		}); err != nil {
			t.Fatal(err)
		}
	}}

	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(raced, reconciler, 0)
	setRouterAddr(monitor, router)

	start := time.Now()
	sweepWithClock(ctx, monitor, start)
	sweepWithClock(ctx, monitor, start.Add(45*time.Second))
	sweepWithClock(ctx, monitor, start.Add(95*time.Second))

	got, err := inner.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Suspended" {
		t.Fatalf("phase = %q, want Suspended: failTask must not overwrite a task that left Running underneath it", got.GetStatus().GetPhase())
	}
}

// listCountingStore wraps a store.Store and counts ListTasks calls, so a
// test can prove Run never lists tasks when the monitor is disabled.
type listCountingStore struct {
	store.Store
	calls int
}

func (s *listCountingStore) ListTasks(ctx context.Context, atespace string, limit, offset int64) ([]*v1alpha1.Task, error) {
	s.calls++
	return s.Store.ListTasks(ctx, atespace, limit, offset)
}

// TestTaskMonitorRunDisabledNeverListsTasks covers AC6's last rule:
// Interval <= 0 disables the monitor, so Run must return at once and never
// call ListTasks.
func TestTaskMonitorRunDisabledNeverListsTasks(t *testing.T) {
	ctx := context.Background()
	counted := &listCountingStore{Store: memory.NewStore()}
	mockSrv := &mockControlServer{}
	reconciler := newTestReconciler(t, mockSrv)

	for _, interval := range []time.Duration{0, -1 * time.Second} {
		monitor := controller.NewTaskMonitor(counted, reconciler, interval)
		done := make(chan struct{})
		go func() {
			monitor.Run(ctx)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("Run(ctx) with interval %v did not return promptly", interval)
		}
	}

	if counted.calls != 0 {
		t.Fatalf("ListTasks was called %d times, want 0 when the monitor is disabled", counted.calls)
	}
}

// TestTaskMonitorNoRouterAddrDisablesSandboxCheck covers ax-9qu.10: with no
// RouterAddr (the ax-9qu.16 field, unset here), the SandboxUnresponsive
// check must stay off, not fail a task from a direct dial that would 421
// for lack of the ate-target-actor header atunnel's ingress requires on
// every request.
func TestTaskMonitorNoRouterAddrDisablesSandboxCheck(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)
	// monitor.RouterAddr is left at its zero value ("") on purpose.

	start := time.Now()
	for _, d := range []time.Duration{0, 45 * time.Second, 95 * time.Second, 200 * time.Second} {
		sweepWithClock(ctx, monitor, start.Add(d))
	}

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running: the sandbox check must stay off with no router address", got.GetStatus().GetPhase())
	}
}

// TestTaskMonitorStaleAssignmentDefersToLivenessChecker covers ax-9qu.8: a
// 421 with X-Ate-Assignment-Stale is LivenessChecker's signal, on its own
// threshold and timeline (docs/networking.md), when LivenessChecker will
// run (DeferStaleAssignment true, ax-9qu.17). TaskMonitor must not count it
// toward SandboxUnresponsive: that would fail the same task for the same
// symptom on a shorter timeline than the documented one.
func TestTaskMonitorStaleAssignmentDefersToLivenessChecker(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	router := newSandboxRouter(t)
	router.set("default", "task1", "stale")
	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)
	setRouterAddr(monitor, router)
	monitor.DeferStaleAssignment = true

	start := time.Now()
	for _, d := range []time.Duration{0, 45 * time.Second, 95 * time.Second} {
		sweepWithClock(ctx, monitor, start.Add(d))
	}

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running: a stale-assignment 421 is LivenessChecker's signal, not TaskMonitor's", got.GetStatus().GetPhase())
	}
}

// TestTaskMonitorStaleAssignmentFailsWhenLivenessCheckerIsOff covers
// ax-9qu.17: with DeferStaleAssignment false (LivenessChecker's sweep is
// off, for example --task-liveness-interval=0), nothing else catches a
// stale-routed task. TaskMonitor must count the 421 like any other dead
// probe, instead of silently leaving the task Running forever.
func TestTaskMonitorStaleAssignmentFailsWhenLivenessCheckerIsOff(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	router := newSandboxRouter(t)
	router.set("default", "task1", "stale")
	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)
	setRouterAddr(monitor, router)
	// monitor.DeferStaleAssignment is left at its zero value (false).

	start := time.Now()
	for _, d := range []time.Duration{0, 45 * time.Second, 95 * time.Second} {
		sweepWithClock(ctx, monitor, start.Add(d))
	}

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Failed" {
		t.Fatalf("phase = %q, want Failed: with LivenessChecker off, TaskMonitor must catch a stale-routed task itself", got.GetStatus().GetPhase())
	}
	if reason := readyReason(got); reason != "SandboxUnresponsive" {
		t.Fatalf("Ready reason = %q, want SandboxUnresponsive", reason)
	}
}

// TestTaskMonitorProbeUsesAteTargetActorHeaderAndRouterAlive covers
// ax-9qu.14: the only production path (ATENET_ROUTER_ADDR set) must send
// the ate-target-actor header, and a 200 through the router must keep the
// task Running.
func TestTaskMonitorProbeUsesAteTargetActorHeaderAndRouterAlive(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	router := newSandboxRouter(t)
	router.set("default", "task1", "alive")
	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)
	setRouterAddr(monitor, router)

	monitor.Sweep(ctx)

	calls := router.headerCalls()
	if len(calls) == 0 {
		t.Fatal("router saw no requests: the monitor did not probe through atenet-router")
	}
	for _, c := range calls {
		if c != "default/task1" {
			t.Fatalf("ate-target-actor header = %q, want %q", c, "default/task1")
		}
	}

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running", got.GetStatus().GetPhase())
	}
}

// TestTaskMonitorMissStreakResetsOnAliveProbe covers ax-9qu.14: a healthy
// probe between two short runs of misses resets the streak, so the task
// never fails even though 4 total dead probes happen across the test.
// Without the reset, these would add up to a streak that reaches the
// threshold.
func TestTaskMonitorMissStreakResetsOnAliveProbe(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	router := newSandboxRouter(t)
	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)
	setRouterAddr(monitor, router)

	start := time.Now()
	router.set("default", "task1", "dead")
	sweepWithClock(ctx, monitor, start)
	sweepWithClock(ctx, monitor, start.Add(45*time.Second))

	router.set("default", "task1", "alive")
	sweepWithClock(ctx, monitor, start.Add(60*time.Second))

	router.set("default", "task1", "dead")
	sweepWithClock(ctx, monitor, start.Add(200*time.Second))
	sweepWithClock(ctx, monitor, start.Add(245*time.Second))

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running: an alive probe must reset the miss streak", got.GetStatus().GetPhase())
	}
}

// TestTaskMonitorPruneMissesAcrossNonRunningGap covers ax-9qu.13: a task
// that leaves Running and later returns to Running, keeping the same
// status.id, must not inherit its earlier, stale miss count. Sweep's
// pruneMisses call must drop the count while the task is not Running, so
// the task starts a fresh run of misses once it returns, instead of
// failing on this sweep's first new miss.
func TestTaskMonitorPruneMissesAcrossNonRunningGap(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := taskWithWorker("default", "task1", "task1", deadRunnerAddr(t), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	router := newSandboxRouter(t)
	router.set("default", "task1", "dead")
	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)
	setRouterAddr(monitor, router)

	start := time.Now()
	// Two dead probes, still short of the 3-miss threshold.
	sweepWithClock(ctx, monitor, start)
	sweepWithClock(ctx, monitor, start.Add(10*time.Second))

	// The task leaves Running for a while, with the same status.id: a
	// suspend/resume cycle without a fresh Id. Sweep must not see it as
	// Running here, so pruneMisses drops its miss count.
	if err := s.UpdateTaskStatus(ctx, "default", "task1", &v1alpha1.TaskStatus{
		Phase: "Suspended", Actor: "task1", WorkerIp: task.GetStatus().GetWorkerIp(), Id: "id-1",
	}); err != nil {
		t.Fatal(err)
	}
	sweepWithClock(ctx, monitor, start.Add(500*time.Second))

	// Back to Running, same Id. A long time has passed since the very
	// first miss above (> the 90s window), so an unpruned count would
	// reach the 3-miss threshold on this single new miss and fail the task
	// at once.
	if err := s.UpdateTaskStatus(ctx, "default", "task1", &v1alpha1.TaskStatus{
		Phase: "Running", Actor: "task1", WorkerIp: task.GetStatus().GetWorkerIp(), Id: "id-1",
	}); err != nil {
		t.Fatal(err)
	}
	sweepWithClock(ctx, monitor, start.Add(600*time.Second))

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running: the pruned miss count must not let one post-gap miss reach the threshold", got.GetStatus().GetPhase())
	}
}
