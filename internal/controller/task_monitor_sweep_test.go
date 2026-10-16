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

// TestTaskMonitorSweepNotFoundActorFailsWithActorLost covers AC4's NotFound
// branch: Substrate no longer knowing the actor becomes Failed/ActorLost.
func TestTaskMonitorSweepNotFoundActorFailsWithActorLost(t *testing.T) {
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
	if reason := readyReason(got); reason != "ActorLost" {
		t.Fatalf("Ready reason = %q, want ActorLost", reason)
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
			mockSrv := &mockControlServer{getActorActor: runningActor()}
			reconciler := newTestReconciler(t, mockSrv)
			monitor := controller.NewTaskMonitor(s, reconciler, 0)

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

// TestTaskMonitorReadyz503WorkspaceInitializingStaysRunning covers AC5's
// alive predicate: a 503 naming workspace setup counts as alive, so the task
// stays Running even after many sweeps.
func TestTaskMonitorReadyz503WorkspaceInitializingStaysRunning(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("workspace initializing"))
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(lis)
	t.Cleanup(func() { _ = srv.Close() })

	task := taskWithWorker("default", "task1", "task1", lis.Addr().String(), "id-1")
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	mockSrv := &mockControlServer{getActorActor: runningActor()}
	reconciler := newTestReconciler(t, mockSrv)
	monitor := controller.NewTaskMonitor(s, reconciler, 0)

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
