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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// captureSweepLogs swaps in a text handler over buf for the duration of fn,
// then restores the previous default logger, and returns what fn logged.
func captureSweepLogs(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	fn()
	return buf.String()
}

// drainN reads and discards n events from sub, failing the test if one does
// not arrive within a second. Tests use it to skip past SaveTask's own
// reconcile event before exercising the delete path.
func drainN(t *testing.T, sub store.Subscription, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := sub.Next(ctx)
		cancel()
		if err != nil {
			t.Fatalf("draining event %d of %d: %v", i+1, n, err)
		}
	}
}

// assertNoEvent fails the test if sub has an event waiting within a short
// window: a test uses it to prove a sweep did not requeue anything.
func assertNoEvent(t *testing.T, sub store.Subscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := sub.Next(ctx); err == nil {
		t.Fatal("got an event when none was expected")
	}
}

// fakeActorLister is a stub ActorLister a test can point at a fixed list
// or a fixed error, standing in for substrate.Client.ListActors.
type fakeActorLister struct {
	actors []*ateapipb.Actor
	err    error
}

func (f *fakeActorLister) ListActors(context.Context, string) ([]*ateapipb.Actor, error) {
	return f.actors, f.err
}

// failNRequeueStore wraps a store.Store and fails the first remainingFails
// calls to RequeueTaskDeletion, standing in for a second failure landing on
// top of the worker's own scheduleDeleteRetry attempt (ax-706 AC1: the
// requeue that drops before any ticker sweep exists to catch it).
type failNRequeueStore struct {
	store.Store
	mu             sync.Mutex
	remainingFails int
}

func (s *failNRequeueStore) RequeueTaskDeletion(ctx context.Context, atespace, name string, p store.TaskDeletePreconditions) error {
	s.mu.Lock()
	fail := s.remainingFails > 0
	if fail {
		s.remainingFails--
	}
	s.mu.Unlock()
	if fail {
		return errors.New("simulated requeue failure")
	}
	return s.Store.RequeueTaskDeletion(ctx, atespace, name, p)
}

// failOnCallListTasksStore wraps a store.Store and fails exactly the call
// numbered failOnCall (1-indexed) to ListTasks. It stands in for one bad
// page, or one transient store outage, landing on a single sweep tick
// (ax-706.7).
type failOnCallListTasksStore struct {
	store.Store
	mu         sync.Mutex
	calls      int
	failOnCall int
}

func (s *failOnCallListTasksStore) ListTasks(ctx context.Context, atespace string, limit, offset int64) ([]*v1alpha1.Task, error) {
	s.mu.Lock()
	s.calls++
	fail := s.calls == s.failOnCall
	s.mu.Unlock()
	if fail {
		return nil, errors.New("simulated list tasks failure")
	}
	return s.Store.ListTasks(ctx, atespace, limit, offset)
}

// newDeletionTestClient starts an in-process Substrate control server and
// returns a client dialed to it, mirroring worker_delete_test.go.
func newDeletionTestClient(t *testing.T, control *deletionControlServer) *substrate.Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	ateapipb.RegisterControlServer(server, control)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	client, err := substrate.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestDeleteSweepRequeuesAfterDroppedWorkerRetry covers AC1: DeleteActor
// fails once, the worker's own scheduleDeleteRetry then also fails to
// requeue once, so the task sits Terminating with no pending retry at all.
// One DeletionSweeper.SweepOnce after MinAge requeues the delete, and
// processEvent on that requeued event finally removes the task.
func TestDeleteSweepRequeuesAfterDroppedWorkerRetry(t *testing.T) {
	control := &deletionControlServer{
		deletions:      make(chan *ateapipb.DeleteActorRequest, 4),
		templates:      make(chan *ateapipb.ListActorTemplatesRequest, 4),
		remainingFails: 1,
	}
	client := newDeletionTestClient(t, control)

	ctx := context.Background()
	base := memory.NewStore()
	wrapped := &failNRequeueStore{Store: base, remainingFails: 1}

	created := time.Now().UTC()
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "stuck", Atespace: "default", CreationTimestamp: timestamppb.New(created)}}
	if err := base.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	sub, err := base.Subscribe(ctx, "test-group", "test-consumer")
	if err != nil {
		t.Fatal(err)
	}
	drainN(t, sub, 1) // SaveTask's own reconcile event.

	deletion := store.TaskDeletePreconditions{ActorUID: "actor-1", CreationTimestamp: created.Format(time.RFC3339Nano)}
	if err := base.MarkTaskDeleting(ctx, "default", "stuck", deletion); err != nil {
		t.Fatal(err)
	}

	worker := NewWorker(wrapped, NewTaskReconciler(client, "default-template", "ax-system"), "", "")
	worker.DeleteBackoffBase = 10 * time.Millisecond
	worker.DeleteBackoffMax = 20 * time.Millisecond

	readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
	ev, err := sub.Next(readCtx)
	readCancel()
	if err != nil {
		t.Fatalf("no initial delete event: %v", err)
	}
	if err := worker.processEvent(context.Background(), ev); err != nil {
		t.Fatalf("processEvent on the first delivery: %v", err)
	}

	// scheduleDeleteRetry's goroutine fires after DeleteBackoffBase and
	// calls wrapped.RequeueTaskDeletion, which fails once: the worker's own
	// retry drops here, with no event published for it.
	time.Sleep(60 * time.Millisecond)
	assertNoEvent(t, sub)

	now := time.Now()
	sweeper := &DeletionSweeper{
		Store:  wrapped,
		MinAge: 15 * time.Minute,
		Now:    func() time.Time { return now },
	}
	sweeper.SweepOnce(ctx) // seeds tracking for "default/stuck" at age 0.

	now = now.Add(16 * time.Minute)
	sweeper.SweepOnce(ctx) // age 16m >= MinAge: requeues the dropped delete.

	readCtx2, readCancel2 := context.WithTimeout(ctx, 2*time.Second)
	retryEv, err := sub.Next(readCtx2)
	readCancel2()
	if err != nil {
		t.Fatalf("sweep did not requeue the stuck delete: %v", err)
	}
	if !retryEv.Retry || retryEv.Action != "delete" {
		t.Fatalf("unexpected requeued event: %+v", retryEv)
	}

	if err := worker.processEvent(context.Background(), retryEv); err != nil {
		t.Fatalf("processEvent on the sweep's retry: %v", err)
	}

	if _, err := base.GetTask(ctx, "default", "stuck"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("task record not removed after the sweep-driven retry: %v", err)
	}
	if got := control.Attempts(); got != 2 {
		t.Fatalf("want 2 DeleteActor attempts (1 failure + 1 success), got %d", got)
	}
}

// TestDeleteSweepDoesNotMultiplyRetryChainsOnRepeatedFailure covers review
// finding ax-706.6: a Substrate failure that outlasts several sweep ticks
// must not grow one new scheduleDeleteRetry chain per tick. Each sweep
// republish that lands while the worker's own chain still sleeps on its
// backoff must join that chain, not start another one of its own.
func TestDeleteSweepDoesNotMultiplyRetryChainsOnRepeatedFailure(t *testing.T) {
	control := &deletionControlServer{
		deletions:      make(chan *ateapipb.DeleteActorRequest, 16),
		templates:      make(chan *ateapipb.ListActorTemplatesRequest, 16),
		remainingFails: 1000, // DeleteActor never succeeds during this test.
	}
	client := newDeletionTestClient(t, control)

	ctx := context.Background()
	s := memory.NewStore()
	created := time.Now().UTC()
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "stuck", Atespace: "default", CreationTimestamp: timestamppb.New(created)}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	sub, err := s.Subscribe(ctx, "test-group", "test-consumer")
	if err != nil {
		t.Fatal(err)
	}
	drainN(t, sub, 1) // SaveTask's own reconcile event.

	deletion := store.TaskDeletePreconditions{ActorUID: "actor-1", CreationTimestamp: created.Format(time.RFC3339Nano)}
	if err := s.MarkTaskDeleting(ctx, "default", "stuck", deletion); err != nil {
		t.Fatal(err)
	}

	worker := NewWorker(s, NewTaskReconciler(client, "default-template", "ax-system"), "", "")
	// A backoff far longer than this test's own run time, so the worker's
	// first chain is still sleeping (pending) when later sweep ticks land.
	worker.DeleteBackoffBase = time.Minute
	worker.DeleteBackoffMax = time.Minute

	readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
	ev, err := sub.Next(readCtx)
	readCancel()
	if err != nil {
		t.Fatalf("no initial delete event: %v", err)
	}
	if err := worker.processEvent(context.Background(), ev); err != nil {
		t.Fatalf("processEvent on the first delivery: %v", err)
	}

	now := time.Now()
	sweeper := &DeletionSweeper{Store: s, MinAge: 15 * time.Minute, Now: func() time.Time { return now }}
	sweeper.SweepOnce(ctx) // seeds tracking for "default/stuck" at age 0.

	// 3 more sweep ticks, each past MinAge, each landing while the first
	// chain still sleeps on its 1-minute backoff.
	for i := 0; i < 3; i++ {
		now = now.Add(16 * time.Minute)
		sweeper.SweepOnce(ctx)

		tickCtx, tickCancel := context.WithTimeout(ctx, 2*time.Second)
		tickEv, err := sub.Next(tickCtx)
		tickCancel()
		if err != nil {
			t.Fatalf("tick %d: sweep did not requeue the stuck delete: %v", i, err)
		}
		if err := worker.processEvent(context.Background(), tickEv); err != nil {
			t.Fatalf("tick %d: processEvent: %v", i, err)
		}
	}

	worker.deleteMu.Lock()
	chains := worker.deleteAttempts["default/stuck"]
	pending := worker.deletePending["default/stuck"]
	worker.deleteMu.Unlock()
	if chains != 1 {
		t.Fatalf("want 1 retry chain total despite 3 extra sweep-driven failures, got %d (each extra failure started its own chain)", chains)
	}
	if !pending {
		t.Fatal("want the one chain still marked pending (its 1-minute backoff has not fired)")
	}
}

// TestDeleteSweepYoungTaskNotCountedOrRequeued covers AC2's first half: a
// Terminating task younger than MinAge gets no requeue at all.
func TestDeleteSweepYoungTaskNotCountedOrRequeued(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "young", Atespace: "default"}, Status: &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseTerminating}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskStatus(ctx, "default", "young", task.Status); err != nil {
		t.Fatal(err)
	}

	sub, err := s.Subscribe(ctx, "g", "c")
	if err != nil {
		t.Fatal(err)
	}
	drainN(t, sub, 1)

	now := time.Now()
	sweeper := &DeletionSweeper{Store: s, MinAge: 15 * time.Minute, Now: func() time.Time { return now }}
	sweeper.SweepOnce(ctx) // seeds age at 0.

	now = now.Add(5 * time.Minute)
	sweeper.SweepOnce(ctx) // age 5m < MinAge: must not requeue.

	assertNoEvent(t, sub)
}

// TestDeleteSweepRecreatedTaskRestartsAge covers AC2's second half: a
// re-created task (a new CreationTimestamp under the same atespace/name)
// restarts its age, instead of inheriting the age of the task that used to
// hold that name.
func TestDeleteSweepRecreatedTaskRestartsAge(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	oldCreated := time.Now().UTC().Add(-time.Hour)
	oldTask := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "reused", Atespace: "default", CreationTimestamp: timestamppb.New(oldCreated)},
		Status:   &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseTerminating},
	}
	if err := s.SaveTask(ctx, oldTask); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskStatus(ctx, "default", "reused", oldTask.Status); err != nil {
		t.Fatal(err)
	}

	sub, err := s.Subscribe(ctx, "g", "c")
	if err != nil {
		t.Fatal(err)
	}
	drainN(t, sub, 1)

	now := time.Now()
	sweeper := &DeletionSweeper{Store: s, MinAge: 15 * time.Minute, Now: func() time.Time { return now }}
	sweeper.SweepOnce(ctx) // seeds age for the old CreationTimestamp at 0.

	now = now.Add(20 * time.Minute)
	sweeper.SweepOnce(ctx) // old task is now stuck: one requeue goes out.
	drainN(t, sub, 1)      // consume that requeue so it does not confuse the next check.

	if err := s.DeleteTask(ctx, "default", "reused"); err != nil {
		t.Fatal(err)
	}
	newTask := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "reused", Atespace: "default", CreationTimestamp: timestamppb.New(now)},
		Status:   &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseTerminating},
	}
	if err := s.SaveTask(ctx, newTask); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskStatus(ctx, "default", "reused", newTask.Status); err != nil {
		t.Fatal(err)
	}
	drainN(t, sub, 1) // the new task's own SaveTask reconcile event.

	sweeper.SweepOnce(ctx) // same wall clock: the new CreationTimestamp is age 0.
	assertNoEvent(t, sub)
}

// TestDeleteSweepProtectedTaskCountedNeverRequeued covers AC3: a stuck
// supervisor-profile task is counted, but the sweep never requeues it,
// because its ActorUID and CreationTimestamp preconditions are not stored
// on the record for an empty-precondition requeue to satisfy.
func TestDeleteSweepProtectedTaskCountedNeverRequeued(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "supervised", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{SupervisorProfile: "fabriek-native-v1"},
		Status:   &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseTerminating},
	}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	sub, err := s.Subscribe(ctx, "g", "c")
	if err != nil {
		t.Fatal(err)
	}
	drainN(t, sub, 1)

	now := time.Now()
	sweeper := &DeletionSweeper{Store: s, MinAge: 15 * time.Minute, Now: func() time.Time { return now }}
	sweeper.SweepOnce(ctx) // seeds age at 0.

	now = now.Add(20 * time.Minute)
	output := captureSweepLogs(t, func() { sweeper.SweepOnce(ctx) })

	assertNoEvent(t, sub)
	if !strings.Contains(output, "stuck_tasks=1") || !strings.Contains(output, "protected=1") {
		t.Fatalf("want stuck_tasks=1 protected=1, got: %s", output)
	}
	if !strings.Contains(output, "requeued=0") {
		t.Fatalf("want requeued=0 for a protected task, got: %s", output)
	}
}

// TestDeleteSweepCountsOnlyOldDeletingActors covers AC4: a DELETING actor
// at or past MinAge counts; one 5 minutes old, or a RUNNING actor, does
// not. ActorLister exposes no DeleteActor method, so the sweep has no way
// to call it (ax-706 DESIGN item 3).
func TestDeleteSweepCountsOnlyOldDeletingActors(t *testing.T) {
	now := time.Now()
	old := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "old", Uid: "uid-old", UpdateTime: timestamppb.New(now.Add(-20 * time.Minute))},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING, WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPod: "worker-1"}},
	}
	young := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "young", UpdateTime: timestamppb.New(now.Add(-5 * time.Minute))},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
	}
	running := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "running", UpdateTime: timestamppb.New(now.Add(-time.Hour))},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	}
	sweeper := &DeletionSweeper{
		Store:  memory.NewStore(),
		Actors: &fakeActorLister{actors: []*ateapipb.Actor{old, young, running}},
		MinAge: 15 * time.Minute,
		Now:    func() time.Time { return now },
	}

	output := captureSweepLogs(t, func() { sweeper.SweepOnce(context.Background()) })
	if !strings.Contains(output, "stuck_actors=1") {
		t.Fatalf("want stuck_actors=1, got: %s", output)
	}
	if !strings.Contains(output, "stuck=1") {
		t.Fatalf("want stuck=1, got: %s", output)
	}
	if !strings.Contains(output, "name=old") || !strings.Contains(output, "uid=uid-old") || !strings.Contains(output, "worker_pod=worker-1") {
		t.Fatalf("missing per-actor detail for the stuck actor: %s", output)
	}
	if strings.Contains(output, "name=young") || strings.Contains(output, "name=running") {
		t.Fatalf("a non-stuck actor was logged as stuck: %s", output)
	}
}

// TestDeleteSweepActorListErrorStillLogsTaskCounts covers AC5: a ListActors
// failure still produces the task counts, with actor_list_error=true.
func TestDeleteSweepActorListErrorStillLogsTaskCounts(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "stuck", Atespace: "default"}, Status: &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseTerminating}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskStatus(ctx, "default", "stuck", task.Status); err != nil {
		t.Fatal(err)
	}
	sub, err := s.Subscribe(ctx, "g", "c")
	if err != nil {
		t.Fatal(err)
	}
	drainN(t, sub, 1)

	now := time.Now()
	sweeper := &DeletionSweeper{
		Store:  s,
		Actors: &fakeActorLister{err: errors.New("substrate unavailable")},
		MinAge: 15 * time.Minute,
		Now:    func() time.Time { return now },
	}
	sweeper.SweepOnce(ctx) // seeds age at 0.
	now = now.Add(20 * time.Minute)

	output := captureSweepLogs(t, func() { sweeper.SweepOnce(ctx) })
	if !strings.Contains(output, "stuck_tasks=1") {
		t.Fatalf("want stuck_tasks=1 despite the actor list error, got: %s", output)
	}
	if !strings.Contains(output, "actor_list_error=true") {
		t.Fatalf("want actor_list_error=true, got: %s", output)
	}
}

// TestDeleteSweepListTasksErrorKeepsAgeAndFlagsLine covers review finding
// ax-706.7: a ListTasks failure on one tick must not reset the ages this
// process already tracked, and must not make that tick's summary line read
// as a false all-clear. The task still counts as stuck once a later tick
// lists tasks again, at the age it had before the failed tick, and the
// failed tick's own line must carry task_list_error=true.
func TestDeleteSweepListTasksErrorKeepsAgeAndFlagsLine(t *testing.T) {
	ctx := context.Background()
	base := memory.NewStore()
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "stuck", Atespace: "default"}, Status: &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseTerminating}}
	if err := base.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := base.UpdateTaskStatus(ctx, "default", "stuck", task.Status); err != nil {
		t.Fatal(err)
	}
	sub, err := base.Subscribe(ctx, "g", "c")
	if err != nil {
		t.Fatal(err)
	}
	drainN(t, sub, 1)

	// Call 1 seeds the age (tick 1). Call 2 fails (tick 2). Call 3 lists
	// again (tick 3).
	wrapped := &failOnCallListTasksStore{Store: base, failOnCall: 2}
	now := time.Now()
	sweeper := &DeletionSweeper{Store: wrapped, MinAge: 15 * time.Minute, Now: func() time.Time { return now }}

	sweeper.SweepOnce(ctx) // tick 1: seeds age for "default/stuck" at 0.

	now = now.Add(16 * time.Minute)
	tick2 := captureSweepLogs(t, func() { sweeper.SweepOnce(ctx) }) // tick 2: ListTasks fails.
	if !strings.Contains(tick2, "task_list_error=true") {
		t.Fatalf("want task_list_error=true on the failed tick, got: %s", tick2)
	}
	if !strings.Contains(tick2, "stuck_tasks=0") {
		t.Fatalf("want stuck_tasks=0 on the failed tick (it listed no tasks), got: %s", tick2)
	}
	assertNoEvent(t, sub) // the failed tick requeued nothing.

	// Same wall clock: if the failed tick had reset the age, this tick
	// would see age 0 and not count the task as stuck.
	tick3 := captureSweepLogs(t, func() { sweeper.SweepOnce(ctx) })
	if !strings.Contains(tick3, "task_list_error=false") {
		t.Fatalf("want task_list_error=false once listing recovers, got: %s", tick3)
	}
	if !strings.Contains(tick3, "stuck_tasks=1") {
		t.Fatalf("want the task still counted stuck at its original age, got: %s", tick3)
	}

	readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
	defer readCancel()
	if _, err := sub.Next(readCtx); err != nil {
		t.Fatalf("want the recovered tick to requeue the stuck delete: %v", err)
	}
}

// TestDeleteSweepLogsOneSummaryLinePerTick covers AC6: every SweepOnce
// call logs exactly one msg="delete sweep: stuck deletions" line, carrying
// every documented field, WARN when stuck>0 and INFO (also) at stuck=0.
func TestDeleteSweepLogsOneSummaryLinePerTick(t *testing.T) {
	ctx := context.Background()

	t.Run("zero stuck logs INFO", func(t *testing.T) {
		sweeper := &DeletionSweeper{Store: memory.NewStore(), MinAge: 15 * time.Minute}
		output := captureSweepLogs(t, func() { sweeper.SweepOnce(ctx) })
		if strings.Count(output, `msg="delete sweep: stuck deletions"`) != 1 {
			t.Fatalf("want exactly 1 summary line, got: %s", output)
		}
		if !strings.Contains(output, "level=INFO") {
			t.Fatalf("want level=INFO at stuck=0, got: %s", output)
		}
		for _, want := range []string{"stuck=0", "stuck_tasks=0", "stuck_actors=0", "protected=0", "requeued=0", "requeue_failed=0", "actor_list_error=false", "min_age="} {
			if !strings.Contains(output, want) {
				t.Fatalf("missing %q in: %s", want, output)
			}
		}
	})

	t.Run("stuck tasks log WARN", func(t *testing.T) {
		s := memory.NewStore()
		task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "stuck", Atespace: "default"}, Status: &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseTerminating}}
		if err := s.SaveTask(ctx, task); err != nil {
			t.Fatal(err)
		}
		if err := s.UpdateTaskStatus(ctx, "default", "stuck", task.Status); err != nil {
			t.Fatal(err)
		}
		sub, err := s.Subscribe(ctx, "g", "c")
		if err != nil {
			t.Fatal(err)
		}
		drainN(t, sub, 1)

		now := time.Now()
		sweeper := &DeletionSweeper{Store: s, MinAge: 15 * time.Minute, Now: func() time.Time { return now }}
		sweeper.SweepOnce(ctx) // seeds age at 0.
		now = now.Add(20 * time.Minute)

		output := captureSweepLogs(t, func() { sweeper.SweepOnce(ctx) })
		if strings.Count(output, `msg="delete sweep: stuck deletions"`) != 1 {
			t.Fatalf("want exactly 1 summary line, got: %s", output)
		}
		if !strings.Contains(output, "level=WARN") {
			t.Fatalf("want level=WARN at stuck>0, got: %s", output)
		}
		if !strings.Contains(output, "stuck=1") {
			t.Fatalf("want stuck=1, got: %s", output)
		}
	})
}
