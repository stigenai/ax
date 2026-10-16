package controller

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProtectedDeletionRejectsDelayedEventBeforeSubstrate(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	created := time.Now().UTC()
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private", CreationTimestamp: timestamppb.New(created)}, Spec: &v1alpha1.TaskSpec{SupervisorProfile: "fabriek-native-v1"}, Status: &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseTerminating}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	worker := &Worker{store: s}
	for _, precondition := range []store.TaskDeletePreconditions{{}, {ActorUID: "old-actor", CreationTimestamp: created.Add(-time.Second).Format(time.RFC3339Nano)}} {
		err := worker.processEvent(ctx, store.TaskEvent{Atespace: "private", Name: "job", Action: "delete", Deletion: precondition})
		if !errors.Is(err, store.ErrConflict) {
			t.Fatalf("stale event reached substrate: %v", err)
		}
	}
	// Queued reconcile after the deletion mark must not create or resume an actor.
	if err := worker.processEvent(ctx, store.TaskEvent{Atespace: "private", Name: "job", Action: "reconcile"}); err != nil {
		t.Fatal(err)
	}
}

type deletionControlServer struct {
	ateapipb.UnimplementedControlServer
	deletions chan *ateapipb.DeleteActorRequest
	templates chan *ateapipb.ListActorTemplatesRequest

	mu             sync.Mutex
	remainingFails int
	attempts       int
}

// DeleteActor fails with codes.Unavailable for the first remainingFails
// calls, then succeeds. This stands in for a transient Substrate failure,
// for example the runsc delete exit 128 seen after an ax worker pod
// OOMKill (ax-tzx).
func (s *deletionControlServer) DeleteActor(_ context.Context, request *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
	s.mu.Lock()
	s.attempts++
	fail := s.remainingFails > 0
	if fail {
		s.remainingFails--
	}
	s.mu.Unlock()
	if fail {
		return nil, status.Error(codes.Unavailable, "transient delete failure")
	}
	s.deletions <- request
	return &ateapipb.Actor{}, nil
}

// Attempts reports the number of DeleteActor calls received so far.
func (s *deletionControlServer) Attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func (s *deletionControlServer) ListActorTemplates(_ context.Context, request *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	s.templates <- request
	return &ateapipb.ListActorTemplatesResponse{}, nil
}

func TestMissingTaskDeletionPreservesOnlyLegacyCleanup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	control := &deletionControlServer{deletions: make(chan *ateapipb.DeleteActorRequest, 4), templates: make(chan *ateapipb.ListActorTemplatesRequest, 4)}
	server := grpc.NewServer()
	ateapipb.RegisterControlServer(server, control)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	client, err := substrate.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	worker := &Worker{store: memory.NewStore(), reconciler: NewTaskReconciler(client, "default-template", "ax-system")}
	created := time.Now().UTC().Format(time.RFC3339Nano)
	for _, test := range []struct {
		name         string
		precondition store.TaskDeletePreconditions
		wantConflict bool
	}{
		{"protected", store.TaskDeletePreconditions{ActorUID: "original-actor", CreationTimestamp: created}, false},
		{"actor-only", store.TaskDeletePreconditions{ActorUID: "original-actor"}, true},
		{"timestamp-only", store.TaskDeletePreconditions{CreationTimestamp: created}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := worker.processEvent(context.Background(), store.TaskEvent{Atespace: "private", Name: "missing", Action: "delete", Deletion: test.precondition})
			if test.wantConflict && !errors.Is(err, store.ErrConflict) || !test.wantConflict && err != nil {
				t.Fatalf("unexpected missing-task result: %v", err)
			}
			if len(control.deletions) != 0 || len(control.templates) != 0 {
				t.Fatal("bound missing-task event reached substrate")
			}
		})
	}
	if err := worker.processEvent(context.Background(), store.TaskEvent{Atespace: "private", Name: "missing", Action: "delete"}); err != nil {
		t.Fatal(err)
	}
	if len(control.deletions) != 1 || len(control.templates) != 1 {
		t.Fatalf("legacy cleanup skipped: actor calls=%d template calls=%d", len(control.deletions), len(control.templates))
	}
	request := <-control.deletions
	if request.GetActor().GetAtespace() != "private" || request.GetActor().GetName() != "missing" || request.GetOptions().GetUid() != "" {
		t.Fatalf("unexpected legacy actor deletion: %v", request)
	}
	if request := <-control.templates; request.GetAtespace() != "private" {
		t.Fatalf("unexpected legacy template lookup: %v", request)
	}
}

// AC1 (ax-8qz.2): when ReconcileDelete fails, the worker must retry the same
// delete after a backoff, instead of dropping it. The retry must keep the
// original ActorUID and CreationTimestamp. Once the retry succeeds, the
// worker must remove the task record.
func TestDeleteRetryAfterFailureRemovesTaskOnEventualSuccess(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	control := &deletionControlServer{
		deletions:      make(chan *ateapipb.DeleteActorRequest, 4),
		templates:      make(chan *ateapipb.ListActorTemplatesRequest, 4),
		remainingFails: 2,
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

	ctx := context.Background()
	s := memory.NewStore()
	created := time.Now().UTC()
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "flaky", Atespace: "private", CreationTimestamp: timestamppb.New(created)}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	sub, err := s.Subscribe(ctx, "test-group", "test-consumer")
	if err != nil {
		t.Fatal(err)
	}
	// Drain the reconcile event SaveTask published, so only delete events follow.
	drainCtx, drainCancel := context.WithTimeout(ctx, time.Second)
	if _, err := sub.Next(drainCtx); err != nil {
		t.Fatal(err)
	}
	drainCancel()

	deletion := store.TaskDeletePreconditions{ActorUID: "actor-1", CreationTimestamp: created.Format(time.RFC3339Nano)}
	if err := s.MarkTaskDeleting(ctx, "private", "flaky", deletion); err != nil {
		t.Fatal(err)
	}

	worker := NewWorker(s, NewTaskReconciler(client, "default-template", "ax-system"), "", "")
	worker.DeleteBackoffBase = 10 * time.Millisecond
	worker.DeleteBackoffMax = 40 * time.Millisecond

	// Drive the retry pump by hand: read each delete event as it is
	// published and feed it to processEvent, exactly as Run's loop would.
	for round := 0; round < 3; round++ {
		readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
		ev, err := sub.Next(readCtx)
		readCancel()
		if err != nil {
			t.Fatalf("round %d: no delete event delivered: %v", round, err)
		}
		if err := worker.processEvent(context.Background(), ev); err != nil {
			t.Fatalf("round %d: processEvent error: %v", round, err)
		}
	}

	if _, err := s.GetTask(ctx, "private", "flaky"); err == nil {
		t.Fatal("task record not removed after eventual delete success")
	}
	if got := control.Attempts(); got != 3 {
		t.Fatalf("want 3 DeleteActor attempts (2 failures + 1 success), got %d", got)
	}
	select {
	case request := <-control.deletions:
		if request.GetOptions().GetUid() != "actor-1" {
			t.Fatalf("retry lost the original ActorUID: %v", request)
		}
	default:
		t.Fatal("no successful deletion request recorded")
	}
}

// AC2 (ax-8qz.2): a retried delete event for a task that still exists but is
// no longer Terminating (it was recreated, or another delivery already
// finished it) must make 0 Substrate calls and keep the record untouched.
func TestDeleteRetrySkipsStaleEventForRecreatedTask(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "recreated", Atespace: "private"},
		Status:   &v1alpha1.TaskStatus{Phase: "Running"},
	}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskStatus(ctx, "private", "recreated", task.Status); err != nil {
		t.Fatal(err)
	}

	// worker.reconciler is left nil on purpose: reaching it would panic, so
	// a pass proves the skip happens before any Substrate call.
	worker := &Worker{store: s}
	ev := store.TaskEvent{Atespace: "private", Name: "recreated", Action: "delete", Retry: true}
	if err := worker.processEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}

	current, err := s.GetTask(ctx, "private", "recreated")
	if err != nil {
		t.Fatal(err)
	}
	if current.GetStatus().GetPhase() != "Running" {
		t.Fatalf("stale retry changed the recreated task: phase %q", current.GetStatus().GetPhase())
	}
}

// AC1 (ax-8qz.2): a successful delete must clear the retry backoff, so a
// later delete of a same-named task starts its own schedule at
// DeleteBackoffBase instead of continuing a stale exponential climb.
func TestDeleteBackoffClearedOnSuccessRestartsSchedule(t *testing.T) {
	ctx := context.Background()
	w := &Worker{
		store:             memory.NewStore(),
		DeleteBackoffBase: 10 * time.Millisecond,
		DeleteBackoffMax:  10 * time.Second,
	}

	w.scheduleDeleteRetry(ctx, "private", "job", store.TaskDeletePreconditions{}, errors.New("boom"))
	w.deleteMu.Lock()
	attempt := w.deleteAttempts["private/job"]
	w.deleteMu.Unlock()
	if attempt != 1 {
		t.Fatalf("want attempt count 1 after the first failure, got %d", attempt)
	}

	w.clearDeleteBackoff("private", "job")

	w.scheduleDeleteRetry(ctx, "private", "job", store.TaskDeletePreconditions{}, errors.New("boom again"))
	w.deleteMu.Lock()
	attempt = w.deleteAttempts["private/job"]
	w.deleteMu.Unlock()
	if attempt != 1 {
		t.Fatalf("want attempt count reset to 1 after clearDeleteBackoff, got %d", attempt)
	}
}

// AC3 (ax-8qz.3): the startup sweep must requeue a delete for every
// Terminating non-supervisor task, skip a Terminating supervisor task (its
// preconditions are not stored on the record), and leave a Running task
// alone.
func TestRequeueStrandedDeletionsOnStart(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()

	terminating := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "terminating", Atespace: "default"}}
	supervised := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "supervised", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{SupervisorProfile: "fabriek-native-v1"},
	}
	running := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "running", Atespace: "default"}}

	for _, task := range []*v1alpha1.Task{terminating, supervised, running} {
		if err := s.SaveTask(ctx, task); err != nil {
			t.Fatal(err)
		}
	}

	sub, err := s.Subscribe(ctx, "sweep-test-group", "sweep-test-consumer")
	if err != nil {
		t.Fatal(err)
	}
	// Drain the 3 reconcile events SaveTask published.
	for i := 0; i < 3; i++ {
		drainCtx, drainCancel := context.WithTimeout(ctx, time.Second)
		_, err := sub.Next(drainCtx)
		drainCancel()
		if err != nil {
			t.Fatalf("draining SaveTask event %d: %v", i, err)
		}
	}

	if err := s.UpdateTaskStatus(ctx, "default", "terminating", &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseTerminating}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskStatus(ctx, "default", "supervised", &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseTerminating}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskStatus(ctx, "default", "running", &v1alpha1.TaskStatus{Phase: "Running"}); err != nil {
		t.Fatal(err)
	}

	worker := &Worker{store: s}
	worker.requeueStrandedDeletions(ctx)

	readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
	ev, err := sub.Next(readCtx)
	readCancel()
	if err != nil {
		t.Fatalf("expected exactly one requeued delete event: %v", err)
	}
	if ev.Action != "delete" || ev.Atespace != "default" || ev.Name != "terminating" || !ev.Retry {
		t.Fatalf("unexpected sweep event: %+v", ev)
	}

	noMoreCtx, noMoreCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err = sub.Next(noMoreCtx)
	noMoreCancel()
	if err == nil {
		t.Fatal("sweep requeued more than the one eligible task")
	}

	if _, err := s.GetTask(ctx, "default", "supervised"); err != nil {
		t.Fatal("supervised task record lost")
	}
	if _, err := s.GetTask(ctx, "default", "running"); err != nil {
		t.Fatal("running task record lost")
	}
}
