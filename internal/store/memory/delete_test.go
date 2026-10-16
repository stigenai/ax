package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProtectedDeleteRetainsExactIncarnation(t *testing.T) {
	ctx := context.Background()
	s := NewStore()
	created := time.Date(2026, 10, 14, 19, 0, 0, 123, time.UTC)
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "supervisor", Atespace: "private", CreationTimestamp: timestamppb.New(created)},
		Spec:     &v1alpha1.TaskSpec{SupervisorProfile: "fabriek-native-v1"},
	}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	<-s.events
	valid := store.TaskDeletePreconditions{ActorUID: "original-actor", CreationTimestamp: created.Format(time.RFC3339Nano)}
	for _, invalid := range []store.TaskDeletePreconditions{
		{},
		{ActorUID: valid.ActorUID},
		{CreationTimestamp: valid.CreationTimestamp},
		{ActorUID: valid.ActorUID, CreationTimestamp: "invalid"},
		{ActorUID: valid.ActorUID, CreationTimestamp: created.Add(time.Nanosecond).Format(time.RFC3339Nano)},
	} {
		if err := s.MarkTaskDeleting(ctx, "private", "supervisor", invalid); !errors.Is(err, store.ErrConflict) {
			t.Fatalf("invalid binding accepted: %v", err)
		}
		if len(s.events) != 0 {
			t.Fatal("denied delete emitted event")
		}
	}
	if err := s.MarkTaskDeleting(ctx, "private", "supervisor", valid); err != nil {
		t.Fatal(err)
	}
	ev := <-s.events
	if ev.Deletion != valid || ev.Action != "delete" {
		t.Fatalf("lost delete identity: %+v", ev)
	}
	if err := s.UpdateTaskStatus(ctx, "private", "supervisor", &v1alpha1.TaskStatus{Phase: "Running"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveTask(ctx, task); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("update cleared deletion: %v", err)
	}
	retained, err := s.GetTask(ctx, "private", "supervisor")
	if err != nil || retained.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatal("stale reconcile cleared deletion")
	}
	if err := s.DeleteTask(ctx, "private", "supervisor", valid); err != nil {
		t.Fatal(err)
	}
	task.Metadata.CreationTimestamp = timestamppb.New(created.Add(time.Second))
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteTask(ctx, "private", "supervisor", ev.Deletion); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("old event deleted replacement: %v", err)
	}
	if _, err := s.GetTask(ctx, "private", "supervisor"); err != nil {
		t.Fatal("replacement lost")
	}
}

func TestOrdinaryDeleteKeepsExistingBehavior(t *testing.T) {
	ctx := context.Background()
	s := NewStore()
	if err := s.SaveTask(ctx, &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "ordinary"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkTaskDeleting(ctx, "", "ordinary"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteTask(ctx, "", "ordinary"); err != nil {
		t.Fatal(err)
	}
}

// ax-bsl.7: UpdateTaskStatus must keep Terminating for an ordinary task too,
// not only a supervisor task. A stale write that still thinks the task is
// Running must not clear Terminating.
func TestOrdinaryUpdateTaskStatusKeepsTerminating(t *testing.T) {
	ctx := context.Background()
	s := NewStore()
	if err := s.SaveTask(ctx, &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "ordinary"}}); err != nil {
		t.Fatal(err)
	}
	<-s.events
	if err := s.MarkTaskDeleting(ctx, "", "ordinary"); err != nil {
		t.Fatal(err)
	}
	<-s.events

	if err := s.UpdateTaskStatus(ctx, "", "ordinary", &v1alpha1.TaskStatus{Phase: "Running"}); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetTask(ctx, "", "ordinary")
	if err != nil || current.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatalf("UpdateTaskStatus cleared Terminating for an ordinary task: %v, phase=%q", err, current.GetStatus().GetPhase())
	}

	applied, err := s.UpdateTaskStatusIf(ctx, "", "ordinary",
		func(*v1alpha1.TaskStatus) bool { return true },
		&v1alpha1.TaskStatus{Phase: "Running"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("UpdateTaskStatusIf cleared Terminating for an ordinary task")
	}
	current, err = s.GetTask(ctx, "", "ordinary")
	if err != nil || current.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatalf("UpdateTaskStatusIf cleared Terminating for an ordinary task: %v, phase=%q", err, current.GetStatus().GetPhase())
	}
}
