package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
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

// ax-bsl.9: UpdateTaskSuspend must not touch the stored status at all, so a
// SuspendTask or ResumeTask call that read the task before a concurrent
// MarkTaskDeleting call cannot replay a stale "Running" status over the new
// Terminating phase.
func TestUpdateTaskSuspendKeepsTerminatingForOrdinaryTask(t *testing.T) {
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

	updated, err := s.UpdateTaskSuspend(ctx, "", "ordinary", true)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.GetSpec().GetSuspend() {
		t.Fatal("UpdateTaskSuspend did not set Spec.Suspend")
	}
	if updated.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatalf("UpdateTaskSuspend cleared Terminating for an ordinary task: phase=%q", updated.GetStatus().GetPhase())
	}

	current, err := s.GetTask(ctx, "", "ordinary")
	if err != nil || current.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatalf("UpdateTaskSuspend cleared Terminating for an ordinary task: %v, phase=%q", err, current.GetStatus().GetPhase())
	}
}

// ax-bsl.11: UpdateTaskSuspend must emit a reconcile TaskEvent, the same way
// SaveTask does. Without it, the worker never reconciles the task, and
// SuspendActor/ResumeActor never run: Spec.Suspend changes but the actor
// keeps running or stays suspended until some unrelated event arrives.
func TestUpdateTaskSuspendEmitsReconcileEvent(t *testing.T) {
	ctx := context.Background()
	s := NewStore()
	if err := s.SaveTask(ctx, &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "ordinary"}}); err != nil {
		t.Fatal(err)
	}
	<-s.events // drain SaveTask's own reconcile event

	if _, err := s.UpdateTaskSuspend(ctx, "", "ordinary", true); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-s.events:
		if ev.Action != "reconcile" || ev.Name != "ordinary" {
			t.Fatalf("UpdateTaskSuspend emitted the wrong event: %+v", ev)
		}
	default:
		t.Fatal("UpdateTaskSuspend emitted no reconcile event")
	}

	select {
	case ev := <-s.events:
		t.Fatalf("UpdateTaskSuspend emitted a second event: %+v", ev)
	default:
	}
}

func drainMemoryEvents(s *MemoryStore) int {
	n := 0
	for {
		select {
		case <-s.events:
			n++
		default:
			return n
		}
	}
}

// TestSaveTaskRejectsTerminating checks that SaveTask refuses to replace a
// Terminating task. The check holds for an ordinary task and a supervisor.
func TestSaveTaskRejectsTerminating(t *testing.T) {
	ctx := context.Background()
	profiles := []struct {
		name    string
		profile string
	}{
		{"ordinary", ""},
		{"supervisor", v1alpha1.FabriekSupervisorProfile},
	}
	statuses := []struct {
		name   string
		status *v1alpha1.TaskStatus
	}{
		{"nil", nil},
		{"pending", &v1alpha1.TaskStatus{Phase: "Pending"}},
		{"suspended", &v1alpha1.TaskStatus{Phase: "Suspended"}},
		{"running", &v1alpha1.TaskStatus{Phase: "Running"}},
	}
	for _, profile := range profiles {
		for _, incoming := range statuses {
			t.Run(profile.name+"/"+incoming.name, func(t *testing.T) {
				s := NewStore()
				created := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
				stored := &v1alpha1.Task{
					Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private", CreationTimestamp: timestamppb.New(created)},
					Spec:     &v1alpha1.TaskSpec{Image: "ghcr.io/example/app:a", SupervisorProfile: profile.profile},
					Status:   &v1alpha1.TaskStatus{Phase: "Running"},
				}
				if err := s.SaveTask(ctx, stored); err != nil {
					t.Fatal(err)
				}
				if profile.profile == "" {
					if err := s.MarkTaskDeleting(ctx, "private", "job"); err != nil {
						t.Fatal(err)
					}
				} else {
					pre := store.TaskDeletePreconditions{ActorUID: "actor-uid", CreationTimestamp: created.Format(time.RFC3339Nano)}
					if err := s.MarkTaskDeleting(ctx, "private", "job", pre); err != nil {
						t.Fatal(err)
					}
				}
				if drainMemoryEvents(s) == 0 {
					t.Fatal("setup emitted no event")
				}
				before, err := s.GetTask(ctx, "private", "job")
				if err != nil {
					t.Fatal(err)
				}
				replacement := &v1alpha1.Task{
					Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private"},
					Spec: &v1alpha1.TaskSpec{
						Image:             "ghcr.io/example/app:b",
						Suspend:           true,
						SupervisorProfile: profile.profile,
					},
					Status: incoming.status,
				}
				if err := s.SaveTask(ctx, replacement); !errors.Is(err, store.ErrConflict) {
					t.Fatalf("err %v", err)
				}
				after, err := s.GetTask(ctx, "private", "job")
				if err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(before, after) {
					t.Fatalf("stored task changed\nbefore %v\nafter  %v", before, after)
				}
				if drainMemoryEvents(s) != 0 {
					t.Fatal("rejected SaveTask emitted an event")
				}
			})
		}
	}

	t.Run("create", func(t *testing.T) {
		s := NewStore()
		if err := s.SaveTask(ctx, &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "new", Atespace: "private"}}); err != nil {
			t.Fatal(err)
		}
		got, err := s.GetTask(ctx, "private", "new")
		if err != nil || got.GetStatus().GetPhase() != "Pending" {
			t.Fatalf("create: %v phase %q", err, got.GetStatus().GetPhase())
		}
	})

	t.Run("update live", func(t *testing.T) {
		s := NewStore()
		if err := s.SaveTask(ctx, &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private"},
			Spec:     &v1alpha1.TaskSpec{Image: "ghcr.io/example/app:a"},
			Status:   &v1alpha1.TaskStatus{Phase: "Running"},
		}); err != nil {
			t.Fatal(err)
		}
		drainMemoryEvents(s)
		if err := s.SaveTask(ctx, &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private"},
			Spec:     &v1alpha1.TaskSpec{Image: "ghcr.io/example/app:b"},
			Status:   &v1alpha1.TaskStatus{Phase: "Running"},
		}); err != nil {
			t.Fatal(err)
		}
		got, err := s.GetTask(ctx, "private", "job")
		if err != nil || got.GetSpec().GetImage() != "ghcr.io/example/app:b" || got.GetStatus().GetPhase() != "Running" {
			t.Fatalf("update: %v image %q phase %q", err, got.GetSpec().GetImage(), got.GetStatus().GetPhase())
		}
		if drainMemoryEvents(s) != 1 {
			t.Fatal("live update emitted no reconcile event")
		}
	})
}
