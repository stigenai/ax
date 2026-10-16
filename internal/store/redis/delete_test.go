package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	redisclient "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type pauseDeleteRead struct {
	key    string
	once   sync.Once
	read   chan struct{}
	resume chan struct{}
}

func (*pauseDeleteRead) DialHook(next redisclient.DialHook) redisclient.DialHook { return next }
func (*pauseDeleteRead) ProcessPipelineHook(next redisclient.ProcessPipelineHook) redisclient.ProcessPipelineHook {
	return next
}
func (h *pauseDeleteRead) ProcessHook(next redisclient.ProcessHook) redisclient.ProcessHook {
	return func(ctx context.Context, cmd redisclient.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && cmd.Name() == "get" && cmd.Args()[1] == h.key {
			h.once.Do(func() {
				close(h.read)
				select {
				case <-h.resume:
				case <-ctx.Done():
				}
			})
		}
		return err
	}
}

// The integration test uses a dedicated disposable Redis, never a shared database.
func TestProtectedDeleteRedisRetention(t *testing.T) {
	address := os.Getenv("AX_DELETE_TEST_REDIS_ADDRESS")
	if address == "" {
		t.Skip("requires explicit disposable AX_DELETE_TEST_REDIS_ADDRESS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("ax-delete-test-%d", time.Now().UnixNano())
	s := NewStore(client, Options{KeyPrefix: prefix, StreamName: prefix + ":events"})
	created := time.Now().UTC()
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private", CreationTimestamp: timestamppb.New(created)}, Spec: &v1alpha1.TaskSpec{SupervisorProfile: "fabriek-native-v1"}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkTaskDeleting(ctx, "private", "job"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("unbound protected delete accepted: %v", err)
	}
	identity := store.TaskDeletePreconditions{ActorUID: "b8fe2249-1732-4d5a-b539-a394ad10f93d", CreationTimestamp: created.Format(time.RFC3339Nano)}
	if err := s.MarkTaskDeleting(ctx, "private", "job", identity); err != nil {
		t.Fatal(err)
	}
	events, err := client.XRange(ctx, s.opts.StreamName, "-", "+").Result()
	if err != nil || len(events) != 2 {
		t.Fatalf("missing exact event: %d %v", len(events), err)
	}
	ev := eventFromMessage(events[1])
	if ev.Action != "delete" || ev.Deletion != identity {
		t.Fatalf("event lost original identity: %+v", ev)
	}
	// A restarted store reads the immutable event and the retained deletion phase.
	s = NewStore(client, s.opts)
	if err := s.UpdateTaskStatus(ctx, "private", "job", &v1alpha1.TaskStatus{Phase: "Running"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveTask(ctx, task); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale update erased deletion: %v", err)
	}
	current, err := s.GetTask(ctx, "private", "job")
	if err != nil || current.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatal("lost deletion phase")
	}
	if err := s.DeleteTask(ctx, "private", "job", ev.Deletion); err != nil {
		t.Fatal(err)
	}
	task.Metadata.CreationTimestamp = timestamppb.New(created.Add(time.Second))
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteTask(ctx, "private", "job", ev.Deletion); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("old event deleted replacement: %v", err)
	}
	if _, err := s.GetTask(ctx, "private", "job"); err != nil {
		t.Fatal("replacement lost")
	}
}

// ax-bsl.7: UpdateTaskStatus must keep Terminating for an ordinary task
// too, not only a supervisor task. A stale write that still thinks the
// task is Running must not clear Terminating.
func TestOrdinaryUpdateTaskStatusKeepsTerminatingRedis(t *testing.T) {
	address := os.Getenv("AX_DELETE_TEST_REDIS_ADDRESS")
	if address == "" {
		t.Skip("requires explicit disposable AX_DELETE_TEST_REDIS_ADDRESS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("ax-ordinary-terminating-test-%d", time.Now().UnixNano())
	s := NewStore(client, Options{KeyPrefix: prefix, StreamName: prefix + ":events"})
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private"}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkTaskDeleting(ctx, "private", "job"); err != nil {
		t.Fatal(err)
	}

	if err := s.UpdateTaskStatus(ctx, "private", "job", &v1alpha1.TaskStatus{Phase: "Running"}); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetTask(ctx, "private", "job")
	if err != nil || current.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatalf("UpdateTaskStatus cleared Terminating for an ordinary task: %v, phase=%q", err, current.GetStatus().GetPhase())
	}

	applied, err := s.UpdateTaskStatusIf(ctx, "private", "job",
		func(*v1alpha1.TaskStatus) bool { return true },
		&v1alpha1.TaskStatus{Phase: "Running"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("UpdateTaskStatusIf cleared Terminating for an ordinary task")
	}
	current, err = s.GetTask(ctx, "private", "job")
	if err != nil || current.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatalf("UpdateTaskStatusIf cleared Terminating for an ordinary task: %v, phase=%q", err, current.GetStatus().GetPhase())
	}
}

// ax-bsl.9: UpdateTaskSuspend must not touch the stored status at all, so a
// SuspendTask or ResumeTask call that read the task before a concurrent
// MarkTaskDeleting call cannot replay a stale "Running" status over the new
// Terminating phase.
func TestUpdateTaskSuspendKeepsTerminatingForOrdinaryTaskRedis(t *testing.T) {
	address := os.Getenv("AX_DELETE_TEST_REDIS_ADDRESS")
	if address == "" {
		t.Skip("requires explicit disposable AX_DELETE_TEST_REDIS_ADDRESS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("ax-suspend-terminating-test-%d", time.Now().UnixNano())
	s := NewStore(client, Options{KeyPrefix: prefix, StreamName: prefix + ":events"})
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private"}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkTaskDeleting(ctx, "private", "job"); err != nil {
		t.Fatal(err)
	}

	updated, err := s.UpdateTaskSuspend(ctx, "private", "job", true)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.GetSpec().GetSuspend() {
		t.Fatal("UpdateTaskSuspend did not set Spec.Suspend")
	}
	if updated.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatalf("UpdateTaskSuspend cleared Terminating for an ordinary task: phase=%q", updated.GetStatus().GetPhase())
	}

	current, err := s.GetTask(ctx, "private", "job")
	if err != nil || current.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatalf("UpdateTaskSuspend cleared Terminating for an ordinary task: %v, phase=%q", err, current.GetStatus().GetPhase())
	}
}

func TestProtectedDeleteRedisConcurrentReplacement(t *testing.T) {
	address := os.Getenv("AX_DELETE_TEST_REDIS_ADDRESS")
	if address == "" {
		t.Skip("requires explicit disposable AX_DELETE_TEST_REDIS_ADDRESS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	defer client.Close()
	other := redisclient.NewClient(&redisclient.Options{Addr: address})
	defer other.Close()
	prefix := fmt.Sprintf("ax-delete-race-%d", time.Now().UnixNano())
	s := NewStore(client, Options{KeyPrefix: prefix, StreamName: prefix + ":events"})
	created := time.Now().UTC()
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private", CreationTimestamp: timestamppb.New(created)}, Spec: &v1alpha1.TaskSpec{SupervisorProfile: "fabriek-native-v1"}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	hook := &pauseDeleteRead{key: s.taskKey("private", "job"), read: make(chan struct{}), resume: make(chan struct{})}
	client.AddHook(hook)
	result := make(chan error, 1)
	go func() {
		result <- s.MarkTaskDeleting(ctx, "private", "job", store.TaskDeletePreconditions{ActorUID: "b8fe2249-1732-4d5a-b539-a394ad10f93d", CreationTimestamp: created.Format(time.RFC3339Nano)})
	}()
	select {
	case <-hook.read:
	case <-ctx.Done():
		t.Fatal("delete did not reach watched read")
	}
	// Replace the record after WATCH/read but before its transaction can commit.
	task.Metadata.CreationTimestamp = timestamppb.New(created.Add(time.Second))
	replacement := NewStore(other, s.opts)
	if err := replacement.SaveTask(ctx, task); err != nil {
		close(hook.resume)
		t.Fatal(err)
	}
	close(hook.resume)
	if err := <-result; !errors.Is(err, redisclient.TxFailedErr) {
		t.Fatalf("conflict did not surface: %v", err)
	}
	current, err := replacement.GetTask(ctx, "private", "job")
	if err != nil || !current.GetMetadata().GetCreationTimestamp().AsTime().Equal(created.Add(time.Second)) || current.GetStatus().GetPhase() == v1alpha1.PhaseTerminating {
		t.Fatal("concurrent replacement overwritten")
	}
	events, err := other.XRange(ctx, s.opts.StreamName, "-", "+").Result()
	if err != nil || len(events) != 2 {
		t.Fatalf("unexpected event count: %d %v", len(events), err)
	}
	for _, event := range events {
		if eventFromMessage(event).Action == "delete" {
			t.Fatal("failed transaction published deletion")
		}
	}
}
