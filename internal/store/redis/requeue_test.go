package redis

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
	redisclient "github.com/redis/go-redis/v9"
)

// RequeueTask must publish a new reconcile event without rewriting the
// stored task record, so a worker that reads the stream reconciles the
// task again while GetTask still returns the untouched status.
func TestRequeueTaskRedisPublishesReconcileEventWithoutRewritingTask(t *testing.T) {
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
	prefix := fmt.Sprintf("ax-requeue-test-%d", time.Now().UnixNano())
	s := NewStore(client, Options{KeyPrefix: prefix, StreamName: prefix + ":events"})

	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private"},
		Spec:     &v1alpha1.TaskSpec{SupervisorProfile: "fabriek-native-v1"},
	}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskStatus(ctx, "private", "job", &v1alpha1.TaskStatus{
		Phase: "Pending",
		Conditions: []*v1alpha1.Condition{
			{Type: "Ready", Status: "False", Reason: "WaitingForCapacity"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.RequeueTask(ctx, "private", "job"); err != nil {
		t.Fatal(err)
	}

	events, err := client.XRange(ctx, s.opts.StreamName, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("want 1 SaveTask event + 1 RequeueTask event, got %d", len(events))
	}
	ev := eventFromMessage(events[1])
	if ev.Action != "reconcile" || ev.Atespace != "private" || ev.Name != "job" {
		t.Fatalf("requeue event malformed: %+v", ev)
	}

	current, err := s.GetTask(ctx, "private", "job")
	if err != nil {
		t.Fatal(err)
	}
	if current.GetStatus().GetPhase() != "Pending" {
		t.Fatalf("RequeueTask rewrote the stored task: phase %q", current.GetStatus().GetPhase())
	}
	found := false
	for _, c := range current.GetStatus().GetConditions() {
		if c.GetType() == "Ready" && c.GetReason() == "WaitingForCapacity" {
			found = true
		}
	}
	if !found {
		t.Fatal("RequeueTask lost the WaitingForCapacity condition")
	}

	// RequeueTask on a task that no longer exists must not fabricate one.
	if err := s.RequeueTask(ctx, "private", "missing"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetTask(ctx, "private", "missing"); err == nil {
		t.Fatal("RequeueTask created a task record for a missing task")
	}
}
