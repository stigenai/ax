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

package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	redisclient "github.com/redis/go-redis/v9"
)

func newUpdateIfTestStore(t *testing.T) (*Store, context.Context, func()) {
	t.Helper()
	address := os.Getenv("AX_DELETE_TEST_REDIS_ADDRESS")
	if address == "" {
		t.Skip("requires explicit disposable AX_DELETE_TEST_REDIS_ADDRESS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	if err := client.Ping(ctx).Err(); err != nil {
		cancel()
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("ax-updateif-test-%d", time.Now().UnixNano())
	s := NewStore(client, Options{KeyPrefix: prefix, StreamName: prefix + ":events"})
	return s, ctx, func() {
		cancel()
		client.Close()
	}
}

// TestUpdateTaskStatusIfSkipsWriteWhenExpectFalse proves a false expect
// leaves the stored task untouched and publishes no pubsub message, so a
// stale caller cannot overwrite a status change it never observed.
func TestUpdateTaskStatusIfSkipsWriteWhenExpectFalse(t *testing.T) {
	s, ctx, cleanup := newUpdateIfTestStore(t)
	defer cleanup()

	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "task1", Atespace: "default"}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskStatus(ctx, "default", "task1", &v1alpha1.TaskStatus{Phase: "Running"}); err != nil {
		t.Fatal(err)
	}

	watchCh, closer, err := s.WatchTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	time.Sleep(200 * time.Millisecond) // let the subscription establish before the write

	applied, err := s.UpdateTaskStatusIf(ctx, "default", "task1",
		func(cur *v1alpha1.TaskStatus) bool { return cur.GetPhase() == "Suspended" },
		&v1alpha1.TaskStatus{Phase: "Failed"},
	)
	if err != nil {
		t.Fatalf("UpdateTaskStatusIf returned an error: %v", err)
	}
	if applied {
		t.Fatal("UpdateTaskStatusIf applied the write although expect returned false")
	}

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("task phase is %q, want unchanged %q", got.GetStatus().GetPhase(), "Running")
	}

	select {
	case <-watchCh:
		t.Fatal("UpdateTaskStatusIf published a pubsub message although expect returned false")
	case <-time.After(500 * time.Millisecond):
	}
}

// TestUpdateTaskStatusIfAppliesWriteWhenExpectTrue proves a true expect
// writes the new status and publishes a pubsub message, like
// UpdateTaskStatus does.
func TestUpdateTaskStatusIfAppliesWriteWhenExpectTrue(t *testing.T) {
	s, ctx, cleanup := newUpdateIfTestStore(t)
	defer cleanup()

	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "task1", Atespace: "default"}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTaskStatus(ctx, "default", "task1", &v1alpha1.TaskStatus{Phase: "Running"}); err != nil {
		t.Fatal(err)
	}

	watchCh, closer, err := s.WatchTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	time.Sleep(200 * time.Millisecond) // let the subscription establish before the write

	applied, err := s.UpdateTaskStatusIf(ctx, "default", "task1",
		func(cur *v1alpha1.TaskStatus) bool { return cur.GetPhase() == "Running" },
		&v1alpha1.TaskStatus{Phase: "Failed", Conditions: []*v1alpha1.Condition{
			{Type: "Ready", Status: "False", Reason: "ActorLost", Message: "actor lost its worker"},
		}},
	)
	if err != nil {
		t.Fatalf("UpdateTaskStatusIf returned an error: %v", err)
	}
	if !applied {
		t.Fatal("UpdateTaskStatusIf did not apply the write although expect returned true")
	}

	got, err := s.GetTask(ctx, "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Failed" {
		t.Fatalf("task phase is %q, want %q", got.GetStatus().GetPhase(), "Failed")
	}

	select {
	case notified := <-watchCh:
		if notified.GetStatus().GetPhase() != "Failed" {
			t.Fatalf("watcher saw phase %q, want %q", notified.GetStatus().GetPhase(), "Failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("UpdateTaskStatusIf did not publish a pubsub message after applying the write")
	}
}

// TestUpdateTaskStatusIfMissingTaskReturnsErrNotFound proves a conditional
// write on a task that does not exist fails clearly, instead of silently
// reporting applied=false.
func TestUpdateTaskStatusIfMissingTaskReturnsErrNotFound(t *testing.T) {
	s, ctx, cleanup := newUpdateIfTestStore(t)
	defer cleanup()

	_, err := s.UpdateTaskStatusIf(ctx, "default", "missing",
		func(*v1alpha1.TaskStatus) bool { return true },
		&v1alpha1.TaskStatus{Phase: "Failed"},
	)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("got error %v, want store.ErrNotFound", err)
	}
}
