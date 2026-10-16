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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	redisclient "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RequeueTaskDeletion must publish a delete event that carries both the given
// preconditions and retry=1, and must not rewrite the stored task record, so
// a worker that reads the stream retries cleanup while GetTask still returns
// the untouched Terminating record.
func TestRequeueTaskDeletionRedisCarriesPreconditionsAndRetryWithoutRewritingTask(t *testing.T) {
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
	prefix := fmt.Sprintf("ax-requeue-delete-test-%d", time.Now().UnixNano())
	s := NewStore(client, Options{KeyPrefix: prefix, StreamName: prefix + ":events"})

	created := time.Date(2026, 10, 15, 18, 0, 0, 0, time.UTC)
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private", CreationTimestamp: timestamppb.New(created)},
	}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	preconditions := store.TaskDeletePreconditions{ActorUID: "actor-1", CreationTimestamp: created.Format(time.RFC3339Nano)}
	if err := s.MarkTaskDeleting(ctx, "private", "job", preconditions); err != nil {
		t.Fatal(err)
	}

	if err := s.RequeueTaskDeletion(ctx, "private", "job", preconditions); err != nil {
		t.Fatal(err)
	}

	events, err := client.XRange(ctx, s.opts.StreamName, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("want 1 SaveTask event + 1 MarkTaskDeleting event + 1 RequeueTaskDeletion event, got %d", len(events))
	}
	ev := eventFromMessage(events[2])
	if ev.Action != "delete" || ev.Atespace != "private" || ev.Name != "job" {
		t.Fatalf("requeue delete event malformed: %+v", ev)
	}
	if ev.Deletion != preconditions {
		t.Fatalf("requeue delete event lost preconditions: %+v", ev.Deletion)
	}
	if !ev.Retry {
		t.Fatal("requeue delete event not marked Retry")
	}
	// The original MarkTaskDeleting event must not itself read as a retry.
	original := eventFromMessage(events[1])
	if original.Retry {
		t.Fatal("original delete event read back as Retry")
	}

	current, err := s.GetTask(ctx, "private", "job")
	if err != nil {
		t.Fatal(err)
	}
	if current.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatalf("RequeueTaskDeletion changed the stored task: phase %q", current.GetStatus().GetPhase())
	}
}
