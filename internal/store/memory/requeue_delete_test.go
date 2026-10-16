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

package memory

import (
	"context"
	"testing"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// RequeueTaskDeletion must publish a delete event that carries both the
// given preconditions and Retry=true, and must not touch the stored task
// record: GetTask must return the same phase as before the call.
func TestRequeueTaskDeletionCarriesPreconditionsAndRetryWithoutRewritingTask(t *testing.T) {
	ctx := context.Background()
	s := NewStore()
	if err := s.SaveTask(ctx, &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private"}}); err != nil {
		t.Fatal(err)
	}
	<-s.events
	if err := s.UpdateTaskStatus(ctx, "private", "job", &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseTerminating}); err != nil {
		t.Fatal(err)
	}

	preconditions := store.TaskDeletePreconditions{ActorUID: "actor-1", CreationTimestamp: "2026-10-15T18:00:00Z"}
	if err := s.RequeueTaskDeletion(ctx, "private", "job", preconditions); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-s.events:
		if ev.Action != "delete" {
			t.Fatalf("want action delete, got %q", ev.Action)
		}
		if ev.Deletion != preconditions {
			t.Fatalf("lost preconditions: %+v", ev.Deletion)
		}
		if !ev.Retry {
			t.Fatal("event not marked Retry")
		}
	default:
		t.Fatal("no event published")
	}

	current, err := s.GetTask(ctx, "private", "job")
	if err != nil {
		t.Fatal(err)
	}
	if current.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatalf("RequeueTaskDeletion changed the stored task: phase %q", current.GetStatus().GetPhase())
	}
}
