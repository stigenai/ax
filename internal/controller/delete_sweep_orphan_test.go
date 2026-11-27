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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// writeSpyStore counts every write call, to prove the orphan report writes
// nothing (ax-vnr.3 AC5).
type writeSpyStore struct {
	store.Store
	mu     sync.Mutex
	writes int
}

func (s *writeSpyStore) hit() {
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
}

func (s *writeSpyStore) SaveTask(ctx context.Context, t *v1alpha1.Task) error {
	s.hit()
	return s.Store.SaveTask(ctx, t)
}

func (s *writeSpyStore) UpdateTaskStatus(ctx context.Context, a, n string, st *v1alpha1.TaskStatus) error {
	s.hit()
	return s.Store.UpdateTaskStatus(ctx, a, n, st)
}

func (s *writeSpyStore) UpdateTaskStatusIf(ctx context.Context, a, n string, e func(*v1alpha1.TaskStatus) bool, st *v1alpha1.TaskStatus) (bool, error) {
	s.hit()
	return s.Store.UpdateTaskStatusIf(ctx, a, n, e, st)
}

func (s *writeSpyStore) UpdateTaskSuspend(ctx context.Context, a, n string, b bool) (*v1alpha1.Task, error) {
	s.hit()
	return s.Store.UpdateTaskSuspend(ctx, a, n, b)
}

func (s *writeSpyStore) MarkTaskDeleting(ctx context.Context, a, n string, p ...store.TaskDeletePreconditions) error {
	s.hit()
	return s.Store.MarkTaskDeleting(ctx, a, n, p...)
}

func (s *writeSpyStore) DeleteTask(ctx context.Context, a, n string, p ...store.TaskDeletePreconditions) error {
	s.hit()
	return s.Store.DeleteTask(ctx, a, n, p...)
}

func (s *writeSpyStore) RequeueTask(ctx context.Context, a, n string) error {
	s.hit()
	return s.Store.RequeueTask(ctx, a, n)
}

func (s *writeSpyStore) RequeueTaskDeletion(ctx context.Context, a, n string, p store.TaskDeletePreconditions) error {
	s.hit()
	return s.Store.RequeueTaskDeletion(ctx, a, n, p)
}

func (s *writeSpyStore) SaveWorkspace(ctx context.Context, w *v1alpha1.Workspace) error {
	s.hit()
	return s.Store.SaveWorkspace(ctx, w)
}

func (s *writeSpyStore) DeleteWorkspace(ctx context.Context, a, n string) error {
	s.hit()
	return s.Store.DeleteWorkspace(ctx, a, n)
}

func (s *writeSpyStore) SaveModel(ctx context.Context, m *v1alpha1.Model) error {
	s.hit()
	return s.Store.SaveModel(ctx, m)
}

func (s *writeSpyStore) DeleteModel(ctx context.Context, a, n string) error {
	s.hit()
	return s.Store.DeleteModel(ctx, a, n)
}

// hiddenTaskStore lists no task but still serves GetTask. It stands in for
// the Redis ListTasks that drops an indexed task whose value it cannot
// read, so a short page hides a task that exists (ax-vnr.12).
type hiddenTaskStore struct {
	store.Store
	getErr error
}

func (s *hiddenTaskStore) ListTasks(context.Context, string, int64, int64) ([]*v1alpha1.Task, error) {
	return nil, nil
}

// listedNotGettableStore lists tasks but GetTask says ErrNotFound. Only the
// task-key lookup in isOrphanActor then stops the orphan report.
type listedNotGettableStore struct{ store.Store }

func (s *listedNotGettableStore) GetTask(context.Context, string, string) (*v1alpha1.Task, error) {
	return nil, store.ErrNotFound
}

func (s *hiddenTaskStore) GetTask(ctx context.Context, a, n string) (*v1alpha1.Task, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.Store.GetTask(ctx, a, n)
}

// orphanActor returns an AX-shaped actor: running, template name matching
// taskTemplatePattern, 1 hour old. A test changes the one field it probes.
func orphanActor(now time.Time, atespace, name string) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name, Uid: "uid-" + name, CreateTime: timestamppb.New(now.Add(-time.Hour))},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: name + "-tmpl-0123abcd"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPod: "worker-1"}},
	}
}

// TestDeleteSweepOrphanActors covers ax-vnr.3 AC4 and AC5: an AX actor with
// no task record in any phase counts as an orphan only when every rule
// item holds, and the report writes nothing.
func TestDeleteSweepOrphanActors(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name      string
		actor     func(a *ateapipb.Actor)
		task      string // phase of a task named orphan; "" for none
		taskSpace string // atespace of that task; "" means factory
		listErr   bool   // ListTasks fails
		actorErr  bool   // ListActors fails
		hidden    bool   // ListTasks hides the task; GetTask finds it
		unGet     bool   // ListTasks lists the task; GetTask says ErrNotFound
		getErr    error  // GetTask fails (with hidden, or with no task)
		want      int
	}{
		{name: "running orphan", want: 1},
		{name: "suspended orphan", actor: func(a *ateapipb.Actor) { a.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED }, want: 1},
		{name: "task Running", task: "Running", want: 0},
		{name: "task Terminating", task: v1alpha1.PhaseTerminating, want: 0},
		{name: "task Failed", task: "Failed", want: 0},
		{name: "same name in another atespace", task: "Running", taskSpace: "other", want: 1},
		{name: "deleting actor", actor: func(a *ateapipb.Actor) { a.Status.State = ateapipb.ActorState_ACTOR_STATE_DELETING }, want: 0},
		{name: "foreign template", actor: func(a *ateapipb.Actor) { a.ActorTemplate.Name = "other-tmpl-0123abcd" }, want: 0},
		{name: "bad digest", actor: func(a *ateapipb.Actor) { a.ActorTemplate.Name = "orphan-tmpl-xyz" }, want: 0},
		{name: "template in another atespace", actor: func(a *ateapipb.Actor) { a.ActorTemplate.Atespace = "other" }, want: 0},
		{name: "nil template", actor: func(a *ateapipb.Actor) { a.ActorTemplate = nil }, want: 0},
		{name: "younger than MinAge", actor: func(a *ateapipb.Actor) { a.Metadata.CreateTime = timestamppb.New(now.Add(-5 * time.Minute)) }, want: 0},
		{name: "exactly MinAge", actor: func(a *ateapipb.Actor) { a.Metadata.CreateTime = timestamppb.New(now.Add(-15 * time.Minute)) }, want: 1},
		{name: "nil create time", actor: func(a *ateapipb.Actor) { a.Metadata.CreateTime = nil }, want: 0},
		{name: "invalid create time", actor: func(a *ateapipb.Actor) { a.Metadata.CreateTime = &timestamppb.Timestamp{Seconds: -1 << 60} }, want: 0},
		{name: "ListTasks error", listErr: true, want: 0},
		{name: "task hidden from a short page", task: "Running", hidden: true, want: 0},
		{name: "listed task, GetTask not found", task: "Running", unGet: true, want: 0},
		{name: "GetTask error", getErr: errors.New("redis down"), want: 0},
		{name: "ListActors error", actorErr: true, want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := memory.NewStore()
			if tc.task != "" {
				space := tc.taskSpace
				if space == "" {
					space = "factory"
				}
				task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "orphan", Atespace: space}, Status: &v1alpha1.TaskStatus{Phase: tc.task}}
				if err := base.SaveTask(context.Background(), task); err != nil {
					t.Fatal(err)
				}
				if err := base.UpdateTaskStatus(context.Background(), space, "orphan", task.Status); err != nil {
					t.Fatal(err)
				}
			}
			var st store.Store = base
			if tc.listErr {
				st = &failOnCallListTasksStore{Store: base, failOnCall: 1}
			}
			if tc.hidden || tc.getErr != nil {
				st = &hiddenTaskStore{Store: st, getErr: tc.getErr}
			}
			if tc.unGet {
				st = &listedNotGettableStore{Store: st}
			}
			spy := &writeSpyStore{Store: st}
			actor := orphanActor(now, "factory", "orphan")
			if tc.actor != nil {
				tc.actor(actor)
			}
			lister := &fakeActorLister{actors: []*ateapipb.Actor{actor}}
			if tc.actorErr {
				lister.err = errors.New("substrate unavailable")
			}
			sweeper := &DeletionSweeper{Store: spy, Actors: lister, MinAge: 15 * time.Minute, Now: func() time.Time { return now }}

			output := captureSweepLogs(t, func() { sweeper.SweepOnce(context.Background()) })
			if want := fmt.Sprintf("orphan_actors=%d", tc.want); !strings.Contains(output, want) {
				t.Fatalf("want %s, got: %s", want, output)
			}
			if got := strings.Count(output, "delete sweep: orphan actor"); got != tc.want {
				t.Fatalf("want %d orphan lines, got %d: %s", tc.want, got, output)
			}
			if tc.want == 1 {
				for _, s := range []string{"atespace=factory", "name=orphan", "uid=uid-orphan", "state=ACTOR_STATE_", "template=orphan-tmpl-0123abcd", "worker_pod=worker-1", "age="} {
					if !strings.Contains(output, s) {
						t.Fatalf("orphan line lacks %s: %s", s, output)
					}
				}
			}
			if strings.Contains(output, "level=WARN msg=\"delete sweep: stuck deletions\"") {
				t.Fatalf("an orphan must not change the stuck level: %s", output)
			}
			if spy.writes != 0 {
				t.Fatalf("the orphan report wrote %d times", spy.writes)
			}
		})
	}
}
