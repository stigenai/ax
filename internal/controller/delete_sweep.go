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
	"log/slog"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// DefaultDeleteSweepMinAge is the age DeletionSweeper.MinAge uses when left
// zero: ax-706's postmortem saw actors stuck in ACTOR_STATE_DELETING for
// hours, so 15 minutes gives a real stall time to clear on its own (a
// transient Substrate failure plus one backoff retry) before the sweep
// counts and acts on it.
const DefaultDeleteSweepMinAge = 15 * time.Minute

// ActorLister lists every Substrate Actor in an atespace, paging
// internally. *substrate.Client implements it with ListActors. A sweep
// takes this narrow interface, not the full Substrate client, so it has no
// way to call DeleteActor on an actor it only read (ax-706 DESIGN item 3:
// that step stays with a person, who has the UID and ownership context the
// sweep does not).
type ActorLister interface {
	ListActors(ctx context.Context, atespace string) ([]*ateapipb.Actor, error)
}

// seenTask records when DeletionSweeper first saw a Terminating task with a
// given creation timestamp.
type seenTask struct {
	creationTimestamp time.Time
	firstSeen         time.Time
}

// DeletionSweeper finds Substrate actors and ax tasks stuck in a deletion
// state longer than MinAge. It republishes the delete for each stuck
// unprotected task. It logs one summary line for each SweepOnce call.
//
// The age of a task is the time since this process first saw its
// atespace/name/creationTimestamp key. The task record does not store
// this time (ax-706 DESIGN item 2). A stored Terminating-since condition
// would change both the store and the API. The startup sweep in worker.go
// already covers a controller restart. A restart would otherwise lose
// this in-memory age.
//
// For a DELETING actor, DeletionSweeper measures the age from Substrate's
// own metadata.update_time. Substrate sets this field each time it marks,
// or marks again, the actor DELETING. The code is in ensureMarkedDeleting,
// in the vendored workflow_delete.go.
type DeletionSweeper struct {
	Store  store.Store
	Actors ActorLister
	// MinAge is how long a Terminating task or a DELETING actor must sit
	// before the sweep counts and acts on it. Zero means
	// DefaultDeleteSweepMinAge.
	MinAge time.Duration
	// Now returns the current time. Tests override it to control age
	// without a real sleep. A nil Now uses time.Now.
	Now func() time.Time

	mu   sync.Mutex
	seen map[string]seenTask
}

func (d *DeletionSweeper) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d *DeletionSweeper) minAge() time.Duration {
	if d.MinAge <= 0 {
		return DefaultDeleteSweepMinAge
	}
	return d.MinAge
}

// SweepOnce runs one sweep of Terminating tasks and DELETING actors. It
// pages ListTasks the same way requeueStrandedDeletions does (worker.go),
// so one call bounds memory even with many tasks. It logs and continues
// past a listing error, for either tasks or actors, so one bad page or one
// Substrate outage cannot stop future ticks. It ends with exactly one log
// line carrying msg="delete sweep: stuck deletions", the line a Loki alert
// rule matches (docs/stuck-deletions.md).
func (d *DeletionSweeper) SweepOnce(ctx context.Context) {
	now := d.now()
	minAge := d.minAge()

	stuckTasks, protected, requeued, requeueFailed, taskListError := d.sweepTasks(ctx, now, minAge)
	stuckActors, actorListError := d.sweepActors(ctx, now, minAge)

	stuck := stuckTasks + stuckActors
	args := []any{
		"stuck", stuck,
		"stuck_tasks", stuckTasks,
		"stuck_actors", stuckActors,
		"protected", protected,
		"requeued", requeued,
		"requeue_failed", requeueFailed,
		"task_list_error", taskListError,
		"actor_list_error", actorListError,
		"min_age", minAge,
	}
	if stuck > 0 {
		slog.Warn("delete sweep: stuck deletions", args...)
	} else {
		slog.Info("delete sweep: stuck deletions", args...)
	}
}

// sweepTasks pages every task, ages each Terminating one against the age
// this process has observed it for, and republishes the delete for every
// stuck unprotected one. A protected (supervisor-profile) task is counted
// as stuck but never requeued: its ActorUID and CreationTimestamp
// preconditions are not stored on the record, so an empty-precondition
// requeue would only fail TaskDeletePreconditions.Check (store.go). Re-run
// `ax delete` with its identity to clean it up, the same limit
// requeueStrandedDeletions already documents.
//
// A ListTasks error stops paging early and sets listError (ax-706.7). This
// tick then skips pruneSeen, so the ages this process already tracked stay
// in place instead of resetting to 0: a partial page must not make the
// next tick look falsely young, and must not make this tick's own summary
// line look falsely like an all-clear.
func (d *DeletionSweeper) sweepTasks(ctx context.Context, now time.Time, minAge time.Duration) (stuckTasks, protected, requeued, requeueFailed int, listError bool) {
	seenKeys := make(map[string]bool)
	var offset int64
	for {
		tasks, err := d.Store.ListTasks(ctx, "*", strandedTasksPageSize, offset)
		if err != nil {
			slog.Warn("delete sweep: failed to list tasks", "error", err)
			listError = true
			break
		}
		for _, task := range tasks {
			if task.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
				continue
			}
			atespace := task.GetMetadata().GetAtespace()
			name := task.GetMetadata().GetName()
			key := atespace + "/" + name
			seenKeys[key] = true

			var created time.Time
			if ct := task.GetMetadata().GetCreationTimestamp(); ct != nil && ct.CheckValid() == nil {
				created = ct.AsTime()
			}

			age := now.Sub(d.touch(key, created, now))
			if age < minAge {
				continue
			}
			stuckTasks++
			if task.GetSpec().GetSupervisorProfile() != "" {
				protected++
				slog.Warn("delete sweep: skipping protected stuck task, re-run ax delete with its identity",
					"atespace", atespace, "name", name, "age", age)
				continue
			}
			if err := d.Store.RequeueTaskDeletion(ctx, atespace, name, store.TaskDeletePreconditions{}); err != nil {
				requeueFailed++
				slog.Warn("delete sweep: failed to requeue stuck task deletion", "atespace", atespace, "name", name, "age", age, "error", err)
				continue
			}
			requeued++
			slog.Info("delete sweep: requeued stuck task deletion", "atespace", atespace, "name", name, "age", age)
		}
		if int64(len(tasks)) < strandedTasksPageSize {
			break
		}
		offset += strandedTasksPageSize
	}
	if !listError {
		d.pruneSeen(seenKeys)
	}
	return stuckTasks, protected, requeued, requeueFailed, listError
}

// touch records that this sweep saw key with creation timestamp created,
// and returns the time this process first saw that exact key/created pair.
// A different created value than what is stored (the name was reused by a
// freshly created task) restarts the age at now, per ax-706 AC2.
func (d *DeletionSweeper) touch(key string, created, now time.Time) time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen == nil {
		d.seen = make(map[string]seenTask)
	}
	prev, ok := d.seen[key]
	if !ok || !prev.creationTimestamp.Equal(created) {
		d.seen[key] = seenTask{creationTimestamp: created, firstSeen: now}
		return now
	}
	return prev.firstSeen
}

// pruneSeen drops every tracked key this sweep did not see Terminating,
// whether the task left Terminating or ListTasks no longer lists it at
// all. This mirrors LivenessChecker.pruneMisses (liveness.go) and stops a
// re-created same-name task from inheriting a stale age before touch's own
// creation-timestamp check would catch it.
func (d *DeletionSweeper) pruneSeen(seenKeys map[string]bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key := range d.seen {
		if !seenKeys[key] {
			delete(d.seen, key)
		}
	}
}

// sweepActors lists every Substrate actor and counts, and logs, each one
// in ACTOR_STATE_DELETING whose metadata.update_time is at least minAge
// old. It never calls DeleteActor (ActorLister exposes no such method):
// ax-706 DESIGN item 3 leaves that step to a person, given in
// docs/stuck-deletions.md.
func (d *DeletionSweeper) sweepActors(ctx context.Context, now time.Time, minAge time.Duration) (stuckActors int, listError bool) {
	if d.Actors == nil {
		return 0, false
	}
	actors, err := d.Actors.ListActors(ctx, "")
	if err != nil {
		slog.Warn("delete sweep: failed to list actors", "error", err)
		return 0, true
	}
	for _, actor := range actors {
		if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_DELETING {
			continue
		}
		updateTime := actor.GetMetadata().GetUpdateTime()
		if updateTime == nil || updateTime.CheckValid() != nil {
			continue
		}
		age := now.Sub(updateTime.AsTime())
		if age < minAge {
			continue
		}
		stuckActors++
		slog.Warn("delete sweep: stuck actor",
			"atespace", actor.GetMetadata().GetAtespace(),
			"name", actor.GetMetadata().GetName(),
			"uid", actor.GetMetadata().GetUid(),
			"age", age,
			"worker_pod", actor.GetStatus().GetWorkerAssignment().GetWorkerPod(),
		)
	}
	return stuckActors, false
}
