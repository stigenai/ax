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
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

const (
	defaultWorkerGroup = "ax-controllers"
	// readRetryDelay is how long the worker waits after a transient error from the
	// event queue before trying again.
	readRetryDelay = time.Second
	// requeueBackoffBase and requeueBackoffMax bound the delay before the
	// worker requeues a task that waits for Substrate capacity. The delay
	// doubles on each consecutive ErrWaitingForCapacity result for that task.
	requeueBackoffBase = 15 * time.Second
	requeueBackoffMax  = 2 * time.Minute
	// deleteBackoffBase and deleteBackoffMax bound the delay before the
	// worker retries a task delete that failed to reconcile. The delay
	// doubles on each consecutive failure for that task. There is no limit
	// on the number of attempts.
	deleteBackoffBase = 15 * time.Second
	deleteBackoffMax  = 5 * time.Minute
	// strandedTasksPageSize bounds one ListTasks call in the startup sweep.
	strandedTasksPageSize = 200
)

// Worker consumes task events from the store's event queue and reconciles each
// task against Substrate. Run several with the same group name to share the load;
// every event is handled by exactly one of them.
type Worker struct {
	store      store.Store
	reconciler *TaskReconciler
	group      string
	consumer   string

	// BackoffBase and BackoffMax override the capacity requeue schedule for
	// tests. Both default to the production schedule when left zero.
	BackoffBase time.Duration
	BackoffMax  time.Duration
	// DeleteBackoffBase and DeleteBackoffMax override the delete-retry
	// schedule for tests. Both default to the production schedule when left
	// zero.
	DeleteBackoffBase time.Duration
	DeleteBackoffMax  time.Duration

	requeueMu       sync.Mutex
	requeueAttempts map[string]int

	deleteMu       sync.Mutex
	deleteAttempts map[string]int
	// deletePending marks a pending key with its goroutine's generation
	// number, while that goroutine sleeps on its delete-retry backoff. A
	// second failure for the same key must not start a second chain (bug
	// ax-706.6). The goroutine clears its own mark before it republishes,
	// not after (bug ax-706.10). A clear after the republish is too late:
	// a fast delete failure can reach processEvent while the mark still
	// shows pending, and drop the retry.
	deletePending map[string]int64
	// deleteGen hands out the next generation number for each key. It is
	// never reset. An old goroutine can then tell its own mark apart from
	// a newer chain's mark, and never clear a mark it did not set. Bug
	// ax-706.10 (sibling case): a success can clear a key and start a new
	// chain, while an old goroutine for the same key still sleeps.
	deleteGen map[string]int64
	// deleteJitter overrides defaultDeleteJitter for tests. A nil value uses
	// defaultDeleteJitter.
	deleteJitter func(time.Duration) time.Duration
}

// NewWorker creates a worker that joins group as consumer. An empty group uses the
// default controller group; an empty consumer derives a unique name from the host.
func NewWorker(s store.Store, reconciler *TaskReconciler, group, consumer string) *Worker {
	if group == "" {
		group = defaultWorkerGroup
	}
	if consumer == "" {
		hostname, _ := os.Hostname()
		consumer = fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano()%10000)
	}
	return &Worker{
		store:             s,
		reconciler:        reconciler,
		group:             group,
		consumer:          consumer,
		BackoffBase:       requeueBackoffBase,
		BackoffMax:        requeueBackoffMax,
		DeleteBackoffBase: deleteBackoffBase,
		DeleteBackoffMax:  deleteBackoffMax,
		requeueAttempts:   make(map[string]int),
		deleteAttempts:    make(map[string]int),
	}
}

// Run subscribes to task events and processes them until ctx is done. It returns
// ctx.Err() on shutdown; every event is acknowledged after processing, even when
// reconciliation fails, so a bad task cannot wedge the queue.
func (w *Worker) Run(ctx context.Context) error {
	slog.Info("starting AX task worker", "group", w.group, "consumer", w.consumer)

	sub, err := w.store.Subscribe(ctx, w.group, w.consumer)
	if err != nil {
		return fmt.Errorf("subscribing to task events: %w", err)
	}
	defer sub.Close()

	// Requeue every task a previous process left waiting for capacity, so a
	// crash or restart strands none. This runs after Subscribe so a brand new
	// consumer group's start position already covers the events it publishes.
	w.requeueStrandedTasks(ctx)
	// Retry every delete a previous process left Terminating, for the same reason.
	w.requeueStrandedDeletions(ctx)

	for {
		ev, err := sub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("stopping AX task worker")
				return ctx.Err()
			}
			slog.Error("error reading task events", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(readRetryDelay):
			}
			continue
		}

		if err := w.processEvent(ctx, ev); err != nil {
			slog.Error("error processing task event",
				"id", ev.ID,
				"atespace", ev.Atespace,
				"name", ev.Name,
				"action", ev.Action,
				"error", err,
			)
		}
		if err := sub.Ack(ctx, ev); err != nil {
			slog.Warn("failed to acknowledge task event", "id", ev.ID, "error", err)
		}
	}
}

func (w *Worker) processEvent(ctx context.Context, ev store.TaskEvent) error {
	if ev.Action == "delete" {
		slog.Info("handling task deletion event", "atespace", ev.Atespace, "name", ev.Name, "retry", ev.Retry)
		task, err := w.store.GetTask(ctx, ev.Atespace, ev.Name)
		if errors.Is(err, store.ErrNotFound) {
			if ev.Deletion.ActorUID == "" && ev.Deletion.CreationTimestamp == "" {
				// Legacy deletion still cleans up when the task key has expired.
				if err := w.reconciler.ReconcileDelete(ctx, ev.Atespace, ev.Name); err != nil {
					w.scheduleDeleteRetry(ctx, ev.Atespace, ev.Name, ev.Deletion, err)
					return nil
				}
				w.clearDeleteBackoff(ev.Atespace, ev.Name)
				return nil
			}
			if ev.Deletion.ActorUID == "" || ev.Deletion.CreationTimestamp == "" {
				return store.ErrConflict
			}
			// Missing protected task state cannot authorize cleanup or prove it.
			return nil
		}
		if err != nil {
			return err
		}
		// A delivery can arrive after the task leaves Terminating. A caller
		// can recreate the task. Or an earlier delivery can already finish
		// the deletion. Skip this stale delivery before any Substrate call.
		//
		// Do not base this check on ev.Retry or ev.Deletion.ActorUID. A
		// XAUTOCLAIM reclaim of a dead consumer's unacked event (ax-bsl.4)
		// carries Retry=false. It can also carry no ActorUID, for a legacy
		// delete. Either flag alone misses a stale reclaimed event that
		// targets a task recreated under the same name.
		if task.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
			slog.Info("skipping stale delete: task left Terminating", "atespace", ev.Atespace, "name", ev.Name, "retry", ev.Retry)
			return nil
		}
		if err := ev.Deletion.Check(task); err != nil {
			return err
		}
		if err := w.reconciler.ReconcileDelete(ctx, ev.Atespace, ev.Name, ev.Deletion.ActorUID); err != nil {
			// Retry instead of leaving the record in Terminating forever: a
			// transient Substrate failure (for example, a worker pod OOMKill
			// mid-delete) must not strand the actor holding its worker slot.
			w.scheduleDeleteRetry(ctx, ev.Atespace, ev.Name, ev.Deletion, err)
			return nil
		}
		w.clearDeleteBackoff(ev.Atespace, ev.Name)
		if err := w.store.DeleteTask(ctx, ev.Atespace, ev.Name, ev.Deletion); err != nil {
			return fmt.Errorf("removing task record %s/%s: %w", ev.Atespace, ev.Name, err)
		}
		return nil
	}

	task, err := w.store.GetTask(ctx, ev.Atespace, ev.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			slog.Info("task not found, skipping reconcile", "atespace", ev.Atespace, "name", ev.Name)
			return nil
		}
		return fmt.Errorf("fetching task %s/%s: %w", ev.Atespace, ev.Name, err)
	}

	if task.GetStatus().GetPhase() == v1alpha1.PhaseTerminating {
		return nil
	}

	// Resolve every bound workspace. A missing one is skipped so the task still
	// runs; the runner creates an empty directory at its path.
	var workspaces []*v1alpha1.Workspace
	for _, ref := range task.Spec.WorkspaceRefs() {
		if ref.Name == "" {
			continue
		}
		wsp, err := w.store.GetWorkspace(ctx, task.Metadata.Atespace, ref.Name)
		if err == nil {
			workspaces = append(workspaces, wsp)
		} else if !errors.Is(err, store.ErrNotFound) {
			slog.Warn("error fetching workspace", "name", ref.Name, "error", err)
		}
	}

	reconciled, err := w.reconciler.Reconcile(ctx, task, workspaces...)
	if err != nil {
		if errors.Is(err, ErrWaitingForCapacity) {
			// task already carries Phase=Pending and the WaitingForCapacity
			// condition: Reconcile set them on this same pointer.
			if serr := w.store.UpdateTaskStatus(ctx, task.Metadata.Atespace, task.Metadata.Name, task.Status); serr != nil {
				return fmt.Errorf("updating task status %s/%s: %w", task.Metadata.Atespace, task.Metadata.Name, serr)
			}
			w.scheduleRequeue(ctx, task.Metadata.Atespace, task.Metadata.Name)
			slog.Info("task waiting for substrate capacity", "atespace", task.Metadata.Atespace, "name", task.Metadata.Name)
			return nil
		}
		task.Status.Phase = "Failed"
		_ = w.store.UpdateTaskStatus(ctx, task.Metadata.Atespace, task.Metadata.Name, task.Status)
		return fmt.Errorf("reconciling task %s/%s: %w", task.Metadata.Atespace, task.Metadata.Name, err)
	}

	w.clearRequeueBackoff(task.Metadata.Atespace, task.Metadata.Name)

	if err := w.store.UpdateTaskStatus(ctx, task.Metadata.Atespace, task.Metadata.Name, reconciled.Status); err != nil {
		return fmt.Errorf("updating task status %s/%s: %w", task.Metadata.Atespace, task.Metadata.Name, err)
	}

	return nil
}

// scheduleRequeue requeues atespace/name after a backoff that doubles on each
// consecutive call for the same task, capped at w.BackoffMax. The requeue
// itself runs in a separate goroutine so it never blocks the event loop.
func (w *Worker) scheduleRequeue(ctx context.Context, atespace, name string) {
	key := atespace + "/" + name

	base, max := w.BackoffBase, w.BackoffMax
	if base <= 0 {
		base = requeueBackoffBase
	}
	if max <= 0 {
		max = requeueBackoffMax
	}

	w.requeueMu.Lock()
	attempt := w.requeueAttempts[key]
	w.requeueAttempts[key] = attempt + 1
	w.requeueMu.Unlock()

	backoff := base
	for i := 0; i < attempt && backoff < max; i++ {
		backoff *= 2
	}
	if backoff > max {
		backoff = max
	}

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if err := w.store.RequeueTask(context.Background(), atespace, name); err != nil {
			slog.Warn("failed to requeue task waiting for capacity", "atespace", atespace, "name", name, "error", err)
		}
	}()
}

// clearRequeueBackoff resets a task's backoff schedule once it reconciles
// without ErrWaitingForCapacity, so the next time it waits for capacity the
// schedule restarts at BackoffBase.
func (w *Worker) clearRequeueBackoff(atespace, name string) {
	w.requeueMu.Lock()
	delete(w.requeueAttempts, atespace+"/"+name)
	w.requeueMu.Unlock()
}

// scheduleDeleteRetry republishes a failed task deletion after a backoff.
// The backoff doubles on each consecutive failure for the same task. It
// stops doubling at w.DeleteBackoffMax. It never gives up: a stuck actor
// that holds its worker assignment forever costs more than one more retry.
// The retry runs in its own goroutine. It never blocks the event loop.
//
// At most one retry stays pending per atespace/name (ax-706.6). Assume a
// goroutine still sleeps on its backoff for a key. A second call for that
// key only logs and returns. It starts no second goroutine. It does not
// count as a new attempt. Without this check, a periodic sweep can
// republish the same stuck delete on every tick. Each tick would then start
// its own chain. Each chain calls Substrate's DeleteActor on its own
// schedule, forever, even after the sweep itself stops ticking.
func (w *Worker) scheduleDeleteRetry(ctx context.Context, atespace, name string, deletion store.TaskDeletePreconditions, cause error) {
	key := atespace + "/" + name

	base, max := w.DeleteBackoffBase, w.DeleteBackoffMax
	if base <= 0 {
		base = deleteBackoffBase
	}
	if max <= 0 {
		max = deleteBackoffMax
	}

	w.deleteMu.Lock()
	if w.deletePending == nil {
		w.deletePending = make(map[string]int64)
	}
	if _, pending := w.deletePending[key]; pending {
		w.deleteMu.Unlock()
		slog.Info("task delete failed, a retry chain is already pending, not starting a second one",
			"atespace", atespace, "name", name, "error", cause)
		return
	}
	if w.deleteGen == nil {
		w.deleteGen = make(map[string]int64)
	}
	w.deleteGen[key]++
	myGen := w.deleteGen[key]
	w.deletePending[key] = myGen
	if w.deleteAttempts == nil {
		w.deleteAttempts = make(map[string]int)
	}
	attempt := w.deleteAttempts[key]
	w.deleteAttempts[key] = attempt + 1
	w.deleteMu.Unlock()

	backoff := deleteBackoff(attempt, base, max)
	jitter := w.deleteJitter
	if jitter == nil {
		jitter = defaultDeleteJitter
	}
	delay := jitter(backoff)

	slog.Warn("task delete failed, retrying", "atespace", atespace, "name", name,
		"attempt", attempt+1, "next_retry", delay, "error", cause)

	// clearOwnMark removes this chain's pending mark, but only while it
	// still holds myGen. A clearDeleteBackoff call for this key, followed
	// by a new chain, hands the key a new generation; this guard then skips
	// the delete so the stale goroutine never erases the newer chain's mark
	// (ax-706.10, sibling case).
	clearOwnMark := func() {
		w.deleteMu.Lock()
		if w.deletePending[key] == myGen {
			delete(w.deletePending, key)
		}
		w.deleteMu.Unlock()
	}

	go func() {
		select {
		case <-ctx.Done():
			clearOwnMark()
			return
		case <-time.After(delay):
		}
		// Clear the mark before republishing, not in a defer after
		// (ax-706.10): the republished event can reach processEvent and
		// fail before a deferred clear runs. scheduleDeleteRetry would then
		// see pending still set and drop the retry, leaving no chain for
		// the next sweep tick to catch other than the 15-minute sweep.
		clearOwnMark()
		if err := w.store.RequeueTaskDeletion(context.Background(), atespace, name, deletion); err != nil {
			slog.Warn("failed to requeue task deletion", "atespace", atespace, "name", name, "error", err)
			// clearOwnMark already ran, so this call starts the next chain
			// at attempt+1 (a longer backoff). If another chain is already
			// pending for this key, it only logs and returns, which is
			// correct: that chain's own retry still covers this failure.
			w.scheduleDeleteRetry(ctx, atespace, name, deletion, fmt.Errorf("requeue task deletion: %w", err))
		}
	}()
}

// deleteBackoff reports the delete-retry delay for attempt (0-indexed),
// doubling base once per attempt and clamping at max. It never overflows:
// the loop stops doubling as soon as backoff reaches max, no matter how
// large attempt is.
func deleteBackoff(attempt int, base, max time.Duration) time.Duration {
	backoff := base
	for i := 0; i < attempt && backoff < max; i++ {
		backoff *= 2
	}
	if backoff > max {
		backoff = max
	}
	return backoff
}

// defaultDeleteJitter spreads a delete retry's delay across a range. This
// stops an outage that fails many deletes at once from retrying them all at
// the same time. The return value is in the range [d/2, d]. The value is
// never less than half of d. The value is never more than d. So the jitter
// never makes a backoff longer than w.DeleteBackoffMax. A d value of zero or
// less returns d unchanged.
func defaultDeleteJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	half := d / 2
	return half + rand.N(half+1)
}

// clearDeleteBackoff resets a task's delete-retry schedule once its deletion
// reconciles without error, so a later deletion of the same name starts its
// own schedule at DeleteBackoffBase. It also clears any pending-chain mark,
// so a later failure for the same name can start a fresh chain right away.
// It does not touch deleteGen: a stale goroutine from the cleared chain may
// still sleep on its own backoff, and deleteGen's next value must stay
// ahead of that goroutine's captured generation so it can never clear a
// newer chain's mark (ax-706.10).
func (w *Worker) clearDeleteBackoff(atespace, name string) {
	w.deleteMu.Lock()
	delete(w.deletePending, atespace+"/"+name)
	delete(w.deleteAttempts, atespace+"/"+name)
	w.deleteMu.Unlock()
}

// requeueStrandedDeletions retries every task a previous controller process
// left Terminating, with no backoff: a restart is not a new delete failure.
// A supervisor-profile task's ActorUID and CreationTimestamp preconditions
// are not stored on the record, so this function skips it. Re-run `ax
// delete` with its identity to clean it up. This logs and continues past a
// listing or requeue error, so one bad page or task cannot stop the worker
// from starting.
func (w *Worker) requeueStrandedDeletions(ctx context.Context) {
	var offset int64
	var requeued, skipped int
	for {
		tasks, err := w.store.ListTasks(ctx, "*", strandedTasksPageSize, offset)
		if err != nil {
			slog.Warn("delete sweep: failed to list tasks", "error", err)
			return
		}
		for _, task := range tasks {
			if task.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
				continue
			}
			atespace := task.GetMetadata().GetAtespace()
			name := task.GetMetadata().GetName()
			if task.GetSpec().GetSupervisorProfile() != "" {
				slog.Warn("delete sweep: skipping protected task, re-run ax delete with its identity",
					"atespace", atespace, "name", name)
				skipped++
				continue
			}
			if err := w.store.RequeueTaskDeletion(ctx, atespace, name, store.TaskDeletePreconditions{}); err != nil {
				slog.Warn("delete sweep: failed to requeue task deletion", "atespace", atespace, "name", name, "error", err)
				continue
			}
			slog.Info("delete sweep: requeued a terminating task", "atespace", atespace, "name", name)
			requeued++
		}
		if int64(len(tasks)) < strandedTasksPageSize {
			break
		}
		offset += strandedTasksPageSize
	}
	slog.Info(fmt.Sprintf("delete sweep: requeued %d terminating tasks, skipped %d protected", requeued, skipped))
}

// requeueStrandedTasks retries every task left waiting for Substrate capacity
// by a previous controller process, with no backoff: a restart is not a new
// capacity failure. It logs and continues past a listing or requeue error, so
// one bad page or task cannot stop the worker from starting.
func (w *Worker) requeueStrandedTasks(ctx context.Context) {
	var offset int64
	for {
		tasks, err := w.store.ListTasks(ctx, "*", strandedTasksPageSize, offset)
		if err != nil {
			slog.Warn("capacity sweep: failed to list tasks", "error", err)
			return
		}
		for _, task := range tasks {
			if !waitingForCapacity(task) {
				continue
			}
			atespace := task.GetMetadata().GetAtespace()
			name := task.GetMetadata().GetName()
			if err := w.store.RequeueTask(ctx, atespace, name); err != nil {
				slog.Warn("capacity sweep: failed to requeue task", "atespace", atespace, "name", name, "error", err)
				continue
			}
			slog.Info("capacity sweep: requeued a task stranded waiting for capacity", "atespace", atespace, "name", name)
		}
		if int64(len(tasks)) < strandedTasksPageSize {
			return
		}
		offset += strandedTasksPageSize
	}
}

// waitingForCapacity reports whether task's Ready condition reason is
// reasonWaitingForCapacity.
func waitingForCapacity(task *v1alpha1.Task) bool {
	for _, c := range task.GetStatus().GetConditions() {
		if c.GetType() == condReady {
			return c.GetReason() == reasonWaitingForCapacity
		}
	}
	return false
}
