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

	requeueMu       sync.Mutex
	requeueAttempts map[string]int
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
		store:           s,
		reconciler:      reconciler,
		group:           group,
		consumer:        consumer,
		BackoffBase:     requeueBackoffBase,
		BackoffMax:      requeueBackoffMax,
		requeueAttempts: make(map[string]int),
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
		slog.Info("handling task deletion event", "atespace", ev.Atespace, "name", ev.Name)
		task, err := w.store.GetTask(ctx, ev.Atespace, ev.Name)
		if errors.Is(err, store.ErrNotFound) {
			if ev.Deletion.ActorUID == "" && ev.Deletion.CreationTimestamp == "" {
				// Legacy deletion still cleans up when the task key has expired.
				return w.reconciler.ReconcileDelete(ctx, ev.Atespace, ev.Name)
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
		if err := ev.Deletion.Check(task); err != nil {
			return err
		}
		if ev.Deletion.ActorUID != "" && task.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
			return store.ErrConflict
		}
		if err := w.reconciler.ReconcileDelete(ctx, ev.Atespace, ev.Name, ev.Deletion.ActorUID); err != nil {
			// Leave the record in Terminating so the failure is visible; re-running
			// `ax delete` republishes the event and retries the cleanup.
			return fmt.Errorf("cleaning up task %s/%s: %w", ev.Atespace, ev.Name, err)
		}
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
