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

package store

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

var (
	ErrNotFound = errors.New("resource not found")
	ErrConflict = errors.New("task deletion precondition failed")
)

// TaskDeletePreconditions binds queued deletion to an already observed actor and task.
type TaskDeletePreconditions struct {
	ActorUID          string
	CreationTimestamp string
}

func (p TaskDeletePreconditions) Check(task *v1alpha1.Task) error {
	if task == nil {
		return ErrNotFound
	}
	if p.ActorUID == "" && p.CreationTimestamp == "" && task.GetSpec().GetSupervisorProfile() == "" {
		return nil
	}
	created := task.GetMetadata().GetCreationTimestamp()
	expected, err := time.Parse(time.RFC3339Nano, p.CreationTimestamp)
	if p.ActorUID == "" || created == nil || created.CheckValid() != nil || err != nil || !created.AsTime().Equal(expected) {
		return ErrConflict
	}
	return nil
}

func DeletePreconditions(values []TaskDeletePreconditions) (TaskDeletePreconditions, error) {
	if len(values) > 1 {
		return TaskDeletePreconditions{}, ErrConflict
	}
	if len(values) == 1 {
		return values[0], nil
	}
	return TaskDeletePreconditions{}, nil
}

// TaskEvent represents an event published to the task event stream.
type TaskEvent struct {
	ID       string
	Atespace string
	Name     string
	Action   string // "reconcile", "delete"
	Deletion TaskDeletePreconditions
	// Retry marks a delete event republished after an earlier delivery of the
	// same deletion failed to reconcile. A worker uses it to tell a stale
	// replay (the task is no longer Terminating) from a fresh request.
	Retry bool
}

// EventQueue delivers task events to groups of cooperating workers. Every event
// is delivered to exactly one member of a group, and stays pending until that
// member acknowledges it, so a crashed worker's events can be picked up again.
type EventQueue interface {
	// Subscribe joins group as consumer, creating the group if it does not exist.
	// All members of a group share one stream of events.
	Subscribe(ctx context.Context, group, consumer string) (Subscription, error)
}

// Subscription is one consumer's view of an EventQueue group.
type Subscription interface {
	// Next blocks until an event is available or ctx is done.
	Next(ctx context.Context) (TaskEvent, error)
	// Ack marks an event as processed so it is not delivered again.
	Ack(ctx context.Context, ev TaskEvent) error
	// Close releases the subscription. Unacknowledged events stay pending for the group.
	Close() error
}

// Store defines the storage and event streaming interface for AX resources.
type Store interface {
	EventQueue

	SaveTask(ctx context.Context, task *v1alpha1.Task) error
	GetTask(ctx context.Context, atespace, name string) (*v1alpha1.Task, error)
	ListTasks(ctx context.Context, atespace string, limit, offset int64) ([]*v1alpha1.Task, error)
	UpdateTaskStatus(ctx context.Context, atespace, name string, status *v1alpha1.TaskStatus) error
	// UpdateTaskStatusIf writes status only when expect reports true for the
	// task's current stored status. It reads and writes in one transaction.
	// This stops a stale caller from overwriting a status change that
	// landed in between. When expect returns false, it returns applied=false
	// and a nil error: it leaves the task unchanged and notifies no watcher.
	// It returns ErrNotFound when the task does not exist. A caller uses it
	// to mark a task Failed only when it is still in the phase the caller
	// last observed, for example the liveness sweep's ActorLost write racing
	// a concurrent SaveTask.
	UpdateTaskStatusIf(ctx context.Context, atespace, name string, expect func(*v1alpha1.TaskStatus) bool, status *v1alpha1.TaskStatus) (applied bool, err error)
	// MarkTaskDeleting begins a two-phase delete: the task's phase becomes
	// "Terminating" and a delete event is published for the controller, which
	// removes the actor and then calls DeleteTask. Returns ErrNotFound if the
	// task does not exist.
	MarkTaskDeleting(ctx context.Context, atespace, name string, preconditions ...TaskDeletePreconditions) error
	// DeleteTask removes the task record. It publishes no event; callers are
	// expected to have cleaned up the task's actor first.
	DeleteTask(ctx context.Context, atespace, name string, preconditions ...TaskDeletePreconditions) error
	// RequeueTask publishes a new reconcile event for an existing task without
	// changing its stored record. A worker calls it to retry a task that found
	// Substrate out of capacity, and at startup to retry every task a previous
	// process left waiting for capacity.
	RequeueTask(ctx context.Context, atespace, name string) error
	// RequeueTaskDeletion publishes a new delete event for atespace/name. The
	// event carries p, the same preconditions as the original MarkTaskDeleting
	// call. It marks the event Retry. It does not change the stored task
	// record. A worker calls it after a failed ReconcileDelete, after a
	// backoff. A worker also calls it at startup, to retry each task a
	// previous process left Terminating.
	RequeueTaskDeletion(ctx context.Context, atespace, name string, p TaskDeletePreconditions) error

	SaveWorkspace(ctx context.Context, ws *v1alpha1.Workspace) error
	GetWorkspace(ctx context.Context, atespace, name string) (*v1alpha1.Workspace, error)
	ListWorkspaces(ctx context.Context, atespace string) ([]*v1alpha1.Workspace, error)
	DeleteWorkspace(ctx context.Context, atespace, name string) error

	SaveModel(ctx context.Context, model *v1alpha1.Model) error
	GetModel(ctx context.Context, atespace, name string) (*v1alpha1.Model, error)
	ListModels(ctx context.Context, atespace string) ([]*v1alpha1.Model, error)
	DeleteModel(ctx context.Context, atespace, name string) error

	WatchTask(ctx context.Context, atespace, name string) (<-chan *v1alpha1.Task, io.Closer, error)
	Close() error
}
