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
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultStreamName    = "ax:stream:tasks"
	defaultReadBatchSize = 10
	defaultReadBlock     = 2 * time.Second
	// defaultClaimMinIdle is how long a pending entry sits unacknowledged
	// before another consumer in the group may reclaim it.
	defaultClaimMinIdle = 5 * time.Minute
	// defaultClaimInterval is the minimum time between XAUTOCLAIM calls from
	// one subscription when a scan finds nothing to claim.
	defaultClaimInterval = 30 * time.Second
	// defaultMaxDeliveries is how many times a task event may be delivered
	// before a subscription drops it as poison.
	defaultMaxDeliveries = 5
)

var (
	jsonMarshalOpts   = protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}
	jsonUnmarshalOpts = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// Options contains configuration for the Redis store.
type Options struct {
	StreamName string
	KeyPrefix  string
	TTL        time.Duration // Optional TTL for task records

	// ReadBatchSize is how many events one XREADGROUP call may return.
	ReadBatchSize int64
	// ReadBlock is how long one XREADGROUP call waits for events before returning
	// empty. Shorter values make shutdown more responsive at the cost of more calls.
	ReadBlock time.Duration

	// ClaimMinIdle sets the idle time before a subscription may steal a
	// pending entry from its consumer, with XAUTOCLAIM.
	// Zero uses the default: 5 minutes.
	// A negative value turns reclaim off.
	// Then a dead consumer's pending entries stay pending forever.
	ClaimMinIdle time.Duration
	// ClaimInterval sets the least time between XAUTOCLAIM scans.
	// A subscription waits this long after a scan finds nothing to claim.
	// Zero uses the default: 30 seconds.
	// This field has no effect when reclaim is off.
	ClaimInterval time.Duration
	// MaxDeliveries caps how many times a subscription may deliver the same
	// event. A subscription acks and drops an event past this count,
	// so a poison event cannot crash-loop the process that reads it forever.
	// Zero uses the default: 5.
	MaxDeliveries int64
}

// Store is a Redis-backed implementation of store.Store.
type Store struct {
	client *redis.Client
	opts   Options
}

// NewStore creates a new Redis store.
func NewStore(client *redis.Client, opts Options) *Store {
	if opts.StreamName == "" {
		opts.StreamName = defaultStreamName
	}
	if opts.KeyPrefix == "" {
		opts.KeyPrefix = "ax"
	}
	if opts.ReadBatchSize <= 0 {
		opts.ReadBatchSize = defaultReadBatchSize
	}
	if opts.ReadBlock <= 0 {
		opts.ReadBlock = defaultReadBlock
	}
	if opts.ClaimMinIdle == 0 {
		opts.ClaimMinIdle = defaultClaimMinIdle
	}
	if opts.ClaimInterval <= 0 {
		opts.ClaimInterval = defaultClaimInterval
	}
	if opts.MaxDeliveries <= 0 {
		opts.MaxDeliveries = defaultMaxDeliveries
	}
	return &Store{
		client: client,
		opts:   opts,
	}
}

func (s *Store) taskKey(atespace, name string) string {
	return fmt.Sprintf("%s:task:%s:%s", s.opts.KeyPrefix, atespace, name)
}

func (s *Store) modelKey(atespace, name string) string {
	return fmt.Sprintf("%s:model:%s:%s", s.opts.KeyPrefix, atespace, name)
}

func (s *Store) modelIndexKey() string {
	return fmt.Sprintf("%s:models:index", s.opts.KeyPrefix)
}

func (s *Store) modelAtespaceIndexKey(atespace string) string {
	return fmt.Sprintf("%s:models:atespace:%s", s.opts.KeyPrefix, atespace)
}

func (s *Store) wsKey(atespace, name string) string {
	return fmt.Sprintf("%s:workspace:%s:%s", s.opts.KeyPrefix, atespace, name)
}

func (s *Store) wsIndexKey() string {
	return fmt.Sprintf("%s:workspaces:index", s.opts.KeyPrefix)
}

func (s *Store) wsAtespaceIndexKey(atespace string) string {
	return fmt.Sprintf("%s:workspaces:atespace:%s", s.opts.KeyPrefix, atespace)
}

func (s *Store) taskIndexKey() string {
	return fmt.Sprintf("%s:tasks:index", s.opts.KeyPrefix)
}

func (s *Store) taskAtespaceIndexKey(atespace string) string {
	return fmt.Sprintf("%s:tasks:atespace:%s", s.opts.KeyPrefix, atespace)
}

func (s *Store) taskPubSubChannel(atespace, name string) string {
	return fmt.Sprintf("%s:pubsub:task:%s:%s", s.opts.KeyPrefix, atespace, name)
}

// SaveTask stores or updates a task. It then publishes a reconcile event.
// It returns ErrConflict when the stored phase is already Terminating.
// This rule covers every task. It leaves the stored task unchanged.
// It publishes no event. The check runs inside the store transaction.
func (s *Store) SaveTask(ctx context.Context, task *v1alpha1.Task) error {
	if task.Metadata == nil {
		task.Metadata = &v1alpha1.ObjectMeta{}
	}
	if task.Metadata.Name == "" {
		return errors.New("task name is required")
	}
	if task.Metadata.Atespace == "" {
		task.Metadata.Atespace = "default"
	}
	if task.ApiVersion == "" {
		task.ApiVersion = v1alpha1.APIVersion
	}
	if task.Kind == "" {
		task.Kind = v1alpha1.KindTask
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	if task.Status.Phase == "" {
		task.Status.Phase = "Pending"
	}

	data, err := protojson.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshaling task: %w", err)
	}

	atespace := task.Metadata.Atespace
	name := task.Metadata.Name
	score := float64(time.Now().UnixNano())
	member := fmt.Sprintf("%s:%s", atespace, name)

	key := s.taskKey(atespace, name)
	return s.client.Watch(ctx, func(tx *redis.Tx) error {
		existing, err := taskForDelete(ctx, tx, key)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		if existing.GetStatus().GetPhase() == v1alpha1.PhaseTerminating {
			return store.ErrConflict
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, s.taskKey(atespace, name), data, s.opts.TTL)
			pipe.ZAdd(ctx, s.taskIndexKey(), redis.Z{Score: score, Member: member})
			pipe.ZAdd(ctx, s.taskAtespaceIndexKey(atespace), redis.Z{Score: score, Member: name})
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: s.opts.StreamName,
				Values: map[string]interface{}{
					"action":   "reconcile",
					"atespace": atespace,
					"name":     name,
				},
			})
			pipe.Publish(ctx, s.taskPubSubChannel(atespace, name), data)

			return nil
		})
		return err
	}, key)
}

// GetTask retrieves a task by atespace and name.
func (s *Store) GetTask(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	if atespace == "" {
		atespace = "default"
	}
	val, err := s.client.Get(ctx, s.taskKey(atespace, name)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting task from redis: %w", err)
	}

	var task v1alpha1.Task
	if err := jsonUnmarshalOpts.Unmarshal([]byte(val), &task); err != nil {
		return nil, fmt.Errorf("unmarshaling task: %w", err)
	}
	return &task, nil
}

// ListTasks lists tasks ordered by newest first.
func (s *Store) ListTasks(ctx context.Context, atespace string, limit, offset int64) ([]*v1alpha1.Task, error) {
	if limit <= 0 {
		limit = 50
	}
	start := offset
	stop := offset + limit - 1

	var members []string
	var err error

	if atespace == "" || atespace == "*" {
		members, err = s.client.ZRevRange(ctx, s.taskIndexKey(), start, stop).Result()
	} else {
		names, nErr := s.client.ZRevRange(ctx, s.taskAtespaceIndexKey(atespace), start, stop).Result()
		if nErr == nil {
			for _, n := range names {
				members = append(members, fmt.Sprintf("%s:%s", atespace, n))
			}
		}
		err = nErr
	}

	if err != nil {
		return nil, fmt.Errorf("listing task index: %w", err)
	}
	if len(members) == 0 {
		return []*v1alpha1.Task{}, nil
	}

	keys := make([]string, len(members))
	for i, m := range members {
		parts := strings.SplitN(m, ":", 2)
		if len(parts) == 2 {
			keys[i] = s.taskKey(parts[0], parts[1])
		} else {
			keys[i] = s.taskKey("default", m)
		}
	}

	vals, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("batch fetching tasks: %w", err)
	}

	tasks := make([]*v1alpha1.Task, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		str, ok := v.(string)
		if !ok {
			continue
		}
		var t v1alpha1.Task
		if err := jsonUnmarshalOpts.Unmarshal([]byte(str), &t); err == nil {
			tasks = append(tasks, &t)
		}
	}
	return tasks, nil
}

// UpdateTaskStatus updates only the status portion of a task.
func (s *Store) UpdateTaskStatus(ctx context.Context, atespace, name string, status *v1alpha1.TaskStatus) error {
	if atespace == "" {
		atespace = "default"
	}
	key := s.taskKey(atespace, name)
	return s.client.Watch(ctx, func(tx *redis.Tx) error {
		task, err := taskForDelete(ctx, tx, key)
		if err != nil {
			return err
		}
		// ax-bsl.7: keep Terminating for every task, not only a
		// supervisor task. A write that started before MarkTaskDeleting
		// can land after it and clear Terminating back to Running. A
		// queued delete event then sees Running, skips as stale, and
		// never deletes the task.
		if task.GetStatus().GetPhase() == v1alpha1.PhaseTerminating {
			return nil
		}
		task.Status = status
		data, err := jsonMarshalOpts.Marshal(task)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, key, data, s.opts.TTL)
			pipe.Publish(ctx, s.taskPubSubChannel(atespace, name), data)
			return nil
		})
		return err
	}, key)
}

// UpdateTaskStatusIf writes status only when expect reports true for the
// task's current status. See store.Store for the full contract. The read
// and the write happen in the same WATCH transaction as UpdateTaskStatus,
// so a concurrent write between the check and this one aborts the
// transaction instead of silently winning.
func (s *Store) UpdateTaskStatusIf(ctx context.Context, atespace, name string, expect func(*v1alpha1.TaskStatus) bool, status *v1alpha1.TaskStatus) (bool, error) {
	if atespace == "" {
		atespace = "default"
	}
	key := s.taskKey(atespace, name)
	applied := false
	err := s.client.Watch(ctx, func(tx *redis.Tx) error {
		applied = false
		task, err := taskForDelete(ctx, tx, key)
		if err != nil {
			return err
		}
		if !expect(task.Status) {
			return nil
		}
		// ax-bsl.7: same Terminating guard as UpdateTaskStatus, for every
		// task, not only a supervisor task.
		if task.GetStatus().GetPhase() == v1alpha1.PhaseTerminating {
			return nil
		}
		task.Status = status
		data, err := jsonMarshalOpts.Marshal(task)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, key, data, s.opts.TTL)
			pipe.Publish(ctx, s.taskPubSubChannel(atespace, name), data)
			return nil
		})
		if err != nil {
			return err
		}
		applied = true
		return nil
	}, key)
	return applied, err
}

// UpdateTaskSuspend sets task.Spec.Suspend inside a WATCH transaction on the
// task key. It does not change the stored status.
//
// It also adds a reconcile event to the stream, in the same pipeline, in
// the shape SaveTask uses. The old code called SaveTask instead. SaveTask's
// reconcile event made the worker reconcile the task and call SuspendActor
// or ResumeActor. Without this event, Spec.Suspend changes. But the actor
// keeps running until some other reconcile happens. See store.Store for the
// full contract.
func (s *Store) UpdateTaskSuspend(ctx context.Context, atespace, name string, suspend bool) (*v1alpha1.Task, error) {
	if atespace == "" {
		atespace = "default"
	}
	key := s.taskKey(atespace, name)
	var updated *v1alpha1.Task
	err := s.client.Watch(ctx, func(tx *redis.Tx) error {
		task, err := taskForDelete(ctx, tx, key)
		if err != nil {
			return err
		}
		if task.Spec == nil {
			task.Spec = &v1alpha1.TaskSpec{}
		}
		task.Spec.Suspend = suspend
		data, err := protojson.Marshal(task)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, key, data, s.opts.TTL)
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: s.opts.StreamName,
				Values: map[string]interface{}{
					"action":   "reconcile",
					"atespace": atespace,
					"name":     name,
				},
			})
			pipe.Publish(ctx, s.taskPubSubChannel(atespace, name), data)
			return nil
		})
		if err != nil {
			return err
		}
		updated = task
		return nil
	}, key)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// MarkTaskDeleting flips the task to the Terminating phase, notifies watchers, and
// publishes a delete event for the controller. The record stays until DeleteTask.
func (s *Store) MarkTaskDeleting(ctx context.Context, atespace, name string, preconditions ...store.TaskDeletePreconditions) error {
	precondition, err := store.DeletePreconditions(preconditions)
	if err != nil {
		return err
	}
	if atespace == "" {
		atespace = "default"
	}
	key := s.taskKey(atespace, name)
	return s.client.Watch(ctx, func(tx *redis.Tx) error {
		task, err := taskForDelete(ctx, tx, key)
		if err != nil {
			return err
		}
		if err := precondition.Check(task); err != nil {
			return err
		}
		if task.Status == nil {
			task.Status = &v1alpha1.TaskStatus{}
		}
		task.Status.Phase = v1alpha1.PhaseTerminating
		data, err := protojson.Marshal(task)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, key, data, s.opts.TTL)
			pipe.Publish(ctx, s.taskPubSubChannel(atespace, name), data)
			pipe.XAdd(ctx, &redis.XAddArgs{Stream: s.opts.StreamName, Values: map[string]interface{}{
				"action": "delete", "atespace": atespace, "name": name,
				"actor_uid": precondition.ActorUID, "creation_timestamp": precondition.CreationTimestamp,
			}})
			return nil
		})
		return err
	}, key)
}

func taskForDelete(ctx context.Context, tx *redis.Tx, key string) (*v1alpha1.Task, error) {
	data, err := tx.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	task := &v1alpha1.Task{}
	if err := jsonUnmarshalOpts.Unmarshal(data, task); err != nil {
		return nil, err
	}
	return task, nil
}

// DeleteTask removes only the task incarnation carried by the delete event.
func (s *Store) DeleteTask(ctx context.Context, atespace, name string, preconditions ...store.TaskDeletePreconditions) error {
	precondition, err := store.DeletePreconditions(preconditions)
	if err != nil {
		return err
	}
	if atespace == "" {
		atespace = "default"
	}
	member := fmt.Sprintf("%s:%s", atespace, name)
	key := s.taskKey(atespace, name)
	return s.client.Watch(ctx, func(tx *redis.Tx) error {
		task, err := taskForDelete(ctx, tx, key)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := precondition.Check(task); err != nil {
			return err
		}
		if precondition.ActorUID != "" && task.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
			return store.ErrConflict
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, key)
			pipe.ZRem(ctx, s.taskIndexKey(), member)
			pipe.ZRem(ctx, s.taskAtespaceIndexKey(atespace), name)
			return nil
		})
		return err
	}, key)
}

// RequeueTask publishes a new reconcile event for an existing task, without
// touching the stored record. The stream entry is the same shape SaveTask
// publishes, so a worker handles it the same way.
func (s *Store) RequeueTask(ctx context.Context, atespace, name string) error {
	if atespace == "" {
		atespace = "default"
	}
	return s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.opts.StreamName,
		Values: map[string]interface{}{
			"action":   "reconcile",
			"atespace": atespace,
			"name":     name,
		},
	}).Err()
}

// RequeueTaskDeletion publishes a new delete event carrying p, marked
// retry=1, without touching the stored task record. The stream entry has the
// same shape MarkTaskDeleting publishes, so a worker handles it the same way.
func (s *Store) RequeueTaskDeletion(ctx context.Context, atespace, name string, p store.TaskDeletePreconditions) error {
	if atespace == "" {
		atespace = "default"
	}
	return s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.opts.StreamName,
		Values: map[string]interface{}{
			"action":             "delete",
			"atespace":           atespace,
			"name":               name,
			"actor_uid":          p.ActorUID,
			"creation_timestamp": p.CreationTimestamp,
			"retry":              "1",
		},
	}).Err()
}

// SaveModel stores a model.
func (s *Store) SaveModel(ctx context.Context, model *v1alpha1.Model) error {
	if model.Metadata == nil {
		model.Metadata = &v1alpha1.ObjectMeta{}
	}
	if model.Metadata.Name == "" {
		return errors.New("model name is required")
	}
	if model.Metadata.Atespace == "" {
		model.Metadata.Atespace = "default"
	}
	if model.ApiVersion == "" {
		model.ApiVersion = v1alpha1.APIVersion
	}
	if model.Kind == "" {
		model.Kind = v1alpha1.KindModel
	}

	data, err := protojson.Marshal(model)
	if err != nil {
		return fmt.Errorf("marshaling model: %w", err)
	}

	atespace := model.Metadata.Atespace
	name := model.Metadata.Name
	score := float64(time.Now().UnixNano())
	member := fmt.Sprintf("%s:%s", atespace, name)

	pipe := s.client.TxPipeline()
	pipe.Set(ctx, s.modelKey(atespace, name), data, 0)
	pipe.ZAdd(ctx, s.modelIndexKey(), redis.Z{Score: score, Member: member})
	pipe.ZAdd(ctx, s.modelAtespaceIndexKey(atespace), redis.Z{Score: score, Member: name})
	_, err = pipe.Exec(ctx)
	return err
}

// DeleteModel removes a model.
func (s *Store) DeleteModel(ctx context.Context, atespace, name string) error {
	if atespace == "" {
		atespace = "default"
	}
	member := fmt.Sprintf("%s:%s", atespace, name)

	pipe := s.client.TxPipeline()
	pipe.Del(ctx, s.modelKey(atespace, name))
	pipe.ZRem(ctx, s.modelIndexKey(), member)
	pipe.ZRem(ctx, s.modelAtespaceIndexKey(atespace), name)
	_, err := pipe.Exec(ctx)
	return err
}

// ListModels lists models for an atespace or across all atespaces.
func (s *Store) ListModels(ctx context.Context, atespace string) ([]*v1alpha1.Model, error) {
	var members []string
	var err error

	if atespace == "" || atespace == "*" {
		members, err = s.client.ZRevRange(ctx, s.modelIndexKey(), 0, -1).Result()
	} else {
		names, nErr := s.client.ZRevRange(ctx, s.modelAtespaceIndexKey(atespace), 0, -1).Result()
		if nErr == nil {
			for _, n := range names {
				members = append(members, fmt.Sprintf("%s:%s", atespace, n))
			}
		}
		err = nErr
	}

	if err != nil {
		return nil, fmt.Errorf("listing model index: %w", err)
	}
	if len(members) == 0 {
		return []*v1alpha1.Model{}, nil
	}

	keys := make([]string, len(members))
	for i, m := range members {
		parts := strings.SplitN(m, ":", 2)
		if len(parts) == 2 {
			keys[i] = s.modelKey(parts[0], parts[1])
		} else {
			keys[i] = s.modelKey("default", m)
		}
	}

	vals, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("batch fetching models: %w", err)
	}

	models := make([]*v1alpha1.Model, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		str, ok := v.(string)
		if !ok {
			continue
		}
		var m v1alpha1.Model
		if err := jsonUnmarshalOpts.Unmarshal([]byte(str), &m); err == nil {
			models = append(models, &m)
		}
	}
	return models, nil
}

// GetModel retrieves a model by atespace and name.
func (s *Store) GetModel(ctx context.Context, atespace, name string) (*v1alpha1.Model, error) {
	if atespace == "" {
		atespace = "default"
	}
	val, err := s.client.Get(ctx, s.modelKey(atespace, name)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting model from redis: %w", err)
	}

	var m v1alpha1.Model
	if err := jsonUnmarshalOpts.Unmarshal([]byte(val), &m); err != nil {
		return nil, fmt.Errorf("unmarshaling model: %w", err)
	}
	return &m, nil
}

// SaveWorkspace stores a workspace.
func (s *Store) SaveWorkspace(ctx context.Context, ws *v1alpha1.Workspace) error {
	if ws.Metadata == nil {
		ws.Metadata = &v1alpha1.ObjectMeta{}
	}
	if ws.Metadata.Name == "" {
		return errors.New("workspace name is required")
	}
	if ws.Metadata.Atespace == "" {
		ws.Metadata.Atespace = "default"
	}
	if ws.ApiVersion == "" {
		ws.ApiVersion = v1alpha1.APIVersion
	}
	if ws.Kind == "" {
		ws.Kind = v1alpha1.KindWorkspace
	}

	data, err := protojson.Marshal(ws)
	if err != nil {
		return fmt.Errorf("marshaling workspace: %w", err)
	}

	atespace := ws.Metadata.Atespace
	name := ws.Metadata.Name
	score := float64(time.Now().UnixNano())
	member := fmt.Sprintf("%s:%s", atespace, name)

	pipe := s.client.TxPipeline()
	pipe.Set(ctx, s.wsKey(atespace, name), data, 0)
	pipe.ZAdd(ctx, s.wsIndexKey(), redis.Z{Score: score, Member: member})
	pipe.ZAdd(ctx, s.wsAtespaceIndexKey(atespace), redis.Z{Score: score, Member: name})
	_, err = pipe.Exec(ctx)
	return err
}

// DeleteWorkspace removes a workspace.
func (s *Store) DeleteWorkspace(ctx context.Context, atespace, name string) error {
	if atespace == "" {
		atespace = "default"
	}
	member := fmt.Sprintf("%s:%s", atespace, name)

	pipe := s.client.TxPipeline()
	pipe.Del(ctx, s.wsKey(atespace, name))
	pipe.ZRem(ctx, s.wsIndexKey(), member)
	pipe.ZRem(ctx, s.wsAtespaceIndexKey(atespace), name)
	_, err := pipe.Exec(ctx)
	return err
}

// ListWorkspaces lists workspaces for an atespace or across all atespaces.
func (s *Store) ListWorkspaces(ctx context.Context, atespace string) ([]*v1alpha1.Workspace, error) {
	var members []string
	var err error

	if atespace == "" || atespace == "*" {
		members, err = s.client.ZRevRange(ctx, s.wsIndexKey(), 0, -1).Result()
	} else {
		names, nErr := s.client.ZRevRange(ctx, s.wsAtespaceIndexKey(atespace), 0, -1).Result()
		if nErr == nil {
			for _, n := range names {
				members = append(members, fmt.Sprintf("%s:%s", atespace, n))
			}
		}
		err = nErr
	}

	if err != nil {
		return nil, fmt.Errorf("listing workspace index: %w", err)
	}
	if len(members) == 0 {
		return []*v1alpha1.Workspace{}, nil
	}

	keys := make([]string, len(members))
	for i, m := range members {
		parts := strings.SplitN(m, ":", 2)
		if len(parts) == 2 {
			keys[i] = s.wsKey(parts[0], parts[1])
		} else {
			keys[i] = s.wsKey("default", m)
		}
	}

	vals, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("batch fetching workspaces: %w", err)
	}

	workspaces := make([]*v1alpha1.Workspace, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		str, ok := v.(string)
		if !ok {
			continue
		}
		var w v1alpha1.Workspace
		if err := jsonUnmarshalOpts.Unmarshal([]byte(str), &w); err == nil {
			workspaces = append(workspaces, &w)
		}
	}
	return workspaces, nil
}

// GetWorkspace retrieves a workspace by atespace and name.
func (s *Store) GetWorkspace(ctx context.Context, atespace, name string) (*v1alpha1.Workspace, error) {
	if atespace == "" {
		atespace = "default"
	}
	val, err := s.client.Get(ctx, s.wsKey(atespace, name)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting workspace from redis: %w", err)
	}

	var w v1alpha1.Workspace
	if err := jsonUnmarshalOpts.Unmarshal([]byte(val), &w); err != nil {
		return nil, fmt.Errorf("unmarshaling workspace: %w", err)
	}
	return &w, nil
}

// Subscribe joins a Redis Streams consumer group and saves its state.
// A group with no prior state starts at the tail of the stream.
// A group from a saved checkpoint keeps its position and pending entries.
func (s *Store) Subscribe(ctx context.Context, group, consumer string) (store.Subscription, error) {
	if _, err := s.consumerGroupCheckpoint(ctx, group, "subscribe", ""); err != nil {
		return nil, fmt.Errorf("subscribing to consumer group %q: %w", group, err)
	}
	return &subscription{store: s, group: group, consumer: consumer, claimCursor: "0-0"}, nil
}

// A group checkpoint retains its cursor and pending entries across group loss.
// Save the pending entry before dispatch so recovery can restore it for a late ACK.
// Save the checkpoint and ACK in the same script to exclude acknowledged entries from recovery.
var checkpointGroup = redis.NewScript(`
local stream, checkpoint = KEYS[1], KEYS[2]
local group, operation, id = ARGV[1], ARGV[2], ARGV[3]
local streamType = redis.call('TYPE', stream).ok
local checkpointType = redis.call('TYPE', checkpoint).ok
if (streamType ~= 'none' and streamType ~= 'stream') or
   (checkpointType ~= 'none' and checkpointType ~= 'string') then
    return redis.error_reply('WRONGTYPE invalid stream or group checkpoint type')
end

local function field(info, name)
    for i = 1, #info, 2 do
        if info[i] == name then return info[i + 1] end
    end
end

local position
if streamType == 'stream' then
    for _, info in ipairs(redis.call('XINFO', 'GROUPS', stream)) do
        if field(info, 'name') == group then
            position = field(info, 'last-delivered-id')
            break
        end
    end
end

if operation == 'recover' and position then return 0 end
local deliveries = 0
if operation == 'deliver' then
    local match = redis.call('XPENDING', stream, group, id, id, 1)
    if #match == 0 then return -1 end
    deliveries = match[1][4]
end
if operation == 'ack' then
    local match = redis.call('XPENDING', stream, group, id, id, 1)
    if #match == 0 then return redis.call('XACK', stream, group, id) end
    id = match[1][1]
    if redis.acl_check_cmd and not redis.acl_check_cmd('XACK', stream, group, id) then
        return redis.error_reply('NOPERM cannot acknowledge stream entries')
    end
end

if not position then
    local saved = redis.call('GET', checkpoint)
    if saved then
        local state = cmsgpack.unpack(saved)
        local pending = state[2]
        if #pending > 0 and redis.acl_check_cmd and
           not redis.acl_check_cmd('XCLAIM', stream, group, pending[1][2], 0, pending[1][1], 'FORCE') then
            return redis.error_reply('NOPERM cannot restore pending stream entries')
        end
        redis.call('XGROUP', 'CREATE', stream, group, state[1], 'MKSTREAM')
        for _, entry in ipairs(pending) do
            redis.call('XCLAIM', stream, group, entry[2], 0, entry[1],
                'TIME', entry[3], 'RETRYCOUNT', entry[4], 'FORCE', 'JUSTID')
        end
        return 1
    end
    position = '0-0'
    if operation == 'subscribe' and streamType == 'stream' then
        position = field(redis.call('XINFO', 'STREAM', stream), 'last-generated-id')
    end
    local empty = cmsgpack.pack({position, {}})
    if redis.acl_check_cmd and not redis.acl_check_cmd('SET', checkpoint, empty) then
        return redis.error_reply('NOPERM cannot save the group checkpoint')
    end
    redis.call('XGROUP', 'CREATE', stream, group, position, 'MKSTREAM')
    redis.call('SET', checkpoint, empty)
    return 1
end

local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
local pending, start, page = {}, '-', {}
repeat
    page = redis.call('XPENDING', stream, group, start, '+', 256)
    for _, entry in ipairs(page) do
        if operation ~= 'ack' or entry[1] ~= id then
            entry[3] = math.max(0, now - entry[3])
            pending[#pending + 1] = entry
        end
    end
    if #page == 256 then start = '(' .. page[#page][1] end
until #page < 256

redis.call('SET', checkpoint, cmsgpack.pack({position, pending}))
if operation == 'ack' then return redis.call('XACK', stream, group, id) end
return deliveries
`)

func (s *Store) checkpointKey(group string) string {
	stream := s.opts.StreamName
	return fmt.Sprintf("ax:stream-checkpoint:%d:%s:%s", len(stream), stream, group)
}

func (s *Store) consumerGroupCheckpoint(ctx context.Context, group, operation, id string) (int, error) {
	return checkpointGroup.Run(ctx, s.client, []string{s.opts.StreamName, s.checkpointKey(group)}, group, operation, id).Int()
}

// subscription reads from a consumer group in batches.
// It hands events out one at a time.
// Delivery is at-least-once. An event stays in the group's pending list
// until a caller acks it.
// Another subscription in the group can also reclaim the event with
// XAUTOCLAIM, once the event sits idle for ClaimMinIdle.
// This can happen, for example, when the consumer that read the event dies
// before it acks the event.
type subscription struct {
	store    *Store
	group    string
	consumer string
	pending  []store.TaskEvent

	// claimCursor is the XAUTOCLAIM scan cursor. "0-0" means the previous
	// scan finished a full pass over the pending list.
	claimCursor string
	// lastClaim is the time claimCursor last returned to "0-0". It throttles
	// how often an empty scan repeats.
	lastClaim time.Time
}

func (sub *subscription) restoreConsumerGroup(ctx context.Context) error {
	created, err := sub.store.consumerGroupCheckpoint(ctx, sub.group, "recover", "")
	if err != nil {
		return fmt.Errorf("restoring consumer group %q: %w", sub.group, err)
	}
	sub.claimCursor = "0-0"
	sub.lastClaim = time.Now()
	if created == 1 {
		slog.Warn("restored Redis consumer group", "stream", sub.store.opts.StreamName, "group", sub.group)
	}
	return nil
}

func (sub *subscription) Next(ctx context.Context) (store.TaskEvent, error) {
	for {
		if err := ctx.Err(); err != nil {
			return store.TaskEvent{}, err
		}
		if len(sub.pending) == 0 {
			events, err := sub.read(ctx)
			if err != nil {
				return store.TaskEvent{}, err
			}
			sub.pending = events
			continue
		}
		ev := sub.pending[0]
		deliveries, err := sub.store.consumerGroupCheckpoint(ctx, sub.group, "deliver", ev.ID)
		if redis.HasErrorPrefix(err, "NOGROUP") {
			if err = sub.restoreConsumerGroup(ctx); err == nil {
				continue
			}
		}
		if err != nil {
			return store.TaskEvent{}, fmt.Errorf("saving pending task event %s: %w", ev.ID, err)
		}
		sub.pending = sub.pending[1:]
		if deliveries < 0 {
			continue
		}
		if int64(deliveries) <= sub.store.opts.MaxDeliveries {
			return ev, nil
		}
		if err := sub.Ack(ctx, ev); err != nil {
			return store.TaskEvent{}, fmt.Errorf("acking poison task event %s: %w", ev.ID, err)
		}
		slog.Error("dropping task event after too many deliveries",
			"id", ev.ID, "atespace", ev.Atespace, "name", ev.Name, "action", ev.Action, "deliveries", deliveries)
	}
}

// read returns the next batch of events. When reclaim is on (ClaimMinIdle >
// 0), it first tries an XAUTOCLAIM scan for entries idle longer than
// ClaimMinIdle; this picks up entries a dead consumer read but never acked.
// Only when that finds nothing does it fall back to one blocking XREADGROUP
// call for fresh entries. It returns an empty slice, not an error, when the
// block time elapses without events.
func (sub *subscription) read(ctx context.Context) ([]store.TaskEvent, error) {
	if sub.store.opts.ClaimMinIdle > 0 {
		events, err := sub.claim(ctx)
		if err != nil {
			return nil, err
		}
		if len(events) > 0 {
			return events, nil
		}
	}
	streams, err := sub.store.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    sub.group,
		Consumer: sub.consumer,
		Streams:  []string{sub.store.opts.StreamName, ">"},
		Count:    sub.store.opts.ReadBatchSize,
		Block:    sub.store.opts.ReadBlock,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		if redis.HasErrorPrefix(err, "NOGROUP") {
			return nil, sub.restoreConsumerGroup(ctx)
		}
		return nil, fmt.Errorf("reading task events: %w", err)
	}

	var events []store.TaskEvent
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			events = append(events, eventFromMessage(msg))
		}
	}
	return events, nil
}

// claim scans the group's pending list for entries idle longer than
// ClaimMinIdle and reassigns them to this consumer with XAUTOCLAIM. It
// resumes the scan from sub.claimCursor, so repeated calls walk the whole
// pending list over several calls instead of rescanning it each time. Once a
// scan completes a full pass (the returned cursor is "0-0"), claim waits at
// least ClaimInterval before scanning again, so an idle group does not poll
// XAUTOCLAIM in a tight loop.
func (sub *subscription) claim(ctx context.Context) ([]store.TaskEvent, error) {
	if sub.claimCursor == "0-0" && time.Since(sub.lastClaim) < sub.store.opts.ClaimInterval {
		return nil, nil
	}
	msgs, cursor, err := sub.store.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   sub.store.opts.StreamName,
		Group:    sub.group,
		Consumer: sub.consumer,
		MinIdle:  sub.store.opts.ClaimMinIdle,
		Start:    sub.claimCursor,
		Count:    sub.store.opts.ReadBatchSize,
	}).Result()
	if err != nil {
		if redis.HasErrorPrefix(err, "NOGROUP") {
			return nil, sub.restoreConsumerGroup(ctx)
		}
		return nil, fmt.Errorf("reclaiming idle task events: %w", err)
	}
	sub.claimCursor = cursor
	if cursor == "0-0" {
		sub.lastClaim = time.Now()
	}
	if len(msgs) == 0 {
		return nil, nil
	}

	events := make([]store.TaskEvent, 0, len(msgs))
	ids := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		ev := eventFromMessage(msg)
		events = append(events, ev)
		ids = append(ids, ev.ID)
	}
	slog.Info("reclaimed idle task events", "consumer", sub.consumer, "count", len(events), "ids", ids)
	return events, nil
}

func (sub *subscription) Ack(ctx context.Context, ev store.TaskEvent) error {
	_, err := sub.store.consumerGroupCheckpoint(ctx, sub.group, "ack", ev.ID)
	if redis.HasErrorPrefix(err, "NOGROUP") {
		if err := sub.restoreConsumerGroup(ctx); err != nil {
			return err
		}
		_, err = sub.store.consumerGroupCheckpoint(ctx, sub.group, "ack", ev.ID)
	}
	return err
}

// Close releases the subscription. It deliberately leaves the consumer
// registered, so another subscription in the group can reclaim with
// XAUTOCLAIM any event this consumer read but did not ack.
func (sub *subscription) Close() error {
	return nil
}

// eventFromMessage decodes a stream entry. "namespace" is accepted as a legacy
// alias for "atespace".
func eventFromMessage(msg redis.XMessage) store.TaskEvent {
	ev := store.TaskEvent{ID: msg.ID}
	if atespace, ok := msg.Values["atespace"].(string); ok {
		ev.Atespace = atespace
	} else if ns, ok := msg.Values["namespace"].(string); ok {
		ev.Atespace = ns
	}
	if name, ok := msg.Values["name"].(string); ok {
		ev.Name = name
	}
	if action, ok := msg.Values["action"].(string); ok {
		ev.Action = action
	}
	ev.Deletion.ActorUID, _ = msg.Values["actor_uid"].(string)
	ev.Deletion.CreationTimestamp, _ = msg.Values["creation_timestamp"].(string)
	if retry, ok := msg.Values["retry"].(string); ok {
		ev.Retry = retry == "1"
	}
	return ev
}

// WatchTask subscribes to status change notifications for a specific task.
func (s *Store) WatchTask(ctx context.Context, atespace, name string) (<-chan *v1alpha1.Task, io.Closer, error) {
	if atespace == "" {
		atespace = "default"
	}
	pubsub := s.client.Subscribe(ctx, s.taskPubSubChannel(atespace, name))
	_, err := pubsub.Receive(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribing to task watch: %w", err)
	}

	ch := make(chan *v1alpha1.Task, 10)
	go func() {
		defer close(ch)
		msgCh := pubsub.Channel()
		for msg := range msgCh {
			var t v1alpha1.Task
			if err := jsonUnmarshalOpts.Unmarshal([]byte(msg.Payload), &t); err == nil {
				ch <- &t
			}
		}
	}()

	return ch, pubsub, nil
}

// Close closes the underlying Redis client connection.
func (s *Store) Close() error {
	return s.client.Close()
}
