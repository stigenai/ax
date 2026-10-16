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
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DefaultLivenessThreshold is how many consecutive stale misses a Running
// task tolerates before SweepOnce marks it Failed.
const DefaultLivenessThreshold = 3

// reasonActorLost and reasonActorMissing are the Ready condition reasons
// SweepOnce sets on a task it fails. reasonActorMissing applies when
// Substrate no longer knows the actor at all; reasonActorLost applies when
// Substrate still reports it RUNNING on the same worker that rejected it.
const (
	reasonActorLost    = "ActorLost"
	reasonActorMissing = "ActorMissing"
)

// LivenessChecker probes every Running task's actor through atenet-router.
// A worker that restarts its container in place keeps the same pod IP but
// loses its activation map, so atenet-router answers every request for an
// actor it held with a 421 forever (ax-lox). SweepOnce finds this case and
// moves the task to Failed with a reason, instead of leaving it Running
// with a route that can never succeed.
type LivenessChecker struct {
	Store     store.Store
	Substrate *substrate.Client
	// RouterAddr is atenet-router's host:port. An empty value means the
	// sweep is disabled: SweepOnce probes nothing.
	RouterAddr string
	// HTTPClient probes atenet-router's /readyz. The default has a 5s
	// timeout; tests override it to run fast.
	HTTPClient *http.Client
	// Threshold is how many consecutive stale misses mark a task Failed.
	// Zero means DefaultLivenessThreshold.
	Threshold int

	mu     sync.Mutex
	misses map[string]int // "atespace/name" -> consecutive stale-miss count
}

// NewLivenessChecker builds a LivenessChecker with the default HTTP timeout
// and threshold.
func NewLivenessChecker(s store.Store, sc *substrate.Client, routerAddr string) *LivenessChecker {
	return &LivenessChecker{
		Store:      s,
		Substrate:  sc,
		RouterAddr: routerAddr,
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Threshold:  DefaultLivenessThreshold,
		misses:     make(map[string]int),
	}
}

// SweepOnce probes every Running task once. It pages ListTasks the same way
// requeueStrandedTasks does (worker.go), so one call bounds memory even
// with many tasks. It logs and continues past a listing error.
func (l *LivenessChecker) SweepOnce(ctx context.Context) {
	threshold := l.Threshold
	if threshold <= 0 {
		threshold = DefaultLivenessThreshold
	}
	var offset int64
	for {
		tasks, err := l.Store.ListTasks(ctx, "*", strandedTasksPageSize, offset)
		if err != nil {
			slog.Warn("task liveness sweep: failed to list tasks", "error", err)
			return
		}
		for _, task := range tasks {
			l.probeOne(ctx, task, threshold)
		}
		if int64(len(tasks)) < strandedTasksPageSize {
			return
		}
		offset += strandedTasksPageSize
	}
}

// probeResult classifies one atenet-router response.
type probeResult int

const (
	// probeInconclusive covers a 502, a timeout, a connection error, and a
	// 421 with no X-Ate-Assignment-Stale header: none of these prove the
	// actor's worker lost it, so they never count toward the threshold.
	probeInconclusive probeResult = iota
	probeHealthy
	probeStale
)

func (l *LivenessChecker) probeRouter(ctx context.Context, atespace, actor string) probeResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/readyz", l.RouterAddr), nil)
	if err != nil {
		return probeInconclusive
	}
	req.Header.Set("ate-target-actor", fmt.Sprintf("%s/%s", atespace, actor))
	resp, err := l.HTTPClient.Do(req)
	if err != nil {
		return probeInconclusive
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return probeHealthy
	}
	if resp.StatusCode == http.StatusMisdirectedRequest && resp.Header.Get("X-Ate-Assignment-Stale") == "true" {
		return probeStale
	}
	return probeInconclusive
}

// probeOne probes one task's actor and updates its miss count and, when
// warranted, its stored status. It never probes a task outside phase
// Running.
func (l *LivenessChecker) probeOne(ctx context.Context, task *v1alpha1.Task, threshold int) {
	if task.GetStatus().GetPhase() != "Running" {
		return
	}
	atespace := task.GetMetadata().GetAtespace()
	name := task.GetMetadata().GetName()
	actor := task.GetStatus().GetActor()
	workerIP := task.GetStatus().GetWorkerIp()
	key := fmt.Sprintf("%s/%s", atespace, name)

	if actor == "" || l.RouterAddr == "" {
		return
	}

	result := l.probeRouter(ctx, atespace, actor)
	if result == probeInconclusive {
		// Cannot tell the actor's fate from this response: reset, so a
		// transient blip never adds to a run of real stale misses.
		l.resetMiss(key)
		return
	}

	// Both a healthy and a stale result confirm against Substrate: the
	// miss path needs it to rule out a router hiccup, and the healthy path
	// uses it to notice the actor already moved worker so WorkerIp can
	// catch up. See docs/networking.md.
	getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	actorStatus, err := l.Substrate.GetActor(getCtx, atespace, actor)
	cancel()
	notFound := status.Code(err) == codes.NotFound
	if err != nil && !notFound {
		slog.Warn("task liveness sweep: GetActor failed", "atespace", atespace, "name", name, "actor", actor, "error", err)
		l.resetMiss(key)
		return
	}
	running := actorStatus.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING
	currentWorkerIP := actorStatus.GetStatus().GetWorkerAssignment().GetWorkerPodIp()

	if result == probeHealthy {
		l.resetMiss(key)
		if running && currentWorkerIP != "" && currentWorkerIP != workerIP {
			l.refreshWorkerIP(ctx, task, currentWorkerIP)
		}
		return
	}

	// result == probeStale: a miss only when Substrate still shows the
	// actor RUNNING on the same worker that just rejected it, or gone
	// entirely. Any other Substrate state (CRASHED, a different worker, a
	// non-RUNNING state) means Substrate already moved on, so this stale
	// reply is stale information on ax's side, not a lost actor.
	switch {
	case notFound:
		l.countMissAndMaybeFail(ctx, task, key, reasonActorMissing,
			fmt.Sprintf("actor %s is no longer known to Substrate (last worker %s)", actor, workerIP), threshold)
	case running && currentWorkerIP == workerIP:
		l.countMissAndMaybeFail(ctx, task, key, reasonActorLost,
			fmt.Sprintf("actor %s lost its worker %s; the worker container likely restarted", actor, workerIP), threshold)
	default:
		l.resetMiss(key)
		if running && currentWorkerIP != "" && currentWorkerIP != workerIP {
			l.refreshWorkerIP(ctx, task, currentWorkerIP)
		}
	}
}

func (l *LivenessChecker) resetMiss(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.misses, key)
}

// countMissAndMaybeFail records one more stale miss for key and, once it
// reaches threshold, tries to move the task to Failed. The write is
// conditional on the task still being Running with the same actor and
// WorkerIp this probe observed, so a task that changed in between (a new
// reconcile, a suspend, a delete) is never overwritten with stale
// information.
func (l *LivenessChecker) countMissAndMaybeFail(ctx context.Context, task *v1alpha1.Task, key, reason, message string, threshold int) {
	l.mu.Lock()
	l.misses[key]++
	count := l.misses[key]
	l.mu.Unlock()

	atespace := task.GetMetadata().GetAtespace()
	name := task.GetMetadata().GetName()
	actor := task.GetStatus().GetActor()
	workerIP := task.GetStatus().GetWorkerIp()

	slog.Warn("task actor probe stale", "atespace", atespace, "name", name, "actor", actor, "workerIP", workerIP, "misses", count)

	if count < threshold {
		return
	}

	expect := func(cur *v1alpha1.TaskStatus) bool {
		return cur.GetPhase() == "Running" && cur.GetActor() == actor && cur.GetWorkerIp() == workerIP
	}
	failed := proto.Clone(task.Status).(*v1alpha1.TaskStatus)
	failed.Phase = "Failed"
	failed.WorkerIp = ""
	setStatusCondition(failed, condReady, "False", reason,
		fmt.Sprintf("%s (misses=%d)", message, count), time.Now())

	applied, err := l.Store.UpdateTaskStatusIf(ctx, atespace, name, expect, failed)
	if err != nil {
		slog.Warn("task liveness sweep: failed to mark task Failed", "atespace", atespace, "name", name, "error", err)
		return
	}
	if !applied {
		// The task changed between this probe and the write: a fresh
		// reconcile, suspend or delete already moved it on. Leave the miss
		// count as is; the next sweep re-probes the task's current state.
		return
	}
	slog.Error("marking task Failed: actor lost", "atespace", atespace, "name", name, "actor", actor, "workerIP", workerIP, "reason", reason, "misses", count)
	l.resetMiss(key)
}

// refreshWorkerIP updates a Running task's WorkerIp after Substrate reports
// the actor running on a different worker than the task's stored status
// shows. ax ssh never reads WorkerIp for routing, but other readers (ax get
// task, the factory's poll) do, so a stale value is still worth correcting
// when a healthy probe gives the chance.
func (l *LivenessChecker) refreshWorkerIP(ctx context.Context, task *v1alpha1.Task, newWorkerIP string) {
	atespace := task.GetMetadata().GetAtespace()
	name := task.GetMetadata().GetName()
	actor := task.GetStatus().GetActor()
	oldWorkerIP := task.GetStatus().GetWorkerIp()

	updated := proto.Clone(task.Status).(*v1alpha1.TaskStatus)
	updated.WorkerIp = newWorkerIP

	expect := func(cur *v1alpha1.TaskStatus) bool {
		return cur.GetPhase() == "Running" && cur.GetActor() == actor && cur.GetWorkerIp() == oldWorkerIP
	}
	applied, err := l.Store.UpdateTaskStatusIf(ctx, atespace, name, expect, updated)
	if err != nil {
		slog.Warn("task liveness sweep: failed to refresh WorkerIp", "atespace", atespace, "name", name, "error", err)
		return
	}
	if applied {
		slog.Info("task liveness sweep: refreshed WorkerIp", "atespace", atespace, "name", name, "actor", actor, "oldWorkerIP", oldWorkerIP, "newWorkerIP", newWorkerIP)
	}
}

// setStatusCondition sets or replaces the condition of type condType on
// status. It mirrors TaskReconciler.setCondition, which operates on a Task
// rather than a bare TaskStatus.
func setStatusCondition(taskStatus *v1alpha1.TaskStatus, condType, condStatus, reason, message string, t time.Time) {
	ts := timestamppb.New(t)
	for i, c := range taskStatus.Conditions {
		if c.Type == condType {
			taskStatus.Conditions[i].Status = condStatus
			taskStatus.Conditions[i].Reason = reason
			taskStatus.Conditions[i].Message = message
			taskStatus.Conditions[i].LastTransitionTime = ts
			return
		}
	}
	taskStatus.Conditions = append(taskStatus.Conditions, &v1alpha1.Condition{
		Type:               condType,
		Status:             condStatus,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: ts,
	})
}
