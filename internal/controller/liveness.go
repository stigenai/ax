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
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxCrashMessageBytes bounds how much of Substrate's crash message, or a
// liveness probe's own failure message, reaches a task's status. This keeps
// an oversized message from growing the stored record without limit.
const maxCrashMessageBytes = 512

// oomPattern matches a crash message that names an out-of-memory kill.
// Substrate's crash message is the only OOM signal ax has today: nothing
// reads the sandbox's cgroup memory.events (ax-j8u asks Substrate to add a
// direct signal).
var oomPattern = regexp.MustCompile(`(?i)\boom\b|out of memory`)

// classifyActorLoss decides whether a Running task's actor is lost, given a
// Client.GetActor result. It is pure. The same actor and error always give
// the same verdict. So a test can check it with no call to Substrate.
//
// A gRPC NotFound error means the actor record itself is gone. The reason is
// ActorLost. State ACTOR_STATE_CRASHED means the sandbox died. The reason is
// OOMKilled when the crash message matches oomPattern, and ActorCrashed
// otherwise. Any other state, or any other error, is not loss. A RUNNING
// actor instead goes to a liveness probe. A transient Substrate error must
// not fail a live task.
func classifyActorLoss(actor *ateapipb.Actor, getErr error) (reason, message string, lost bool) {
	if getErr != nil {
		if status.Code(getErr) == codes.NotFound {
			return "ActorLost", truncateMessage(getErr.Error()), true
		}
		return "", "", false
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		return "", "", false
	}
	msg := truncateMessage(actor.GetStatus().GetCrash().GetMessage())
	if oomPattern.MatchString(msg) {
		return "OOMKilled", msg, true
	}
	return "ActorCrashed", msg, true
}

// truncateMessage bounds msg to maxCrashMessageBytes, so a status field
// never grows without limit.
func truncateMessage(msg string) string {
	if len(msg) > maxCrashMessageBytes {
		return msg[:maxCrashMessageBytes]
	}
	return msg
}

// readyzProbe is one /readyz response: the HTTP status code and the body.
type readyzProbe struct {
	statusCode int
	body       string
}

// readyzStatusOK is the alive predicate Reconcile's workspace-ready poll
// uses: only a 200 counts, matching its behavior before probeReadyz existed.
func readyzStatusOK(p readyzProbe) bool {
	return p.statusCode == http.StatusOK
}

// readyzAlive is the alive predicate the liveness monitor uses: a 200, or a
// 503 that names workspace setup as the cause, both mean the runner itself
// is alive even though workspace setup has not finished. The 503 body comes
// from internal/metadata/server.go's handleReadyz.
func readyzAlive(p readyzProbe) bool {
	if p.statusCode == http.StatusOK {
		return true
	}
	return p.statusCode == http.StatusServiceUnavailable && strings.Contains(p.body, "workspace initializing")
}

// probeReadyz checks a runner's /readyz endpoint for workerIP: first a
// direct connection, then, if that attempt does not satisfy alive, the
// in-cluster atenet-router with an ate-target-actor header. It stops at the
// first attempt alive accepts, and returns that probe with ok true. When no
// attempt satisfies alive (including when every attempt fails outright, a
// network error), it returns the last probe it got a response for, if any,
// and ok false.
func (r *TaskReconciler) probeReadyz(ctx context.Context, workerIP, atespace, actorName string, alive func(readyzProbe) bool) (readyzProbe, bool) {
	host := workerIP
	port := "80"
	if h, p, err := net.SplitHostPort(workerIP); err == nil {
		host = h
		port = p
	}

	type attempt struct {
		url    string
		header string
	}
	attempts := []attempt{{url: fmt.Sprintf("http://%s:%s/readyz?check=workspace", host, port)}}
	if routerAddr := os.Getenv("ATENET_ROUTER_ADDR"); routerAddr != "" {
		attempts = append(attempts, attempt{
			url:    fmt.Sprintf("http://%s/readyz?check=workspace", routerAddr),
			header: fmt.Sprintf("%s/%s", atespace, actorName),
		})
	}

	var last readyzProbe
	for _, a := range attempts {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.url, nil)
		if err != nil {
			continue
		}
		if a.header != "" {
			req.Header.Set("ate-target-actor", a.header)
		}
		resp, err := r.httpClient.Do(req)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		last = readyzProbe{statusCode: resp.StatusCode, body: string(body)}
		if alive(last) {
			return last, true
		}
	}
	return last, false
}

// monitorPageSize bounds one ListTasks call in a liveness sweep.
const monitorPageSize = 200

// sandboxUnresponsiveMisses and sandboxUnresponsiveWindow bound when the
// monitor fails a RUNNING actor whose runner stops answering /readyz. It
// takes this many consecutive dead probes, spanning at least this long, so
// one slow sweep or a single blip does not fail a live task.
const (
	sandboxUnresponsiveMisses = 3
	sandboxUnresponsiveWindow = 90 * time.Second
)

// livenessMiss tracks one task's run of consecutive dead /readyz probes.
type livenessMiss struct {
	count int
	first time.Time
}

// TaskMonitor watches every Running task's actor and runner, and fails a
// task whose sandbox is gone instead of leaving it Running forever with a
// dead actor underneath it.
type TaskMonitor struct {
	store      store.Store
	reconciler *TaskReconciler

	// Interval is how often Run sweeps every Running task. Interval <= 0
	// disables the monitor: Run returns at once and does nothing.
	Interval time.Duration

	// Now returns the current time. Tests replace it so the
	// sandboxUnresponsiveWindow check needs no real sleep.
	Now func() time.Time

	mu     sync.Mutex
	misses map[string]*livenessMiss
}

// NewTaskMonitor creates a TaskMonitor that reads and writes tasks through s,
// and uses reconciler's Substrate client and HTTP client for its checks.
func NewTaskMonitor(s store.Store, reconciler *TaskReconciler, interval time.Duration) *TaskMonitor {
	return &TaskMonitor{
		store:      s,
		reconciler: reconciler,
		Interval:   interval,
		Now:        time.Now,
		misses:     make(map[string]*livenessMiss),
	}
}

func (m *TaskMonitor) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

// Run sweeps every Running task once per Interval, until ctx is done.
// Interval <= 0 disables the monitor: Run returns at once.
func (m *TaskMonitor) Run(ctx context.Context) {
	if m.Interval <= 0 {
		return
	}
	ticker := time.NewTicker(m.Interval)
	defer ticker.Stop()
	for {
		m.Sweep(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Sweep checks every Running task once. It logs and goes on past a listing
// or per-task error, so one bad page or task cannot stop the sweep.
func (m *TaskMonitor) Sweep(ctx context.Context) {
	var offset int64
	for {
		tasks, err := m.store.ListTasks(ctx, "*", monitorPageSize, offset)
		if err != nil {
			slog.Warn("liveness monitor: failed to list tasks", "error", err)
			return
		}
		for _, task := range tasks {
			if task.GetStatus().GetPhase() != "Running" {
				continue
			}
			m.checkTask(ctx, task)
		}
		if int64(len(tasks)) < monitorPageSize {
			return
		}
		offset += monitorPageSize
	}
}

// missKey identifies one task incarnation's run of dead probes. Including
// status.id means a deleted and recreated task, or a suspended and resumed
// one, starts its count over rather than inheriting a stale one.
func missKey(atespace, name, statusID string) string {
	return atespace + "/" + name + "/" + statusID
}

func (m *TaskMonitor) clearMiss(key string) {
	m.mu.Lock()
	delete(m.misses, key)
	m.mu.Unlock()
}

// recordMiss adds one dead probe for key and returns the resulting count and
// the time of the first dead probe in the current run.
func (m *TaskMonitor) recordMiss(key string, at time.Time) (count int, first time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec := m.misses[key]
	if rec == nil {
		rec = &livenessMiss{first: at}
		m.misses[key] = rec
	}
	rec.count++
	return rec.count, rec.first
}

// checkTask decides whether task's actor is lost or its runner has stopped
// answering, and fails the task when either is true.
func (m *TaskMonitor) checkTask(ctx context.Context, task *v1alpha1.Task) {
	atespace := task.GetMetadata().GetAtespace()
	name := task.GetMetadata().GetName()
	actorName := task.GetStatus().GetActor()
	if actorName == "" {
		actorName = name
	}
	statusID := task.GetStatus().GetId()
	key := missKey(atespace, name, statusID)

	actor, getErr := m.reconciler.client.GetActor(ctx, atespace, actorName)
	if reason, message, lost := classifyActorLoss(actor, getErr); lost {
		m.clearMiss(key)
		m.failTask(ctx, atespace, name, statusID, reason, message)
		return
	}
	if getErr != nil {
		// A transient Substrate error. Leave the task alone; the next sweep
		// tries again.
		return
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		// Only a RUNNING actor gets probed: the router resumes a suspended
		// one, which would make it look alive, or resume it as a side effect.
		m.clearMiss(key)
		return
	}

	workerIP := task.GetStatus().GetWorkerIp()
	if workerIP == "" {
		return
	}
	if _, alive := m.reconciler.probeReadyz(ctx, workerIP, atespace, actorName, readyzAlive); alive {
		m.clearMiss(key)
		return
	}

	count, first := m.recordMiss(key, m.now())
	if count >= sandboxUnresponsiveMisses && m.now().Sub(first) >= sandboxUnresponsiveWindow {
		m.failTask(ctx, atespace, name, statusID, "SandboxUnresponsive", "the task's runner did not answer /readyz")
	}
}

// failTask re-reads the task and, only if it is still the same incarnation
// and still Running, marks it Failed with reason. A task whose status.id
// changed, or that left Running, moved on since the check started: writing
// over that change would race it, so failTask leaves it alone.
func (m *TaskMonitor) failTask(ctx context.Context, atespace, name, statusID, reason, message string) {
	task, err := m.store.GetTask(ctx, atespace, name)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			slog.Warn("liveness monitor: failed to re-read task before failing it", "atespace", atespace, "name", name, "error", err)
		}
		return
	}
	if task.GetStatus().GetId() != statusID || task.GetStatus().GetPhase() != "Running" {
		return
	}

	taskStatus := task.Status
	taskStatus.Phase = "Failed"
	taskStatus.WorkerIp = ""
	m.reconciler.setCondition(task, condReady, "False", reason, truncateMessage(message), m.now())

	if err := m.store.UpdateTaskStatus(ctx, atespace, name, taskStatus); err != nil {
		slog.Warn("liveness monitor: failed to update task status", "atespace", atespace, "name", name, "error", err)
		return
	}
	slog.Warn("task failed by liveness monitor", "atespace", atespace, "name", name, "reason", reason)
}
