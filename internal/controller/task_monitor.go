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
	"unicode/utf8"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

// reasonActorCrashed and reasonOOMKilled name the Ready condition reason
// for a CRASHED actor. liveness.go defines reasonActorLost and
// reasonActorMissing. classifyActorLoss reuses those two names below for
// the matching GetActor-error case, so TaskMonitor and LivenessChecker
// report the same reason for the same actor-gone state.
const (
	reasonActorCrashed = "ActorCrashed"
	reasonOOMKilled    = "OOMKilled"
)

// classifyActorLoss decides whether a Running task's actor is lost. It
// takes a Client.GetActor result for actorName. It is pure: the same
// inputs always give the same verdict. A test can check it with no call to
// Substrate.
//
// A gRPC NotFound error means Substrate no longer knows the actor. This
// case sets reason reasonActorMissing. liveness.go:44-45 and
// docs/networking.md use this same reason for this same NotFound case.
// State ACTOR_STATE_CRASHED means the sandbox died. A crash message that
// matches oomPattern sets reason reasonOOMKilled. Any other crash message
// sets reason reasonActorCrashed. Any other state, or any other error, is
// not loss. A RUNNING actor instead goes to a liveness probe. A transient
// Substrate error must not fail a live task.
func classifyActorLoss(actorName string, actor *ateapipb.Actor, getErr error) (reason, message string, lost bool) {
	if getErr != nil {
		if status.Code(getErr) == codes.NotFound {
			return reasonActorMissing, fmt.Sprintf("actor %s is no longer known to Substrate", actorName), true
		}
		return "", "", false
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		return "", "", false
	}
	msg := truncateMessage(actor.GetStatus().GetCrash().GetMessage())
	if oomPattern.MatchString(msg) {
		return reasonOOMKilled, msg, true
	}
	return reasonActorCrashed, msg, true
}

// truncateMessage bounds msg to maxCrashMessageBytes, so a status field
// never grows without limit. It never cuts in the middle of a UTF-8 rune:
// a message that splits a multi-byte rune fails protojson's write of the
// Condition (reproduced: "Condition.message contains invalid UTF-8"), which
// would leave the task stuck Running instead of Failed.
func truncateMessage(msg string) string {
	if len(msg) <= maxCrashMessageBytes {
		return msg
	}
	cut := maxCrashMessageBytes
	for cut > 0 && !utf8.ValidString(msg[:cut]) {
		cut--
	}
	return msg[:cut]
}

// readyzProbe is one /readyz response: the HTTP status code and the body.
// staleAssignment marks a 421 that carries X-Ate-Assignment-Stale: true.
// This is a worker-restarted-in-place signal (ax-lox), not proof that the
// sandbox itself died. See probeSandboxViaRouter.
type readyzProbe struct {
	statusCode      int
	body            string
	staleAssignment bool
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

// probeSandboxViaRouter checks a runner's /readyz through atenet-router
// only. It never dials a worker directly.
//
// Substrate's atunnel ingress needs the ate-target-actor header on every
// request (internal/atunnel/ingress.go:519-531). A direct dial has no such
// header, so it always answers 421. The monitor used to try a direct dial
// first, through probeReadyz, and that 421 counted as a dead probe. So
// every RUNNING task failed SandboxUnresponsive after 3 sweeps, whether or
// not its sandbox was still alive. probeSandboxViaRouter fixes this by
// going through the router, which supplies the header, or not probing at
// all (ax-9qu.10).
//
// It returns ok=false with a zero readyzProbe at once when routerAddr is
// empty: the SandboxUnresponsive check needs ATENET_ROUTER_ADDR, the same
// as LivenessChecker's sweep.
func (r *TaskReconciler) probeSandboxViaRouter(ctx context.Context, routerAddr, atespace, actorName string) (readyzProbe, bool) {
	if routerAddr == "" {
		return readyzProbe{}, false
	}
	url := fmt.Sprintf("http://%s/readyz?check=workspace", routerAddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return readyzProbe{}, false
	}
	req.Header.Set("ate-target-actor", fmt.Sprintf("%s/%s", atespace, actorName))
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return readyzProbe{}, false
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	probe := readyzProbe{
		statusCode:      resp.StatusCode,
		body:            string(body),
		staleAssignment: resp.StatusCode == http.StatusMisdirectedRequest && resp.Header.Get("X-Ate-Assignment-Stale") == "true",
	}
	return probe, readyzAlive(probe)
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

// taskMonitorMiss tracks one task's run of consecutive dead /readyz probes.
type taskMonitorMiss struct {
	count int
	first time.Time
}

// TaskMonitor watches every Running task's actor and runner, and fails a
// task whose sandbox is gone instead of leaving it Running forever with a
// dead actor underneath it. This covers an actor Substrate itself reports
// CRASHED or gone (classifyActorLoss), and a RUNNING actor whose runner
// stops answering /readyz (SandboxUnresponsive); LivenessChecker in
// liveness.go covers a third case, a worker that restarted in place and
// rejects every request for an actor it still claims with a 421.
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
	misses map[string]*taskMonitorMiss
}

// NewTaskMonitor creates a TaskMonitor that reads and writes tasks through s,
// and uses reconciler's Substrate client and HTTP client for its checks.
func NewTaskMonitor(s store.Store, reconciler *TaskReconciler, interval time.Duration) *TaskMonitor {
	return &TaskMonitor{
		store:      s,
		reconciler: reconciler,
		Interval:   interval,
		Now:        time.Now,
		misses:     make(map[string]*taskMonitorMiss),
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
//
// After the pass, it prunes misses for every key it did not see as Running
// this sweep: a key for a task that changed phase, or that a delete already
// removed from ListTasks. This mirrors LivenessChecker.pruneMisses
// (ax-lox.8, d254c8f). Without it, a deleted and recreated task with the
// same name, atespace and status.id would start with a stale miss count
// instead of a fresh run (ax-9qu.13).
func (m *TaskMonitor) Sweep(ctx context.Context) {
	seenRunning := make(map[string]bool)
	var offset int64
	for {
		tasks, err := m.store.ListTasks(ctx, "*", monitorPageSize, offset)
		if err != nil {
			slog.Warn("sandbox check monitor: failed to list tasks", "error", err)
			return
		}
		for _, task := range tasks {
			if task.GetStatus().GetPhase() != "Running" {
				continue
			}
			key := taskMonitorMissKey(task.GetMetadata().GetAtespace(), task.GetMetadata().GetName(), task.GetStatus().GetId())
			seenRunning[key] = true
			m.checkTask(ctx, task)
		}
		if int64(len(tasks)) < monitorPageSize {
			break
		}
		offset += monitorPageSize
	}
	m.pruneMisses(seenRunning)
}

// taskMonitorMissKey identifies one task incarnation's run of dead probes.
// Including status.id means a deleted and recreated task, or a suspended
// and resumed one, starts its count over rather than inheriting a stale one.
func taskMonitorMissKey(atespace, name, statusID string) string {
	return atespace + "/" + name + "/" + statusID
}

func (m *TaskMonitor) clearMiss(key string) {
	m.mu.Lock()
	delete(m.misses, key)
	m.mu.Unlock()
}

// pruneMisses drops every miss count whose key is not in seenRunning: a
// task this sweep did not see as Running, whether it moved to another phase
// or a delete already removed it from ListTasks.
func (m *TaskMonitor) pruneMisses(seenRunning map[string]bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.misses {
		if !seenRunning[key] {
			delete(m.misses, key)
		}
	}
}

// recordMiss adds one dead probe for key and returns the resulting count and
// the time of the first dead probe in the current run.
func (m *TaskMonitor) recordMiss(key string, at time.Time) (count int, first time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec := m.misses[key]
	if rec == nil {
		rec = &taskMonitorMiss{first: at}
		m.misses[key] = rec
	}
	rec.count++
	return rec.count, rec.first
}

// getActorTimeout bounds one checkTask's call to Substrate's GetActor, the
// same bound LivenessChecker.probeOne uses (liveness.go:193). Without it, a
// hung Substrate call would stall the whole serial sweep (ax-9qu.11).
const getActorTimeout = 5 * time.Second

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
	key := taskMonitorMissKey(atespace, name, statusID)

	getCtx, cancel := context.WithTimeout(ctx, getActorTimeout)
	actor, getErr := m.reconciler.client.GetActor(getCtx, atespace, actorName)
	cancel()
	if reason, message, lost := classifyActorLoss(actorName, actor, getErr); lost {
		m.clearMiss(key)
		m.failTask(ctx, atespace, name, statusID, reason, message)
		return
	}
	if getErr != nil {
		// A transient Substrate error, or this call's own timeout. Leave the
		// task alone; the next sweep tries again.
		return
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		// Only a RUNNING actor gets probed: the router resumes a suspended
		// one, which would make it look alive, or resume it as a side effect.
		m.clearMiss(key)
		return
	}

	// The SandboxUnresponsive probe goes through atenet-router only, gated
	// on ATENET_ROUTER_ADDR, same as LivenessChecker's sweep (main.go). A
	// direct dial to the worker has no ate-target-actor header, so it
	// always 421s and would fail every RUNNING task after 3 sweeps
	// regardless of the sandbox's real state (ax-9qu.10). With no router
	// address, this check stays off: clear any stale count and return.
	routerAddr := os.Getenv("ATENET_ROUTER_ADDR")
	if routerAddr == "" {
		m.clearMiss(key)
		return
	}
	probe, alive := m.reconciler.probeSandboxViaRouter(ctx, routerAddr, atespace, actorName)
	if alive {
		m.clearMiss(key)
		return
	}
	if probe.staleAssignment {
		// A 421 with X-Ate-Assignment-Stale is LivenessChecker's signal, on
		// its own threshold and timeline (docs/networking.md). Counting it
		// here too would fail the same task for the same symptom on a
		// shorter timeline (ax-9qu.8). Leave it to LivenessChecker.
		m.clearMiss(key)
		return
	}

	count, first := m.recordMiss(key, m.now())
	if count >= sandboxUnresponsiveMisses && m.now().Sub(first) >= sandboxUnresponsiveWindow {
		m.failTask(ctx, atespace, name, statusID, "SandboxUnresponsive", "the task's runner did not answer /readyz")
	}
}

// failTask re-reads the task. It marks the task Failed with reason, but
// only when the task is still the same incarnation and still Running. The
// write to the store happens only when both facts still hold at write time.
// So a task whose status.id changed, or that left Running, between this
// re-read and the write keeps its current state.
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

	failed := proto.Clone(task.Status).(*v1alpha1.TaskStatus)
	failed.Phase = "Failed"
	failed.WorkerIp = ""
	m.reconciler.setCondition(&v1alpha1.Task{Status: failed}, condReady, "False", reason, truncateMessage(message), m.now())

	expect := func(cur *v1alpha1.TaskStatus) bool {
		return cur.GetId() == statusID && cur.GetPhase() == "Running"
	}
	applied, err := m.store.UpdateTaskStatusIf(ctx, atespace, name, expect, failed)
	if err != nil {
		slog.Warn("liveness monitor: failed to update task status", "atespace", atespace, "name", name, "error", err)
		return
	}
	if !applied {
		// The task changed between the re-read above and this write: a
		// fresh reconcile, a suspend, or a delete already moved it on.
		return
	}
	slog.Warn("task failed by liveness monitor", "atespace", atespace, "name", name, "reason", reason)
}
