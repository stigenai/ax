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

package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// routingRouter is a fake atenet-router plus guest ProcessService. It reads
// the ate-target-actor header on each call and looks up a mutable actor ->
// worker map, so a change to the map between two calls is visible on the
// very next call. StreamProcessOutput writes the resolved worker name to
// stdout, so a test can read the worker a call actually reached.
type routingRouter struct {
	ateenvv1alpha.UnimplementedProcessServiceServer

	mu            sync.Mutex
	actorToWorker map[string]string
	results       map[string]string
	nextID        int
}

func newRoutingRouter() *routingRouter {
	return &routingRouter{actorToWorker: map[string]string{}, results: map[string]string{}}
}

func (r *routingRouter) setWorker(actor, worker string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actorToWorker[actor] = worker
}

func (r *routingRouter) StartProcess(ctx context.Context, _ *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	actor := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("ate-target-actor"); len(vals) > 0 {
			actor = vals[0]
		}
	}
	if idx := strings.Index(actor, "/"); idx >= 0 {
		actor = actor[idx+1:]
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	id := fmt.Sprintf("%d", r.nextID)
	r.results[id] = r.actorToWorker[actor]
	return &ateenvv1alpha.Process{ProcessId: id, State: ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING}, nil
}

func (r *routingRouter) StreamProcessOutput(req *ateenvv1alpha.StreamProcessOutputRequest, stream ateenvv1alpha.ProcessService_StreamProcessOutputServer) error {
	r.mu.Lock()
	worker := r.results[req.GetProcessId()]
	r.mu.Unlock()

	if err := stream.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: []byte(worker)}}); err != nil {
		return err
	}
	return stream.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Exit{
		Exit: &ateenvv1alpha.Process{ProcessId: req.GetProcessId(), State: ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED},
	}})
}

// deadListener accepts and immediately closes a connection, like a TCP port
// still open on a restarted worker pod with no application behind it. It
// counts every accept, so a test can prove ax ssh never dials it.
type deadListener struct {
	net.Listener
	accepts atomic.Int64
}

func newDeadListener(t *testing.T) *deadListener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d := &deadListener{Listener: lis}
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			d.accepts.Add(1)
			_ = conn.Close()
		}
	}()
	return d
}

// TestSSHFollowsRouterNotCachedWorkerIP proves that ax ssh resolves a task's
// worker fresh through atenet-router on every call, instead of trusting the
// WorkerIp cached on the task's status. task.Status.WorkerIp always points
// at a dead listener that never speaks the guest protocol: if ax ssh dials
// it directly, this test fails or hangs. The fake router's actor -> worker
// map changes between the two calls, so the second call must reach the new
// worker.
func TestSSHFollowsRouterNotCachedWorkerIP(t *testing.T) {
	dead := newDeadListener(t)
	defer dead.Close()

	router := newRoutingRouter()
	router.setWorker("task1", "worker-a")

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	grpcServer := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(grpcServer, router)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "task1", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Debug: true},
		Status: &v1alpha1.TaskStatus{
			Phase:    "Running",
			Actor:    "task1",
			WorkerIp: dead.Addr().String(),
		},
	}
	getTask := func(context.Context) (*v1alpha1.Task, error) { return task, nil }
	dialRouter := func() (string, func(), error) { return lis.Addr().String(), nil, nil }

	var stdout bytes.Buffer
	if err := runSSHExec("task1", []string{"true"}, getTask, dialRouter, time.Millisecond, time.Second, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("call 1: runSSHExec returned an error: %v", err)
	}
	if got := stdout.String(); got != "worker-a" {
		t.Fatalf("call 1: got stdout %q, want %q", got, "worker-a")
	}

	router.setWorker("task1", "worker-b")
	stdout.Reset()
	if err := runSSHExec("task1", []string{"true"}, getTask, dialRouter, time.Millisecond, time.Second, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("call 2: runSSHExec returned an error: %v", err)
	}
	if got := stdout.String(); got != "worker-b" {
		t.Fatalf("call 2: got stdout %q, want %q (routing did not re-resolve)", got, "worker-b")
	}

	if n := dead.accepts.Load(); n != 0 {
		t.Fatalf("ax ssh dialed the cached WorkerIp %d time(s); it must only use atenet-router", n)
	}
}

// TestSSHRejectsTaskWithNoActor proves ax ssh refuses a task with no actor
// assigned, instead of requiring a worker IP that routing no longer uses.
func TestSSHRejectsTaskWithNoActor(t *testing.T) {
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "task1", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Debug: true},
		Status:   &v1alpha1.TaskStatus{Phase: "Running"},
	}
	getTask := func(context.Context) (*v1alpha1.Task, error) { return task, nil }
	dialRouter := func() (string, func(), error) {
		t.Fatal("dialRouter must not be called for a task with no actor")
		return "", nil, nil
	}
	err := runSSHExec("task1", []string{"true"}, getTask, dialRouter, time.Millisecond, time.Second, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "actor") {
		t.Fatalf("got err %v, want an error naming the missing actor", err)
	}
}

func TestNewRouterDialerUsesAXRouter(t *testing.T) {
	pf := func() (string, func(), error) {
		t.Fatal("port-forward must not run when AX_ROUTER is set")
		return "", nil, nil
	}
	dial := newRouterDialer("router.example:443", pf)
	addr, cleanup, err := dial()
	if err != nil || addr != "router.example:443" || cleanup != nil {
		t.Fatalf("got %q, cleanup %v, err %v", addr, cleanup != nil, err)
	}
	called := false
	dial = newRouterDialer("", func() (string, func(), error) { called = true; return "", nil, nil })
	_, _, _ = dial()
	if !called {
		t.Fatal("port-forward must run when AX_ROUTER is empty")
	}
}
