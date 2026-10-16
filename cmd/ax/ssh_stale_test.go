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
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// staleRouter is an h2c-wrapped grpc server that answers every call with a
// real HTTP 421 plus X-Ate-Assignment-Stale: true, the exact shape atunnel
// sends when a worker's activation map no longer holds the target actor
// (substrate internal/atunnel/ingress.go lines 553-556), until told to pass
// calls through to the embedded ProcessService.
type staleRouter struct {
	ateenvv1alpha.UnimplementedProcessServiceServer

	stale atomic.Bool
	// startAttempts counts StartProcess attempts only (a stale 421 rejects
	// before StartProcess runs, so the mux's stale branch counts as one
	// attempt too). It excludes StreamProcessOutput, so a single successful
	// retry after one stale rejection reads back as exactly 2, not 3.
	startAttempts atomic.Int64
	grpcSrv       *grpc.Server
	httpSrv       *http.Server
	lis           net.Listener
}

func newStaleRouter(t *testing.T) *staleRouter {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &staleRouter{lis: lis}
	r.stale.Store(true)

	r.grpcSrv = grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(r.grpcSrv, r)

	mux := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if r.stale.Load() {
			r.startAttempts.Add(1)
			w.Header().Set("X-Ate-Assignment-Stale", "true")
			w.WriteHeader(http.StatusMisdirectedRequest)
			return
		}
		r.grpcSrv.ServeHTTP(w, req)
	})
	r.httpSrv = &http.Server{Handler: h2c.NewHandler(mux, &http2.Server{})}
	go r.httpSrv.Serve(lis)
	return r
}

func (r *staleRouter) StartProcess(context.Context, *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	r.startAttempts.Add(1)
	return &ateenvv1alpha.Process{ProcessId: "1", State: ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING}, nil
}

func (r *staleRouter) StreamProcessOutput(_ *ateenvv1alpha.StreamProcessOutputRequest, stream ateenvv1alpha.ProcessService_StreamProcessOutputServer) error {
	return stream.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Exit{
		Exit: &ateenvv1alpha.Process{ProcessId: "1", State: ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED},
	}})
}

func (r *staleRouter) addr() string { return r.lis.Addr().String() }

func (r *staleRouter) close() {
	r.httpSrv.Close()
	r.grpcSrv.Stop()
}

func runningTask(name string) *v1alpha1.Task {
	return &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Debug: true},
		Status:   &v1alpha1.TaskStatus{Phase: "Running", Actor: name},
	}
}

// TestSSHReportsFailedTaskAfterStaleAssignment covers AC2(a): the actor died
// with its worker between ssh's dial and atunnel's reject. ssh re-fetches
// the task once, finds it already Failed with a Ready reason, and reports
// that instead of retrying a task that cannot recover.
func TestSSHReportsFailedTaskAfterStaleAssignment(t *testing.T) {
	router := newStaleRouter(t)
	defer router.close()

	task := runningTask("task1")
	getCalls := 0
	getTask := func(context.Context) (*v1alpha1.Task, error) {
		getCalls++
		if getCalls == 1 {
			return task, nil
		}
		failed := runningTask("task1")
		failed.Status.Phase = "Failed"
		failed.Status.Conditions = []*v1alpha1.Condition{{
			Type:               "Ready",
			Status:             "False",
			Reason:             "ActorLost",
			Message:            "actor task1 lost its worker 10.244.3.47; probe saw 3 stale misses",
			LastTransitionTime: timestamppb.Now(),
		}}
		return failed, nil
	}
	dialRouter := func() (string, func(), error) { return router.addr(), nil, nil }

	err := runSSHExec("task1", []string{"true"}, getTask, dialRouter, time.Millisecond, time.Second, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("got no error for a task that died with its worker")
	}
	for _, want := range []string{"Failed", "ActorLost", "stale misses"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err.Error(), want)
		}
	}
	if getCalls != 2 {
		t.Fatalf("got %d getTask calls, want exactly 2 (initial + one re-fetch)", getCalls)
	}
	if n := router.startAttempts.Load(); n != 1 {
		t.Fatalf("got %d router calls, want exactly 1 (no retry once the task is no longer Running)", n)
	}
}

// TestSSHRetriesOnceThenFailsOnPersistentStaleAssignment covers AC2(b): the
// task stays Running (the controller has not yet caught the lost actor), so
// ssh retries once after retryDelay, then fails naming 421, stale
// assignment and the actor, with at most 2 router attempts total.
func TestSSHRetriesOnceThenFailsOnPersistentStaleAssignment(t *testing.T) {
	router := newStaleRouter(t)
	defer router.close()

	task := runningTask("task1")
	getTask := func(context.Context) (*v1alpha1.Task, error) { return task, nil }
	dialRouter := func() (string, func(), error) { return router.addr(), nil, nil }

	start := time.Now()
	err := runSSHExec("task1", []string{"true"}, getTask, dialRouter, 50*time.Millisecond, time.Second, &bytes.Buffer{}, &bytes.Buffer{})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("got no error for a persistently stale router")
	}
	for _, want := range []string{"421", "stale assignment", "task1", "delete and re-create"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err.Error(), want)
		}
	}
	if n := router.startAttempts.Load(); n != 2 {
		t.Fatalf("got %d router calls, want exactly 2 (1 retry, at most 2 attempts)", n)
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("runSSHExec returned after %s, before its retryDelay elapsed", elapsed)
	}
}

// TestSSHRetrySucceeds covers the recovery half of AC2(b): the router is
// stale on the first call (the old worker) and healthy on the retry (a
// router request that reaches the actor's current worker), so ssh must
// succeed with no error after exactly one retry.
func TestSSHRetrySucceeds(t *testing.T) {
	router := newStaleRouter(t)
	defer router.close()

	task := runningTask("task1")
	getTask := func(context.Context) (*v1alpha1.Task, error) { return task, nil }
	dialRouter := func() (string, func(), error) { return router.addr(), nil, nil }

	go func() {
		time.Sleep(20 * time.Millisecond)
		router.stale.Store(false)
	}()

	err := runSSHExec("task1", []string{"true"}, getTask, dialRouter, 50*time.Millisecond, time.Second, &bytes.Buffer{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("got error %v, want success once the retry reaches a healthy router", err)
	}
	if n := router.startAttempts.Load(); n != 2 {
		t.Fatalf("got %d router calls, want exactly 2", n)
	}
}

// hangingRouter is an h2c-wrapped grpc server whose StartProcess accepts the
// call but never answers, like a router connection that stays open with no
// response. It proves runSSHExec does not hang on it: StartProcess must
// fail within startTimeout.
type hangingRouter struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	grpcSrv *grpc.Server
	httpSrv *http.Server
	lis     net.Listener
}

func newHangingRouter(t *testing.T) *hangingRouter {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &hangingRouter{lis: lis}
	r.grpcSrv = grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(r.grpcSrv, r)
	r.httpSrv = &http.Server{Handler: h2c.NewHandler(r.grpcSrv, &http2.Server{})}
	go r.httpSrv.Serve(lis)
	return r
}

func (r *hangingRouter) StartProcess(ctx context.Context, _ *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *hangingRouter) addr() string { return r.lis.Addr().String() }

func (r *hangingRouter) close() {
	r.httpSrv.Close()
	r.grpcSrv.Stop()
}

// TestSSHRespectsStartTimeout covers AC3 end to end through runSSHExec: a
// router that accepts the connection but never answers StartProcess must
// not hang ax ssh. It must fail within startTimeout (here 200ms).
func TestSSHRespectsStartTimeout(t *testing.T) {
	router := newHangingRouter(t)
	defer router.close()

	task := runningTask("task1")
	getTask := func(context.Context) (*v1alpha1.Task, error) { return task, nil }
	dialRouter := func() (string, func(), error) { return router.addr(), nil, nil }

	start := time.Now()
	err := runSSHExec("task1", []string{"true"}, getTask, dialRouter, time.Millisecond, 200*time.Millisecond, &bytes.Buffer{}, &bytes.Buffer{})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("got no error for a router that never answers StartProcess")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("runSSHExec took %s to fail; it must respect startTimeout (200ms) and not hang", elapsed)
	}
}
