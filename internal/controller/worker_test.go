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

package controller_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestWorkerReconciliation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start in-process mock Substrate server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	// 2. Substrate client
	subClient, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, "default-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	// 3. In-memory store
	memStore := memory.NewStore()

	// Save task
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "worker-task", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image: "ghrc.io/test/img",
		},
	}
	if err := memStore.SaveTask(ctx, task); err != nil {
		t.Fatalf("failed to save task: %v", err)
	}

	// 4. Start the worker in the background
	worker := controller.NewWorker(memStore, reconciler, "test-group", "worker-1")
	go func() {
		_ = worker.Run(ctx)
	}()

	// 5. Poll store until task reaches "Running" phase
	deadline := time.Now().Add(3 * time.Second)
	var finalTask *v1alpha1.Task
	for time.Now().Before(deadline) {
		tItem, err := memStore.GetTask(ctx, "default", "worker-task")
		if err == nil && tItem.Status.Phase == "Running" {
			finalTask = tItem
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalTask == nil {
		t.Fatalf("task did not transition to Running phase in time")
	}

	if finalTask.Status.Actor != "worker-task" {
		t.Errorf("expected actor 'worker-task', got %q", finalTask.Status.Actor)
	}
	if finalTask.Status.WorkerIp != "10.244.1.42" {
		t.Errorf("expected worker IP '10.244.1.42', got %q", finalTask.Status.WorkerIp)
	}
}

func TestWorkerDeletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{actorTemplates: map[string]bool{"doomed-tmpl-0a1b2c3d": true}}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	subClient, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, "default-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	memStore := memory.NewStore()
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "doomed", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Image: "ghcr.io/test/img"},
		Status:   &v1alpha1.TaskStatus{Phase: "Running", Actor: "doomed"},
	}
	if err := memStore.SaveTask(ctx, task); err != nil {
		t.Fatalf("failed to save task: %v", err)
	}
	// Drain the reconcile event SaveTask published so only the delete is processed.
	drain, _ := memStore.Subscribe(ctx, "drain", "drain")
	drainCtx, drainCancel := context.WithTimeout(ctx, time.Second)
	_, _ = drain.Next(drainCtx)
	drainCancel()

	if err := memStore.MarkTaskDeleting(ctx, "default", "doomed"); err != nil {
		t.Fatalf("MarkTaskDeleting failed: %v", err)
	}
	marked, err := memStore.GetTask(ctx, "default", "doomed")
	if err != nil {
		t.Fatalf("GetTask after mark failed: %v", err)
	}
	if marked.Status.Phase != v1alpha1.PhaseTerminating {
		t.Fatalf("expected phase Terminating, got %q", marked.Status.Phase)
	}

	worker := controller.NewWorker(memStore, reconciler, "test-group", "worker-1")
	go func() { _ = worker.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := memStore.GetTask(ctx, "default", "doomed"); err != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := memStore.GetTask(ctx, "default", "doomed"); err == nil {
		t.Fatalf("expected task record to be removed after cleanup")
	}
	if len(mockSrv.deletedActors) != 1 || mockSrv.deletedActors[0] != "doomed" {
		t.Errorf("expected actor 'doomed' deleted, got %v", mockSrv.deletedActors)
	}
	if len(mockSrv.deletedTemplates) != 1 || mockSrv.deletedTemplates[0] != "doomed-tmpl-0a1b2c3d" {
		t.Errorf("expected template deleted, got %v", mockSrv.deletedTemplates)
	}
}

// A full Substrate worker pool must park the task as Pending, not fail it,
// and must retry the resume after a backoff instead of giving up.
func TestWorkerResumeResourceExhaustedRequeues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{resumeErr: status.Error(codes.ResourceExhausted, "no free workers available")}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	subClient, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, "default-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	memStore := memory.NewStore()
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "capacity-task", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Image: "ghcr.io/test/img"},
	}
	if err := memStore.SaveTask(ctx, task); err != nil {
		t.Fatalf("failed to save task: %v", err)
	}

	worker := controller.NewWorker(memStore, reconciler, "test-group", "worker-1")
	worker.BackoffBase = 20 * time.Millisecond
	worker.BackoffMax = 100 * time.Millisecond
	go func() { _ = worker.Run(ctx) }()

	// SaveTask itself defaults a fresh task's phase to "Pending", so wait for
	// the WaitingForCapacity reason, not the phase alone, to be sure this
	// reflects Reconcile's ResourceExhausted handling, not the initial save.
	deadline := time.Now().Add(3 * time.Second)
	var saved *v1alpha1.Task
	for time.Now().Before(deadline) {
		t2, err := memStore.GetTask(ctx, "default", "capacity-task")
		if err == nil {
			for _, c := range t2.Status.Conditions {
				if c.Type == "Ready" && c.Reason == "WaitingForCapacity" {
					saved = t2
				}
			}
		}
		if saved != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if saved == nil {
		t.Fatalf("task never got Ready reason WaitingForCapacity")
	}
	if saved.Status.Phase != "Pending" {
		t.Errorf("expected phase Pending, got %q", saved.Status.Phase)
	}

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if mockSrv.resumeAttempts.Load() >= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected a second resume attempt from the requeued event, got %d", mockSrv.resumeAttempts.Load())
}

// A controller restart must retry every task a previous process left
// waiting for capacity, so none stay stranded.
func TestWorkerRequeuesStrandedCapacityTasksOnStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	subClient, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, "default-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	memStore := memory.NewStore()
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "stranded-task", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Image: "ghcr.io/test/img"},
		Status: &v1alpha1.TaskStatus{
			Phase:      "Pending",
			Conditions: []*v1alpha1.Condition{{Type: "Ready", Status: "False", Reason: "WaitingForCapacity"}},
		},
	}
	if err := memStore.SaveTask(ctx, task); err != nil {
		t.Fatalf("failed to save task: %v", err)
	}
	// Drain the reconcile event SaveTask published, so only the startup
	// sweep's requeue reaches the worker's Run loop.
	drain, _ := memStore.Subscribe(ctx, "drain", "drain")
	drainCtx, drainCancel := context.WithTimeout(ctx, time.Second)
	_, _ = drain.Next(drainCtx)
	drainCancel()

	worker := controller.NewWorker(memStore, reconciler, "test-group", "worker-1")
	go func() { _ = worker.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		t2, err := memStore.GetTask(ctx, "default", "stranded-task")
		if err == nil && t2.Status.Phase == "Running" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("stranded task was not requeued and reconciled on worker start")
}

// A task can already be stuck waiting for capacity, with a memory limit
// above MaxTaskMemory. The worker must fail it on the next reconcile, not
// loop. This also clears a task a previous controller build left stranded.
func TestWorkerFailsTaskAboveMaxMemory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	subClient, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, "default-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond
	reconciler.MaxTaskMemory = resource.MustParse("16Gi")

	memStore := memory.NewStore()
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "too-big-task", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image:     "ghcr.io/test/img",
			Resources: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Memory: "32Gi"}},
		},
		Status: &v1alpha1.TaskStatus{
			Phase:      "Pending",
			Conditions: []*v1alpha1.Condition{{Type: "Ready", Status: "False", Reason: "WaitingForCapacity"}},
		},
	}
	if err := memStore.SaveTask(ctx, task); err != nil {
		t.Fatalf("failed to save task: %v", err)
	}

	worker := controller.NewWorker(memStore, reconciler, "test-group", "worker-1")
	worker.BackoffBase = 20 * time.Millisecond
	worker.BackoffMax = 100 * time.Millisecond
	go func() { _ = worker.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	var failed *v1alpha1.Task
	for time.Now().Before(deadline) {
		t2, err := memStore.GetTask(ctx, "default", "too-big-task")
		if err == nil && t2.Status.Phase == "Failed" {
			failed = t2
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if failed == nil {
		t.Fatalf("task above the memory cap never reached phase Failed")
	}
	var reason string
	for _, c := range failed.Status.Conditions {
		if c.Type == "Ready" {
			reason = c.Reason
		}
	}
	if reason != "MemoryExceedsCapacity" {
		t.Errorf("expected reason MemoryExceedsCapacity, got %q", reason)
	}

	// The early memory-cap check in Reconcile runs before any Substrate
	// call, so ResumeActor is never invoked, and nothing schedules a
	// requeue: the task stays Failed with no second attempt.
	time.Sleep(200 * time.Millisecond)
	if n := mockSrv.resumeAttempts.Load(); n != 0 {
		t.Errorf("expected zero resume attempts, got %d", n)
	}
	stillFailed, err := memStore.GetTask(ctx, "default", "too-big-task")
	if err != nil || stillFailed.Status.Phase != "Failed" {
		t.Errorf("expected the task to stay Failed, got phase %q (err %v)", stillFailed.GetStatus().GetPhase(), err)
	}
}

// A task can be stuck waiting for capacity with a cpu limit above
// MaxTaskCPU. The worker must fail it on the next reconcile (ax-6cj).
func TestWorkerFailsTaskAboveMaxCPU(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	subClient, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, "default-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond
	reconciler.MaxTaskCPU = resource.MustParse("6")

	memStore := memory.NewStore()
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "too-many-cpus-task", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image:     "ghcr.io/test/img",
			Resources: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Cpu: "32"}},
		},
		Status: &v1alpha1.TaskStatus{
			Phase:      "Pending",
			Conditions: []*v1alpha1.Condition{{Type: "Ready", Status: "False", Reason: "WaitingForCapacity"}},
		},
	}
	if err := memStore.SaveTask(ctx, task); err != nil {
		t.Fatalf("failed to save task: %v", err)
	}

	worker := controller.NewWorker(memStore, reconciler, "test-group", "worker-1")
	worker.BackoffBase = 20 * time.Millisecond
	worker.BackoffMax = 100 * time.Millisecond
	go func() { _ = worker.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	var failed *v1alpha1.Task
	for time.Now().Before(deadline) {
		t2, err := memStore.GetTask(ctx, "default", "too-many-cpus-task")
		if err == nil && t2.Status.Phase == "Failed" {
			failed = t2
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if failed == nil {
		t.Fatalf("task above the cpu cap never reached phase Failed")
	}
	var reason string
	for _, c := range failed.Status.Conditions {
		if c.Type == "Ready" {
			reason = c.Reason
		}
	}
	if reason != "CPUExceedsCapacity" {
		t.Errorf("expected reason CPUExceedsCapacity, got %q", reason)
	}

	// The early cpu-cap check in Reconcile runs before any Substrate call,
	// so ResumeActor is never invoked, and nothing schedules a requeue: the
	// task stays Failed with no second attempt.
	time.Sleep(200 * time.Millisecond)
	if n := mockSrv.resumeAttempts.Load(); n != 0 {
		t.Errorf("expected zero resume attempts, got %d", n)
	}
	stillFailed, err := memStore.GetTask(ctx, "default", "too-many-cpus-task")
	if err != nil || stillFailed.Status.Phase != "Failed" {
		t.Errorf("expected the task to stay Failed, got phase %q (err %v)", stillFailed.GetStatus().GetPhase(), err)
	}
}
