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
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/resource"
)

type mockControlServer struct {
	ateapipb.UnimplementedControlServer
	workerIP         string
	createdAtespaces []string
	createdActors    []string
	resumedActors    []string
	suspendedActors  []string
	deletedActors    []string
	actorTemplates   map[string]bool
	deletedTemplates []string
	// rejectTemplates makes CreateActorTemplate fail, as Substrate's validation does
	rejectTemplates   error
	lastTemplate      *ateapipb.ActorTemplate
	existingActor     *ateapipb.Actor
	deleteUID         string
	expectedDeleteUID string
	// resumeErr makes ResumeActor fail, as Substrate does when it has no free
	// worker (codes.ResourceExhausted) or for any other reason.
	resumeErr      error
	resumeAttempts atomic.Int32
	// getActorActor and getActorErr, when set, override GetActor's reply.
	// getActorErr takes priority; getActorActor is checked next; with
	// neither set, GetActor falls back to existingActor, so every test
	// written before these fields existed keeps its original behavior.
	getActorActor *ateapipb.Actor
	getActorErr   error
	// getActorDelay, when positive, makes GetActor wait this long, or until
	// the caller's context is done, before replying. This proves a caller
	// bounds the call with its own timeout instead of hanging forever
	// (ax-9qu.11).
	getActorDelay time.Duration
}

// noSecrets is a SecretResolver for tests: it never finds a key and never touches a cluster.
func noSecrets(context.Context, string, string, string) (string, error) {
	return "", nil
}

func (m *mockControlServer) GetActorTemplate(_ context.Context, req *ateapipb.GetActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	ref := req.GetActorTemplate()
	if !m.actorTemplates[ref.GetName()] {
		return nil, status.Error(codes.NotFound, "template not found")
	}
	if m.lastTemplate != nil && m.lastTemplate.GetMetadata().GetName() == ref.GetName() {
		return proto.Clone(m.lastTemplate).(*ateapipb.ActorTemplate), nil
	}
	return &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Name: ref.GetName(), Atespace: ref.GetAtespace()}}, nil
}

func (m *mockControlServer) CreateActorTemplate(_ context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if m.rejectTemplates != nil {
		return nil, m.rejectTemplates
	}
	if m.actorTemplates == nil {
		m.actorTemplates = make(map[string]bool)
	}
	tmpl := req.GetActorTemplate()
	m.lastTemplate = tmpl
	m.actorTemplates[tmpl.GetMetadata().GetName()] = true
	return tmpl, nil
}

func (m *mockControlServer) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	name := ""
	if req.Atespace != nil && req.Atespace.Metadata != nil {
		name = req.Atespace.Metadata.Name
	}
	m.createdAtespaces = append(m.createdAtespaces, name)
	return &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}, nil
}

func (m *mockControlServer) GetActor(ctx context.Context, _ *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	if m.getActorDelay > 0 {
		select {
		case <-time.After(m.getActorDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if m.getActorErr != nil {
		return nil, m.getActorErr
	}
	if m.getActorActor != nil {
		return m.getActorActor, nil
	}
	return m.existingActor, nil
}

func (m *mockControlServer) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	if m.existingActor != nil {
		return nil, status.Error(codes.AlreadyExists, "actor exists")
	}
	name := ""
	if req.Actor != nil && req.Actor.Metadata != nil {
		name = req.Actor.Metadata.Name
	}
	m.createdActors = append(m.createdActors, name)
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
	}, nil
}

func (m *mockControlServer) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	m.resumeAttempts.Add(1)
	if m.resumeErr != nil {
		return nil, m.resumeErr
	}
	name := ""
	if req.Actor != nil {
		name = req.Actor.Name
	}
	m.resumedActors = append(m.resumedActors, name)
	wIP := "10.244.1.42"
	if m.workerIP != "" {
		wIP = m.workerIP
	}
	return &ateapipb.ResumeActorResponse{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Name: name},
			Status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
				WorkerAssignment: &ateapipb.WorkerAssignment{
					WorkerPod:   "worker-pod-1",
					WorkerPodIp: wIP,
				},
			},
		},
		Resumed: true,
	}, nil
}

func (m *mockControlServer) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	name := ""
	if req.Actor != nil {
		name = req.Actor.Name
	}
	m.suspendedActors = append(m.suspendedActors, name)
	return &ateapipb.SuspendActorResponse{}, nil
}

func (m *mockControlServer) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
	m.deleteUID = req.GetOptions().GetUid()
	if m.expectedDeleteUID != "" && m.deleteUID != m.expectedDeleteUID {
		return nil, status.Error(codes.Aborted, "actor UID differs")
	}
	name := req.GetActor().GetName()
	for _, deleted := range m.deletedActors {
		if deleted == name {
			// Substrate returns NotFound for an actor already gone. The ax
			// client treats that as a successful no-op, so a retried delete
			// stays idempotent.
			return nil, status.Error(codes.NotFound, "actor not found")
		}
	}
	m.deletedActors = append(m.deletedActors, name)
	return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}}, nil
}

func TestProtectedDeleteCannotTargetReplacementActor(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	mock := &mockControlServer{expectedDeleteUID: "original-uid", actorTemplates: map[string]bool{"job-tmpl-0a1b2c3d": true}}
	srv := grpc.NewServer()
	ateapipb.RegisterControlServer(srv, mock)
	go srv.Serve(listener)
	defer srv.Stop()
	client, err := substrate.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reconciler := controller.NewTaskReconciler(client, "default-template", "ax-system")
	if err := reconciler.ReconcileDelete(context.Background(), "private", "job", "original-uid"); err != nil {
		t.Fatal(err)
	}
	if mock.deleteUID != "original-uid" || len(mock.deletedActors) != 1 || len(mock.deletedTemplates) != 0 {
		t.Fatal("protected identity or template retention lost")
	}
	mock.expectedDeleteUID = "replacement-uid"
	if err := reconciler.ReconcileDelete(context.Background(), "private", "job", "original-uid"); status.Code(err) != codes.Aborted {
		t.Fatalf("replacement deletion did not fail closed: %v", err)
	}
	if len(mock.deletedActors) != 1 {
		t.Fatal("replacement actor deleted")
	}
}

func (m *mockControlServer) ListActorTemplates(ctx context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	resp := &ateapipb.ListActorTemplatesResponse{}
	for name := range m.actorTemplates {
		resp.ActorTemplates = append(resp.ActorTemplates, &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: name, Atespace: req.GetAtespace()},
		})
	}
	return resp, nil
}

func (m *mockControlServer) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	name := req.GetActorTemplate().GetName()
	delete(m.actorTemplates, name)
	m.deletedTemplates = append(m.deletedTemplates, name)
	return &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Name: name}}, nil
}

func TestTaskReconciler(t *testing.T) {
	ctx := context.Background()

	// 1. Start in-process mock gRPC Substrate server
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

	// 2. Initialize Substrate client
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()

	// 3. Reconcile Task
	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	task := &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata: &v1alpha1.ObjectMeta{
			Name:     "test-task",
			Atespace: "default",
		},
		Spec: &v1alpha1.TaskSpec{
			Image:   "ghrc.io/my-org/my-image",
			Command: []string{"/bin/task-runner"},
		},
		// A client-supplied actor name must not survive: the actor is always
		// named after the task.
		Status: &v1alpha1.TaskStatus{Actor: "not-the-task"},
	}

	reconciled, err := reconciler.Reconcile(ctx, task)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// 4. Validate reconciliation results
	if reconciled.Status.Phase != "Running" {
		t.Errorf("expected phase 'Running', got %q", reconciled.Status.Phase)
	}
	if reconciled.Status.Actor != "test-task" {
		t.Errorf("expected actor 'test-task', got %q", reconciled.Status.Actor)
	}
	if reconciled.Status.WorkerIp != "10.244.1.42" {
		t.Errorf("expected worker IP '10.244.1.42', got %q", reconciled.Status.WorkerIp)
	}

	if got := mockSrv.lastTemplate.GetWorkerSelector(); got != nil {
		t.Fatalf("ordinary task gained a worker selector: %v", got)
	}

	// Verify mock was called
	if len(mockSrv.createdAtespaces) != 1 || mockSrv.createdAtespaces[0] != "default" {
		t.Errorf("expected atespace 'default' created, got %v", mockSrv.createdAtespaces)
	}
	if len(mockSrv.createdActors) != 1 || mockSrv.createdActors[0] != "test-task" {
		t.Errorf("expected actor 'test-task' created, got %v", mockSrv.createdActors)
	}
	if len(mockSrv.resumedActors) != 1 || mockSrv.resumedActors[0] != "test-task" {
		t.Errorf("expected actor 'test-task' resumed, got %v", mockSrv.resumedActors)
	}
}

func TestTaskReconciler_Suspend(t *testing.T) {
	ctx := context.Background()

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

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()

	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	task := &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata: &v1alpha1.ObjectMeta{
			Name:     "suspend-task",
			Atespace: "default",
		},
		Spec: &v1alpha1.TaskSpec{
			Suspend: true,
			Image:   "ghrc.io/my-org/my-image",
		},
	}

	reconciled, err := reconciler.Reconcile(ctx, task, nil)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if reconciled.Status.Phase != "Suspended" {
		t.Errorf("expected phase 'Suspended', got %q", reconciled.Status.Phase)
	}
	if reconciled.Status.WorkerIp != "" {
		t.Errorf("expected empty worker IP, got %q", reconciled.Status.WorkerIp)
	}
	if len(mockSrv.suspendedActors) != 1 || mockSrv.suspendedActors[0] != "suspend-task" {
		t.Errorf("expected actor 'suspend-task' suspended, got %v", mockSrv.suspendedActors)
	}
}

func TestTaskReconciler_WorkspaceReady(t *testing.T) {
	ctx := context.Background()

	// 1. Mock Substrate Control Server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	// 2. Mock Worker readyz HTTP server
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen http: %v", err)
	}
	defer httpLis.Close()

	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	httpServer := &http.Server{Handler: httpMux}
	go httpServer.Serve(httpLis)
	defer httpServer.Close()

	workerHost, workerPortStr, _ := net.SplitHostPort(httpLis.Addr().String())
	// In our mock, the worker IP returned by ResumeActor will have our mock ready server listening.
	// But our reconciler connects to port 9999 by default: fmt.Sprintf("http://%s:9999/readyz", workerIP).
	// If workerIP includes a port or is a host, let's verify how it handles it.
	_ = workerHost
	_ = workerPortStr

	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()

	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	task := &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata: &v1alpha1.ObjectMeta{
			Name:     "ready-task",
			Atespace: "default",
		},
		Spec: &v1alpha1.TaskSpec{},
	}

	// Case 1: Worker not responding on readyz -> WorkspaceReady=False and Ready=False.
	reconciled, err := reconciler.Reconcile(ctx, task, nil)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	assertCondition(t, reconciled, "WorkspaceReady", "False", "Initializing")
	assertCondition(t, reconciled, "Ready", "False", "WorkspaceInitializing")

	// Case 2: Worker readyz endpoint succeeds -> WorkspaceReady=True and Ready=True.
	mockSrv.workerIP = httpLis.Addr().String()
	reconciledReady, err := reconciler.Reconcile(ctx, task, nil)
	if err != nil {
		t.Fatalf("Reconcile with ready worker failed: %v", err)
	}
	assertCondition(t, reconciledReady, "WorkspaceReady", "True", "SetupComplete")
	assertCondition(t, reconciledReady, "Ready", "True", "TaskRunning")

	// Case 3: Suspending the task -> Ready=False (TaskSuspended), but the workspace was
	// already initialized so WorkspaceReady stays True.
	task = reconciledReady
	task.Spec.Suspend = true
	reconciledSuspended, err := reconciler.Reconcile(ctx, task, nil)
	if err != nil {
		t.Fatalf("Reconcile with suspend failed: %v", err)
	}
	if reconciledSuspended.Status.Phase != "Suspended" {
		t.Errorf("expected phase Suspended, got %s", reconciledSuspended.Status.Phase)
	}
	assertCondition(t, reconciledSuspended, "Ready", "False", "TaskSuspended")
	assertCondition(t, reconciledSuspended, "WorkspaceReady", "True", "SetupComplete")

	// Case 4: Resuming with the worker unreachable -> the reconciler trusts the recorded
	// WorkspaceReady instead of re-polling, so the task is Ready again immediately.
	mockSrv.workerIP = "127.0.0.1:1"
	task = reconciledSuspended
	task.Spec.Suspend = false
	reconciledResumed, err := reconciler.Reconcile(ctx, task, nil)
	if err != nil {
		t.Fatalf("Reconcile with resume failed: %v", err)
	}
	assertCondition(t, reconciledResumed, "WorkspaceReady", "True", "SetupComplete")
	assertCondition(t, reconciledResumed, "Ready", "True", "TaskRunning")
	if got := len(mockSrv.actorTemplates); got != 1 {
		t.Fatalf("readiness updates and suspend/resume created %d templates, want 1", got)
	}

	// A launch configuration change must still get a distinct template.
	task.Spec.Command = []string{"python3", "agent.py"}
	if _, err := reconciler.Reconcile(ctx, task, nil); err != nil {
		t.Fatalf("Reconcile with changed command failed: %v", err)
	}
	if got := len(mockSrv.actorTemplates); got != 2 {
		t.Errorf("command change left %d templates, want 2", got)
	}
}

// assertCondition fails the test unless the task has a condition of the given type with
// the expected status and reason.
func assertCondition(t *testing.T, task *v1alpha1.Task, condType, wantStatus, wantReason string) {
	t.Helper()
	for _, c := range task.Status.Conditions {
		if c.Type != condType {
			continue
		}
		if c.Status != wantStatus || c.Reason != wantReason {
			t.Errorf("expected %s=%s (%s), got Status=%s Reason=%s", condType, wantStatus, wantReason, c.Status, c.Reason)
		}
		return
	}
	t.Errorf("expected %s condition to be set", condType)
}

func TestReconcileDelete_RemovesActorAndTemplates(t *testing.T) {
	ctx := context.Background()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{actorTemplates: map[string]bool{
		"job-tmpl-0a1b2c3d":               true, // current revision of task "job"
		"job-tmpl-deadbeef":               true, // stale revision of task "job"
		"job-tmpl-deadbeef-tmpl-01234567": true, // belongs to a task literally named "job-tmpl-deadbeef"
		"jobs-tmpl-0a1b2c3d":              true, // belongs to task "jobs"
		"default-template":                true,
	}}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()

	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	if err := reconciler.ReconcileDelete(ctx, "default", "job"); err != nil {
		t.Fatalf("ReconcileDelete failed: %v", err)
	}

	if len(mockSrv.deletedActors) != 1 || mockSrv.deletedActors[0] != "job" {
		t.Errorf("expected actor 'job' to be deleted, got %v", mockSrv.deletedActors)
	}

	wantDeleted := map[string]bool{"job-tmpl-0a1b2c3d": true, "job-tmpl-deadbeef": true}
	if len(mockSrv.deletedTemplates) != len(wantDeleted) {
		t.Errorf("expected %d templates deleted, got %v", len(wantDeleted), mockSrv.deletedTemplates)
	}
	for _, name := range mockSrv.deletedTemplates {
		if !wantDeleted[name] {
			t.Errorf("unexpected template deleted: %s", name)
		}
	}
	for _, keep := range []string{"job-tmpl-deadbeef-tmpl-01234567", "jobs-tmpl-0a1b2c3d", "default-template"} {
		if !mockSrv.actorTemplates[keep] {
			t.Errorf("template %s should not have been deleted", keep)
		}
	}
}

// newTestReconciler serves mockSrv over gRPC and returns a reconciler bound to it.
func newTestReconciler(t *testing.T, mockSrv *mockControlServer) *controller.TaskReconciler {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond
	return reconciler
}

func envTask(value string) *v1alpha1.Task {
	return &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata:   &v1alpha1.ObjectMeta{Name: "env-task", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image:   "ghcr.io/my-org/my-image",
			Command: []string{"/bin/task-runner"},
			Env:     []*v1alpha1.EnvVar{{Name: "BRIEF", Value: value}},
		},
	}
}

// Each env value reaches the container once, as itself: AX_TASK_YAML does not
// repeat them, so the values together are not held to one variable's limit.
func TestTaskReconciler_TaskYAMLLeavesOutEnv(t *testing.T) {
	mockSrv := &mockControlServer{}
	value := strings.Repeat("b", 20_000)
	if _, err := newTestReconciler(t, mockSrv).Reconcile(context.Background(), envTask(value)); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	env := map[string]string{}
	for _, e := range mockSrv.lastTemplate.GetContainers()[0].GetEnv() {
		env[e.GetName()] = e.GetValue()
	}
	if env["BRIEF"] != value {
		t.Errorf("BRIEF did not reach the template whole")
	}
	if strings.Contains(env["AX_TASK_YAML"], value) {
		t.Errorf("AX_TASK_YAML repeats the env values")
	}
	if !strings.Contains(env["AX_TASK_YAML"], "/bin/task-runner") {
		t.Errorf("AX_TASK_YAML lost the task's command: %q", env["AX_TASK_YAML"])
	}
}

// spec.capabilities reach the container's securityContext, but not AX_TASK_YAML, which a runner built before the field would refuse.
func TestTaskReconciler_Capabilities(t *testing.T) {
	mockSrv := &mockControlServer{}
	task := envTask("x")
	task.Spec.Capabilities = []string{"SETUID", "SETGID"}
	if _, err := newTestReconciler(t, mockSrv).Reconcile(context.Background(), task); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	c := mockSrv.lastTemplate.GetContainers()[0]
	if got := c.GetSecurityContext().GetCapabilities().GetAdd(); strings.Join(got, ",") != "SETUID,SETGID" {
		t.Errorf("securityContext.capabilities.add = %v", got)
	}
	for _, e := range c.GetEnv() {
		if e.GetName() == "AX_TASK_YAML" && strings.Contains(e.GetValue(), "SETUID") {
			t.Errorf("AX_TASK_YAML carries the capabilities: %q", e.GetValue())
		}
	}
}

// taskTopLevelMemory returns the top-level memory limit quantity the mock
// server captured, or "" if the template carries no top-level Resources.
func taskTopLevelMemory(tmpl *ateapipb.ActorTemplate) string {
	for _, l := range tmpl.GetResources().GetLimits() {
		if l.GetName() == "memory" {
			return l.GetQuantity()
		}
	}
	return ""
}

// A task with no memory limit gets the controller's default booked at the
// top level, so Substrate's scheduler sees it. An explicit task limit wins
// over the default, and disabling the default (empty DefaultTaskMemory)
// leaves the task with no top-level memory booking, as before this field
// existed.
func TestTaskReconciler_DefaultTaskMemory(t *testing.T) {
	t.Run("no task limit gets the controller default", func(t *testing.T) {
		mockSrv := &mockControlServer{}
		r := newTestReconciler(t, mockSrv)
		r.DefaultTaskMemory = "8Gi"
		if _, err := r.Reconcile(context.Background(), envTask("x")); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		if got := taskTopLevelMemory(mockSrv.lastTemplate); got != "8Gi" {
			t.Errorf("top-level memory = %q, want 8Gi", got)
		}
	})

	t.Run("an explicit task limit wins over the default", func(t *testing.T) {
		mockSrv := &mockControlServer{}
		r := newTestReconciler(t, mockSrv)
		r.DefaultTaskMemory = "4Gi"
		task := envTask("x")
		task.Spec.Resources = &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Memory: "8Gi"}}
		if _, err := r.Reconcile(context.Background(), task); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		if got := taskTopLevelMemory(mockSrv.lastTemplate); got != "8Gi" {
			t.Errorf("top-level memory = %q, want 8Gi (the task's own limit)", got)
		}
	})

	t.Run("an empty default disables the booking", func(t *testing.T) {
		mockSrv := &mockControlServer{}
		r := newTestReconciler(t, mockSrv)
		r.DefaultTaskMemory = ""
		if _, err := r.Reconcile(context.Background(), envTask("x")); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		if mockSrv.lastTemplate.GetResources() != nil {
			t.Errorf("want nil top-level Resources with the default disabled, got %v", mockSrv.lastTemplate.GetResources())
		}
	})

	t.Run("the default does not change the task's template digest or AX_TASK_YAML", func(t *testing.T) {
		withoutDefault := &mockControlServer{}
		r := newTestReconciler(t, withoutDefault)
		r.DefaultTaskMemory = ""
		if _, err := r.Reconcile(context.Background(), envTask("x")); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		withDefault := &mockControlServer{}
		r2 := newTestReconciler(t, withDefault)
		r2.DefaultTaskMemory = "8Gi"
		if _, err := r2.Reconcile(context.Background(), envTask("x")); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		if withoutDefault.lastTemplate.GetMetadata().GetName() != withDefault.lastTemplate.GetMetadata().GetName() {
			t.Errorf("template name changed: %q vs %q", withoutDefault.lastTemplate.GetMetadata().GetName(), withDefault.lastTemplate.GetMetadata().GetName())
		}
		yamlWithout, yamlWith := "", ""
		for _, e := range withoutDefault.lastTemplate.GetContainers()[0].GetEnv() {
			if e.GetName() == "AX_TASK_YAML" {
				yamlWithout = e.GetValue()
			}
		}
		for _, e := range withDefault.lastTemplate.GetContainers()[0].GetEnv() {
			if e.GetName() == "AX_TASK_YAML" {
				yamlWith = e.GetValue()
			}
		}
		if yamlWithout != yamlWith {
			t.Errorf("AX_TASK_YAML changed with the default applied:\nwithout: %q\nwith:    %q", yamlWithout, yamlWith)
		}
	})

	t.Run("a supervisor task keeps its own memory limit, never the default", func(t *testing.T) {
		// validateSupervisorTask (pkg/apis/v1alpha1/types.go) already requires
		// a supervisor task to name explicit cpu and memory limits, so the
		// default-booking branch (guarded by SupervisorProfile == "") must
		// never run for one. This proves it leaves the supervisor's own
		// 512Mi in place rather than the 4Gi default.
		mockSrv := &mockControlServer{}
		r := newTestReconciler(t, mockSrv)
		r.DefaultTaskMemory = "4Gi"
		if _, err := r.Reconcile(context.Background(), supervisorTask()); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		if got := taskTopLevelMemory(mockSrv.lastTemplate); got != "512Mi" {
			t.Errorf("top-level memory = %q, want the supervisor's own 512Mi, not the 4Gi default", got)
		}
	})
}

// A template Substrate refuses fails the task with Substrate's reason, rather
// than an actor made from a default template that is not this task.
func TestTaskReconciler_TemplateRefusedFailsTask(t *testing.T) {
	mockSrv := &mockControlServer{rejectTemplates: status.Error(codes.InvalidArgument, "env[0].value: Too long")}
	task, err := newTestReconciler(t, mockSrv).Reconcile(context.Background(), envTask("x"))
	if err == nil {
		t.Fatal("Reconcile succeeded with a refused template")
	}
	if task.Status.Phase != "Failed" {
		t.Errorf("expected phase Failed, got %q", task.Status.Phase)
	}
	var reason, message string
	for _, c := range task.Status.Conditions {
		if c.Type == "Ready" {
			reason, message = c.Reason, c.Message
		}
	}
	if reason != "ActorTemplateCreationFailed" || !strings.Contains(message, "Too long") {
		t.Errorf("expected ActorTemplateCreationFailed naming Substrate's reason, got %q: %q", reason, message)
	}
	if len(mockSrv.createdActors) != 0 {
		t.Errorf("an actor was created from another template: %v", mockSrv.createdActors)
	}
}

// A full Substrate worker pool must not fail the task: it must wait and retry.
func TestTaskReconciler_ResumeResourceExhaustedWaitsForCapacity(t *testing.T) {
	mockSrv := &mockControlServer{resumeErr: status.Error(codes.ResourceExhausted, "no free workers available")}
	task, err := newTestReconciler(t, mockSrv).Reconcile(context.Background(), envTask("x"))
	if !errors.Is(err, controller.ErrWaitingForCapacity) {
		t.Fatalf("expected ErrWaitingForCapacity, got %v", err)
	}
	if task.Status.Phase != "Pending" {
		t.Errorf("expected phase Pending, got %q", task.Status.Phase)
	}
	var reason, message string
	for _, c := range task.Status.Conditions {
		if c.Type == "Ready" {
			reason, message = c.Reason, c.Message
		}
	}
	if reason != "WaitingForCapacity" || !strings.Contains(message, "no free workers") {
		t.Errorf("expected WaitingForCapacity naming Substrate's reason, got %q: %q", reason, message)
	}
}

// Any other resume failure still fails the task, as before.
func TestTaskReconciler_ResumeOtherErrorFailsTask(t *testing.T) {
	mockSrv := &mockControlServer{resumeErr: status.Error(codes.Internal, "boom")}
	task, err := newTestReconciler(t, mockSrv).Reconcile(context.Background(), envTask("x"))
	if err == nil || errors.Is(err, controller.ErrWaitingForCapacity) {
		t.Fatalf("expected a plain resume failure, got %v", err)
	}
	if task.Status.Phase != "Failed" {
		t.Errorf("expected phase Failed, got %q", task.Status.Phase)
	}
}

// A task's own memory limit can exceed MaxTaskMemory. Reconcile must fail it
// at once, before any Substrate call. It must not retry forever like a
// capacity wait (ax-9gp).
func TestTaskReconciler_MemoryAboveMaxFailsTask(t *testing.T) {
	mockSrv := &mockControlServer{}
	r := newTestReconciler(t, mockSrv)
	r.MaxTaskMemory = resource.MustParse("16Gi")

	task := envTask("x")
	task.Spec.Resources = &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Memory: "32Gi"}}
	reconciled, err := r.Reconcile(context.Background(), task)
	if err == nil || errors.Is(err, controller.ErrWaitingForCapacity) {
		t.Fatalf("expected a plain memory-cap failure, got %v", err)
	}
	if reconciled.Status.Phase != "Failed" {
		t.Errorf("expected phase Failed, got %q", reconciled.Status.Phase)
	}
	var reason, message string
	for _, c := range reconciled.Status.Conditions {
		if c.Type == "Ready" {
			reason, message = c.Reason, c.Message
		}
	}
	if reason != "MemoryExceedsCapacity" || !strings.Contains(message, "32Gi") || !strings.Contains(message, "16Gi") {
		t.Errorf("expected MemoryExceedsCapacity naming both sizes, got %q: %q", reason, message)
	}
	if len(mockSrv.createdAtespaces) != 0 || len(mockSrv.createdActors) != 0 || mockSrv.resumeAttempts.Load() != 0 {
		t.Errorf("expected zero Substrate calls, got atespaces=%v actors=%v resumeAttempts=%d", mockSrv.createdAtespaces, mockSrv.createdActors, mockSrv.resumeAttempts.Load())
	}
}

// A task that names no memory limit falls back to DefaultTaskMemory.
// Reconcile must check that default against the cap too.
func TestTaskReconciler_DefaultMemoryAboveMaxFailsTask(t *testing.T) {
	mockSrv := &mockControlServer{}
	r := newTestReconciler(t, mockSrv)
	r.MaxTaskMemory = resource.MustParse("16Gi")
	r.DefaultTaskMemory = "32Gi"

	reconciled, err := r.Reconcile(context.Background(), envTask("x"))
	if err == nil || errors.Is(err, controller.ErrWaitingForCapacity) {
		t.Fatalf("expected a plain memory-cap failure from the default, got %v", err)
	}
	if reconciled.Status.Phase != "Failed" {
		t.Errorf("expected phase Failed, got %q", reconciled.Status.Phase)
	}
	if len(mockSrv.createdAtespaces) != 0 {
		t.Errorf("expected zero Substrate calls, got atespaces=%v", mockSrv.createdAtespaces)
	}
}

// A supervisor task ignores DefaultTaskMemory. It always sets its own
// explicit limit; validateSupervisorTask enforces that. So an oversized
// default must not fail it.
func TestTaskReconciler_SupervisorIgnoresDefaultMemoryCap(t *testing.T) {
	mockSrv := &mockControlServer{}
	r := newTestReconciler(t, mockSrv)
	r.MaxTaskMemory = resource.MustParse("16Gi")
	r.DefaultTaskMemory = "32Gi"

	if _, err := r.Reconcile(context.Background(), supervisorTask()); err != nil {
		t.Fatalf("supervisor task with its own 512Mi limit should not be affected by an oversized default: %v", err)
	}
}

// With no cap configured, Reconcile keeps the existing WaitingForCapacity
// path (A6): a task waits for capacity instead of failing, even above a
// size that would exceed a configured cap.
func TestTaskReconciler_NoMaxLeavesCapacityWaitUnchanged(t *testing.T) {
	mockSrv := &mockControlServer{resumeErr: status.Error(codes.ResourceExhausted, "no free workers available")}
	r := newTestReconciler(t, mockSrv)
	task := envTask("x")
	task.Spec.Resources = &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Memory: "32Gi"}}
	reconciled, err := r.Reconcile(context.Background(), task)
	if !errors.Is(err, controller.ErrWaitingForCapacity) {
		t.Fatalf("expected ErrWaitingForCapacity, got %v", err)
	}
	if reconciled.Status.Phase != "Pending" {
		t.Errorf("expected phase Pending, got %q", reconciled.Status.Phase)
	}
}

// Reconcile fails a task at once when its cpu limit exceeds MaxTaskCPU,
// before any Substrate call (ax-6cj).
func TestTaskReconciler_CPUAboveMaxFailsTask(t *testing.T) {
	mockSrv := &mockControlServer{}
	r := newTestReconciler(t, mockSrv)
	r.MaxTaskCPU = resource.MustParse("6")

	task := envTask("x")
	task.Spec.Resources = &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Cpu: "32"}}
	reconciled, err := r.Reconcile(context.Background(), task)
	if err == nil || errors.Is(err, controller.ErrWaitingForCapacity) {
		t.Fatalf("expected a plain cpu-cap failure, got %v", err)
	}
	if reconciled.Status.Phase != "Failed" {
		t.Errorf("expected phase Failed, got %q", reconciled.Status.Phase)
	}
	var reason, message string
	for _, c := range reconciled.Status.Conditions {
		if c.Type == "Ready" {
			reason, message = c.Reason, c.Message
		}
	}
	if reason != "CPUExceedsCapacity" || !strings.Contains(message, "32") || !strings.Contains(message, "6") {
		t.Errorf("expected CPUExceedsCapacity naming both sizes, got %q: %q", reason, message)
	}
	if len(mockSrv.createdAtespaces) != 0 || len(mockSrv.createdActors) != 0 || mockSrv.resumeAttempts.Load() != 0 {
		t.Errorf("expected zero Substrate calls, got atespaces=%v actors=%v resumeAttempts=%d", mockSrv.createdAtespaces, mockSrv.createdActors, mockSrv.resumeAttempts.Load())
	}
}

// A cpu limit equal to MaxTaskCPU passes Reconcile through to Substrate.
func TestTaskReconciler_CPUAtMaxPassesToResume(t *testing.T) {
	mockSrv := &mockControlServer{}
	r := newTestReconciler(t, mockSrv)
	r.MaxTaskCPU = resource.MustParse("6")

	task := envTask("x")
	task.Spec.Resources = &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Cpu: "6"}}
	if _, err := r.Reconcile(context.Background(), task); err != nil {
		t.Fatalf("expected a task at the cpu cap to reach Substrate, got %v", err)
	}
	if mockSrv.resumeAttempts.Load() == 0 {
		t.Errorf("expected a ResumeActor attempt, got none")
	}
}

// With no cpu cap set, Reconcile keeps the old WaitingForCapacity path.
func TestTaskReconciler_NoMaxCPULeavesCapacityWaitUnchanged(t *testing.T) {
	mockSrv := &mockControlServer{resumeErr: status.Error(codes.ResourceExhausted, "no free workers available")}
	r := newTestReconciler(t, mockSrv)
	task := envTask("x")
	task.Spec.Resources = &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Cpu: "32"}}
	reconciled, err := r.Reconcile(context.Background(), task)
	if !errors.Is(err, controller.ErrWaitingForCapacity) {
		t.Fatalf("expected ErrWaitingForCapacity, got %v", err)
	}
	if reconciled.Status.Phase != "Pending" {
		t.Errorf("expected phase Pending, got %q", reconciled.Status.Phase)
	}
}

func supervisorTask() *v1alpha1.Task {
	return &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "fabriek-task", Atespace: "fabriek"}, Spec: &v1alpha1.TaskSpec{
		SupervisorProfile: v1alpha1.FabriekSupervisorProfile,
		Image:             "ghcr.io/stigenai/native@sha256:" + strings.Repeat("a", 64),
		Command:           []string{"/usr/local/bin/python", "-m", "factory.execution.ax_worker", "serve"},
		Env:               []*v1alpha1.EnvVar{{Name: "FACTORY_ATTEMPT_ID", Value: "11111111-1111-4111-8111-111111111111"}, {Name: "FACTORY_NATIVE_LAUNCH_DIGEST", Value: "sha256:" + strings.Repeat("b", 64)}},
		Resources:         &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Cpu: "500m", Memory: "512Mi"}},
		Workspaces:        []*v1alpha1.WorkspaceRef{{Name: "session", Path: "/workspace/session"}},
	}}
}

func TestFabriekSupervisorProjection(t *testing.T) {
	mock := &mockControlServer{}
	r := newTestReconciler(t, mock)
	r.SecretResolver = func(context.Context, string, string, string) (string, error) {
		t.Fatal("protected task looked up shared credentials")
		return "", nil
	}
	t.Setenv("GEMINI_API_KEY", "must-not-reach-template")
	task := supervisorTask()
	if _, err := r.Reconcile(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	tmpl := mock.lastTemplate
	wantSelector := &ateapipb.Selector{MatchLabels: map[string]string{
		"fabriek.dev/supervisor-profile": v1alpha1.FabriekSupervisorProfile,
	}}
	if !proto.Equal(tmpl.GetWorkerSelector(), wantSelector) {
		t.Fatalf("protected worker placement missing: %v", tmpl.GetWorkerSelector())
	}
	if len(tmpl.GetVolumes()) != 2 {
		t.Fatalf("volumes: %v", tmpl.GetVolumes())
	}
	projection := tmpl.GetVolumes()[1].GetSystemInfo().GetDataSources()
	if len(projection) != 1 || projection[0].GetSupervisorBootstrap().GetProfile() != v1alpha1.FabriekSupervisorProfile {
		t.Fatal("protected bootstrap missing")
	}
	guest := tmpl.GetContainers()[0]
	if got := guest.GetSecurityContext().GetCapabilities().GetAdd(); strings.Join(got, ",") != "CHOWN,DAC_OVERRIDE,FOWNER,SETGID,SETUID" {
		t.Fatalf("unexpected protected supervisor capabilities: %v", got)
	}
	if guest.GetVolumeMounts()[1].GetMountPath() != "/run/substrate/fabriek-supervisor" {
		t.Fatal("wrong protected mount")
	}
	if got := guest.GetResources().GetLimits(); len(got) != 2 || got[0].GetQuantity() != "500m" || got[1].GetQuantity() != "512Mi" {
		t.Fatalf("limits not enforced: %v", got)
	}
	for _, e := range guest.GetEnv() {
		if e.GetName() == "GEMINI_API_KEY" || strings.Contains(e.GetValue(), "must-not-reach-template") {
			t.Fatal("shared credential injected")
		}
	}
	snapshot := tmpl.GetSnapshotConfig()
	if snapshot.GetOnPause() != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA || snapshot.GetOnCommit() != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA || snapshot.GetOnResume().GetFromData() != ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN {
		t.Fatal("unsafe snapshot policy")
	}
	if _, err := r.Reconcile(context.Background(), supervisorTask()); err != nil {
		t.Fatalf("identical reconcile failed: %v", err)
	}
	// A different environment order must not alter the signed template's protobuf digest.
	if !proto.Equal(tmpl, mock.lastTemplate) {
		t.Fatal("reconcile changed immutable template")
	}
	mock.lastTemplate.Containers[0].Command = []string{"sh"}
	before := len(mock.resumedActors)
	if _, err := r.Reconcile(context.Background(), supervisorTask()); err == nil {
		t.Fatal("mutated template accepted")
	}
	if len(mock.resumedActors) != before {
		t.Fatal("resumed changed template")
	}
}

func TestFabriekSupervisorRejectsWorkspaceActionsBeforeProvisioning(t *testing.T) {
	mock := &mockControlServer{}
	r := newTestReconciler(t, mock)
	ws := &v1alpha1.Workspace{Spec: &v1alpha1.WorkspaceSpec{Git: []*v1alpha1.GitRepo{{Repo: "https://example.test/repo"}}}}
	if _, err := r.Reconcile(context.Background(), supervisorTask(), ws); err == nil {
		t.Fatal("workspace actions accepted")
	}
	if len(mock.createdAtespaces) != 0 || len(mock.createdActors) != 0 {
		t.Fatal("provisioning before validation")
	}
}

func TestFabriekSupervisorDoesNotReplaceExistingActor(t *testing.T) {
	mock := &mockControlServer{}
	r := newTestReconciler(t, mock)
	if _, err := r.Reconcile(context.Background(), supervisorTask()); err != nil {
		t.Fatal(err)
	}
	mock.existingActor = &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "fabriek", Name: "another-template"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED}}
	before := len(mock.resumedActors)
	if _, err := r.Reconcile(context.Background(), supervisorTask()); err == nil {
		t.Fatal("existing actor with another template accepted")
	}
	if len(mock.deletedActors) != 0 || len(mock.resumedActors) != before {
		t.Fatal("unexpected existing actor mutation")
	}
}

func TestFabriekSupervisorRejectsTaskCapabilitiesBeforeProvisioning(t *testing.T) {
	for _, capability := range []string{"SETUID", "SYS_ADMIN", "UNKNOWN_CAPABILITY"} {
		t.Run(capability, func(t *testing.T) {
			mock := &mockControlServer{}
			r := newTestReconciler(t, mock)
			task := supervisorTask()
			task.Spec.Capabilities = []string{capability}
			if _, err := r.Reconcile(context.Background(), task); err == nil {
				t.Fatal("task capability accepted")
			}
			if len(mock.createdAtespaces) != 0 || len(mock.createdActors) != 0 || mock.lastTemplate != nil {
				t.Fatal("provisioning before capability validation")
			}
		})
	}
}

func TestFabriekSupervisorRejectsRetainedWorkerSelectorChange(t *testing.T) {
	for name, selector := range map[string]*ateapipb.Selector{
		"missing":         nil,
		"wrong profile":   {MatchLabels: map[string]string{"fabriek.dev/supervisor-profile": "another-profile"}},
		"unrelated label": {MatchLabels: map[string]string{"workload": "ax"}},
	} {
		t.Run(name, func(t *testing.T) {
			mock := &mockControlServer{}
			r := newTestReconciler(t, mock)
			if _, err := r.Reconcile(context.Background(), supervisorTask()); err != nil {
				t.Fatal(err)
			}
			mock.lastTemplate.WorkerSelector = selector
			retained := proto.Clone(mock.lastTemplate).(*ateapipb.ActorTemplate)
			created, resumed := len(mock.createdActors), len(mock.resumedActors)
			if _, err := r.Reconcile(context.Background(), supervisorTask()); err == nil || !strings.Contains(err.Error(), "immutable task intent") {
				t.Fatalf("retained selector change was not rejected: %v", err)
			}
			if len(mock.createdActors) != created || len(mock.resumedActors) != resumed || len(mock.deletedActors) != 0 || len(mock.deletedTemplates) != 0 {
				t.Fatal("changed placement mutated an actor or deleted its template")
			}
			if !proto.Equal(mock.lastTemplate, retained) {
				t.Fatal("changed placement overwrote the retained template")
			}
		})
	}
}
