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

package server_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestProtectedDeleteRequiresRetainedIdentity(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	created := time.Now().UTC()
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private", CreationTimestamp: timestamppb.New(created)}, Spec: &v1alpha1.TaskSpec{SupervisorProfile: "fabriek-native-v1"}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	srv := server.NewServer(s)
	for _, request := range []*v1alpha1.DeleteTaskRequest{
		{Atespace: "private", Name: "job"},
		{Atespace: "private", Name: "job", ActorUid: "observed-actor"},
		{Atespace: "private", Name: "job", ActorUid: "observed-actor", CreationTimestamp: created.Add(time.Second).Format(time.RFC3339Nano)},
	} {
		if _, err := srv.DeleteTask(ctx, request); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("identity not required: %v", err)
		}
	}
	if _, err := srv.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Atespace: "private", Name: "job", ActorUid: "observed-actor", CreationTimestamp: created.Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
}

// ax-bsl.9: SuspendTask and ResumeTask must report NotFound for a missing
// task, exactly as they did when they called GetTask first. UpdateTaskSuspend
// is the only store call they make now, so its own ErrNotFound mapping is
// the one guard left on this path.
func TestSuspendResumeTaskReportsNotFoundForMissingTask(t *testing.T) {
	ctx := context.Background()
	srv := server.NewServer(memory.NewStore())

	if _, err := srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Atespace: "default", Name: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("SuspendTask on a missing task: want NotFound, got %v", err)
	}
	if _, err := srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Atespace: "default", Name: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("ResumeTask on a missing task: want NotFound, got %v", err)
	}
}

// raceDuringSuspendStore wraps a store.Store and pauses inside the one
// store call SuspendTask/ResumeTask makes, so a test can run a concurrent
// MarkTaskDeleting call while that store call is still in flight. The old
// GetTask implementation paused after its real read returned a stale
// snapshot, standing in for a read that ran before the mark landed.
type raceDuringSuspendStore struct {
	store.Store
	reached chan struct{}
	release chan struct{}
}

func (s *raceDuringSuspendStore) GetTask(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	task, err := s.Store.GetTask(ctx, atespace, name)
	s.reached <- struct{}{}
	<-s.release
	return task, err
}

func (s *raceDuringSuspendStore) UpdateTaskSuspend(ctx context.Context, atespace, name string, suspend bool) (*v1alpha1.Task, error) {
	s.reached <- struct{}{}
	<-s.release
	return s.Store.UpdateTaskSuspend(ctx, atespace, name, suspend)
}

// ax-bsl.9: a SuspendTask call must not clear a Terminating phase that a
// MarkTaskDeleting call sets while SuspendTask's own store call is still in
// flight. The old code read the task with GetTask, set Spec.Suspend, and
// wrote the result back with SaveTask. A read that ran before the mark,
// paired with a write that landed after it, replayed the pre-mark status
// and cleared Terminating.
func TestSuspendTaskSurvivesMarkTaskDeletingRace(t *testing.T) {
	ctx := context.Background()
	inner := memory.NewStore()
	if err := inner.SaveTask(ctx, &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "raced", Atespace: "default"}}); err != nil {
		t.Fatal(err)
	}

	raced := &raceDuringSuspendStore{Store: inner, reached: make(chan struct{}), release: make(chan struct{})}
	srv := server.NewServer(raced)

	type suspendResult struct {
		task *v1alpha1.Task
		err  error
	}
	done := make(chan suspendResult, 1)
	go func() {
		task, err := srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Atespace: "default", Name: "raced"})
		done <- suspendResult{task, err}
	}()

	<-raced.reached
	if err := inner.MarkTaskDeleting(ctx, "default", "raced"); err != nil {
		t.Fatal(err)
	}
	close(raced.release)

	res := <-done
	if res.err != nil {
		t.Fatal(res.err)
	}
	if !res.task.GetSpec().GetSuspend() {
		t.Fatal("SuspendTask did not set Spec.Suspend")
	}

	current, err := inner.GetTask(ctx, "default", "raced")
	if err != nil {
		t.Fatal(err)
	}
	if current.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Fatalf("a GetTask read before a concurrent MarkTaskDeleting call, replayed by a later write, cleared Terminating: phase %q", current.GetStatus().GetPhase())
	}
}

func TestServerHealthzHTTP(t *testing.T) {
	memStore := memory.NewStore()
	srv := server.NewServer(memStore)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /healthz, got %d", rec.Code)
	}
	if rec.Body.String() != "ok\n" {
		t.Fatalf("expected 'ok\\n' from /healthz, got %q", rec.Body.String())
	}

	// Verify non-healthz HTTP returns 404
	req404 := httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil)
	rec404 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec404, req404)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for /api/v1/tasks, got %d", rec404.Code)
	}
}

func TestServerGRPC(t *testing.T) {
	memStore := memory.NewStore()
	srv := server.NewServer(memStore)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	httpServer := &http.Server{
		Handler: srv.Handler(),
	}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)

	go func() {
		_ = httpServer.Serve(ln)
	}()
	defer httpServer.Close()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial gRPC: %v", err)
	}
	defer conn.Close()

	client := v1alpha1.NewAXClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Create one resource of each kind through the typed RPCs. Metadata is left
	// partially empty to exercise server-side defaulting.
	if _, err := client.UpdateWorkspace(ctx, &v1alpha1.UpdateWorkspaceRequest{Workspace: &v1alpha1.Workspace{
		Metadata: &v1alpha1.ObjectMeta{Name: "grpc-ws"},
		Spec:     &v1alpha1.WorkspaceSpec{},
	}}); err != nil {
		t.Fatalf("UpdateWorkspace failed: %v", err)
	}
	if _, err := client.UpdateModel(ctx, &v1alpha1.UpdateModelRequest{Model: &v1alpha1.Model{
		Metadata: &v1alpha1.ObjectMeta{Name: "grpc-model"},
		Spec:     &v1alpha1.ModelSpec{Provider: "google", Model: "gemini-3.8-flash"},
	}}); err != nil {
		t.Fatalf("UpdateModel failed: %v", err)
	}
	if _, err := client.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "grpc-task"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
	}}); err != nil {
		t.Fatalf("UpdateTask failed: %v", err)
	}

	// 2. Defaulting applies to every kind: atespace and creation timestamp are filled in.
	ws, err := client.GetWorkspace(ctx, &v1alpha1.GetWorkspaceRequest{Atespace: "default", Name: "grpc-ws"})
	if err != nil {
		t.Fatalf("GetWorkspace failed: %v", err)
	}
	if ws.GetMetadata().GetAtespace() != "default" || ws.GetMetadata().GetCreationTimestamp() == nil {
		t.Errorf("expected workspace metadata to be defaulted, got %v", ws.GetMetadata())
	}

	// 3. GetTask & ListTasks
	task, err := client.GetTask(ctx, &v1alpha1.GetTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}
	if task.Metadata.Name != "grpc-task" {
		t.Errorf("expected name 'grpc-task', got %s", task.Metadata.Name)
	}
	if task.Metadata.CreationTimestamp == nil {
		t.Errorf("expected creation timestamp to be populated on applied task")
	}

	listTasksResp, err := client.ListTasks(ctx, &v1alpha1.ListTasksRequest{Atespace: "default"})
	if err != nil {
		t.Fatalf("ListTasks failed: %v", err)
	}
	if len(listTasksResp.Tasks) != 1 {
		t.Fatalf("expected 1 task in list, got %d", len(listTasksResp.Tasks))
	}
	if listTasksResp.Tasks[0].Metadata.CreationTimestamp == nil {
		t.Errorf("expected creation timestamp on listed task")
	}

	// Test UpdateTask
	task.Spec.Image = "ghcr.io/test/updated-image"
	updatedTask, err := client.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: task})
	if err != nil {
		t.Fatalf("UpdateTask failed: %v", err)
	}
	if updatedTask.Spec.Image != "ghcr.io/test/updated-image" {
		t.Errorf("expected image 'ghcr.io/test/updated-image', got %s", updatedTask.Spec.Image)
	}

	// 4. Suspend & Resume Task
	suspTask, err := client.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("SuspendTask failed: %v", err)
	}
	if !suspTask.Spec.Suspend {
		t.Errorf("expected task to be suspended")
	}

	resTask, err := client.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("ResumeTask failed: %v", err)
	}
	if resTask.Spec.Suspend {
		t.Errorf("expected task to be resumed")
	}

	// 5. Workspaces
	ws, err = client.GetWorkspace(ctx, &v1alpha1.GetWorkspaceRequest{Atespace: "default", Name: "grpc-ws"})
	if err != nil {
		t.Fatalf("GetWorkspace failed: %v", err)
	}
	if ws.Metadata.Name != "grpc-ws" {
		t.Errorf("expected workspace 'grpc-ws', got %s", ws.Metadata.Name)
	}

	listWorkspacesResp, err := client.ListWorkspaces(ctx, &v1alpha1.ListWorkspacesRequest{Atespace: "default"})
	if err != nil {
		t.Fatalf("ListWorkspaces failed: %v", err)
	}
	if len(listWorkspacesResp.Workspaces) != 1 {
		t.Fatalf("expected 1 workspace in list, got %d", len(listWorkspacesResp.Workspaces))
	}

	// 7. Models
	model, err := client.GetModel(ctx, &v1alpha1.GetModelRequest{Atespace: "default", Name: "grpc-model"})
	if err != nil {
		t.Fatalf("GetModel failed: %v", err)
	}
	if model.Metadata.Name != "grpc-model" {
		t.Errorf("expected model 'grpc-model', got %s", model.Metadata.Name)
	}

	listModelsResp, err := client.ListModels(ctx, &v1alpha1.ListModelsRequest{Atespace: "default"})
	if err != nil {
		t.Fatalf("ListModels failed: %v", err)
	}
	if len(listModelsResp.Models) != 1 {
		t.Fatalf("expected 1 model in list, got %d", len(listModelsResp.Models))
	}

	// 7b. WatchTask (should receive INITIAL state)
	stream, err := client.WatchTask(ctx, &v1alpha1.WatchTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("WatchTask failed: %v", err)
	}
	watchMsg, err := stream.Recv()
	if err != nil {
		t.Fatalf("WatchTask Recv failed: %v", err)
	}
	if watchMsg.Action != "INITIAL" {
		t.Errorf("expected INITIAL action, got %s", watchMsg.Action)
	}
	if watchMsg.Task == nil || watchMsg.Task.Metadata == nil || watchMsg.Task.Metadata.Name != "grpc-task" {
		t.Errorf("expected task 'grpc-task' in watch, got %+v", watchMsg.Task)
	}

	// 8. Delete operations
	// Task deletion is two-phase: the RPC marks the task Terminating and the
	// controller removes the record after tearing down the actor.
	if _, err := client.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Atespace: "default", Name: "grpc-task"}); err != nil {
		t.Fatalf("DeleteTask failed: %v", err)
	}
	terminating, err := client.GetTask(ctx, &v1alpha1.GetTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("GetTask after DeleteTask failed: %v", err)
	}
	if terminating.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Errorf("expected phase %q after DeleteTask, got %q", v1alpha1.PhaseTerminating, terminating.GetStatus().GetPhase())
	}
	if _, err := client.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Atespace: "default", Name: "no-such-task"}); status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound deleting a missing task, got %v", err)
	}
	// Stand in for the controller finishing cleanup.
	if err := memStore.DeleteTask(ctx, "default", "grpc-task"); err != nil {
		t.Fatalf("removing task record failed: %v", err)
	}
	if _, err := client.DeleteWorkspace(ctx, &v1alpha1.DeleteWorkspaceRequest{Atespace: "default", Name: "grpc-ws"}); err != nil {
		t.Fatalf("DeleteWorkspace failed: %v", err)
	}
	if _, err := client.DeleteModel(ctx, &v1alpha1.DeleteModelRequest{Atespace: "default", Name: "grpc-model"}); err != nil {
		t.Fatalf("DeleteModel failed: %v", err)
	}

	// Check 404 after delete
	_, err = client.GetTask(ctx, &v1alpha1.GetTaskRequest{Atespace: "default", Name: "grpc-task"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound code, got %v", err)
	}
}

func TestUpdateTask_ValidatesWorkspaceBindings(t *testing.T) {
	srv := server.NewServer(memory.NewStore())
	ctx := context.Background()

	_, err := srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "bad"},
		Spec: &v1alpha1.TaskSpec{
			Workspaces: []*v1alpha1.WorkspaceRef{{Name: "a", Path: "/same"}, {Name: "b", Path: "/same"}},
		},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for colliding workspace paths, got %v", err)
	}

	_, err = srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "good"},
		Spec: &v1alpha1.TaskSpec{
			Workspaces: []*v1alpha1.WorkspaceRef{{Name: "a"}, {Name: "b"}},
		},
	}})
	if err != nil {
		t.Fatalf("expected a valid multi-workspace task to be accepted, got %v", err)
	}
}

func TestUpdateTask_RejectsMemoryAboveMax(t *testing.T) {
	ctx := context.Background()
	memStore := memory.NewStore()
	srv := server.NewServer(memStore)
	srv.MaxTaskMemory = resource.MustParse("16Gi")

	// UpdateTask rejects a task that asks for more memory than any worker can
	// hold, and the store never gets a record of it.
	_, err := srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "too-big"},
		Spec:     &v1alpha1.TaskSpec{Resources: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Memory: "32Gi"}}},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for a task over the memory cap, got %v", err)
	}
	if !strings.Contains(err.Error(), `spec.resources.limits.memory: "32Gi"`) || !strings.Contains(err.Error(), "16Gi") {
		t.Fatalf("error does not name the limit and the cap: %v", err)
	}
	if _, serr := memStore.GetTask(ctx, "default", "too-big"); serr == nil {
		t.Fatalf("rejected task was saved anyway")
	}

	// A task at or under the cap still saves.
	if _, err := srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "at-cap"},
		Spec:     &v1alpha1.TaskSpec{Resources: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Memory: "16Gi"}}},
	}}); err != nil {
		t.Fatalf("expected a task at the cap to be accepted, got %v", err)
	}
	if _, err := srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "under-cap"},
		Spec:     &v1alpha1.TaskSpec{Resources: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Memory: "4Gi"}}},
	}}); err != nil {
		t.Fatalf("expected a task under the cap to be accepted, got %v", err)
	}

	// With no cap configured, UpdateTask keeps the old behavior: it rejects nothing.
	unbounded := server.NewServer(memory.NewStore())
	if _, err := unbounded.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "no-cap"},
		Spec:     &v1alpha1.TaskSpec{Resources: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Memory: "32Gi"}}},
	}}); err != nil {
		t.Fatalf("expected an unbounded server to accept any memory limit, got %v", err)
	}
}

func TestUpdateTask_RejectsCPUAboveMax(t *testing.T) {
	ctx := context.Background()
	memStore := memory.NewStore()
	srv := server.NewServer(memStore)
	srv.MaxTaskCPU = resource.MustParse("6")

	// UpdateTask rejects a task that asks for more cpu than any worker can
	// hold, and the store never gets a record of it.
	_, err := srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "too-big"},
		Spec:     &v1alpha1.TaskSpec{Resources: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Cpu: "32"}}},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for a task over the cpu cap, got %v", err)
	}
	if !strings.Contains(err.Error(), `spec.resources.limits.cpu: "32"`) || !strings.Contains(err.Error(), "6") {
		t.Fatalf("error does not name the limit and the cap: %v", err)
	}
	if _, serr := memStore.GetTask(ctx, "default", "too-big"); serr == nil {
		t.Fatalf("rejected task was saved anyway")
	}

	// A task at or under the cap still saves.
	if _, err := srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "at-cap"},
		Spec:     &v1alpha1.TaskSpec{Resources: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Cpu: "6"}}},
	}}); err != nil {
		t.Fatalf("expected a task at the cap to be accepted, got %v", err)
	}
	if _, err := srv.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "under-cap"},
		Spec:     &v1alpha1.TaskSpec{Resources: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Cpu: "500m"}}},
	}}); err != nil {
		t.Fatalf("expected a task under the cap to be accepted, got %v", err)
	}

	// With no cap configured, UpdateTask keeps the old behavior: it rejects nothing.
	unbounded := server.NewServer(memory.NewStore())
	if _, err := unbounded.UpdateTask(ctx, &v1alpha1.UpdateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "no-cap"},
		Spec:     &v1alpha1.TaskSpec{Resources: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Cpu: "32"}}},
	}}); err != nil {
		t.Fatalf("expected an unbounded server to accept any cpu limit, got %v", err)
	}
}
