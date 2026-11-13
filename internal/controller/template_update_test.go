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
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	repointUID     = "11111111-1111-4111-8111-111111111111"
	repointVersion = int64(4)
)

func ordinaryImageTask(image string, suspend bool) *v1alpha1.Task {
	return &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata:   &v1alpha1.ObjectMeta{Name: "job", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image:   image,
			Command: []string{"/bin/task-runner"},
			Suspend: suspend,
		},
	}
}

func supervisorImage(digit string) string {
	return "ghcr.io/stigenai/native@sha256:" + strings.Repeat(digit, 64)
}

func fullSuspendedActor(atespace, name, template string) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: atespace,
			Name:     name,
			Uid:      repointUID,
			Version:  repointVersion,
		},
		ActorTemplate:  &ateapipb.ObjectRef{Atespace: atespace, Name: template},
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"pool": "keep-me"}},
		SourceTag:      &ateapipb.ObjectRef{Atespace: atespace, Name: "seed-tag"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			ExternalSnapshot: &ateapipb.ExternalSnapshot{
				SnapshotUri:      "gs://bucket/atespaces/" + atespace + "/actors/" + repointUID + "/snapshots/snap",
				ContentScope:     ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				ActorTemplateUid: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
			},
		},
	}
}

func resetMockCounts(m *mockControlServer) {
	m.calls = nil
	m.updateRequests = nil
	m.createdActors = nil
	m.createdAtespaces = nil
	m.deletedActors = nil
	m.resumedActors = nil
	m.suspendedActors = nil
}

func assertCallBefore(t *testing.T, calls []string, earlier, later string) {
	t.Helper()
	ie, il := -1, -1
	for i, call := range calls {
		if call == earlier && ie < 0 {
			ie = i
		}
		if call == later && il < 0 {
			il = i
		}
	}
	if ie < 0 || il < 0 || ie > il {
		t.Fatalf("%s must precede %s, calls=%v", earlier, later, calls)
	}
}

func assertSuspendedTask(t *testing.T, task *v1alpha1.Task) {
	t.Helper()
	if task.GetStatus().GetPhase() != "Suspended" {
		t.Fatalf("phase %q, want Suspended", task.GetStatus().GetPhase())
	}
	if task.GetStatus().GetWorkerIp() != "" {
		t.Fatalf("worker ip %q, want empty", task.GetStatus().GetWorkerIp())
	}
	assertCondition(t, task, "Ready", "False", "TaskSuspended")
}

func assertIdle(t *testing.T, m *mockControlServer, updates int) {
	t.Helper()
	if len(m.updateRequests) != updates || len(m.deletedActors) != 0 || len(m.resumedActors) != 0 || len(m.createdActors) != 0 || len(m.suspendedActors) != 0 {
		t.Fatalf("updates=%d deletes=%v resumes=%v creates=%v suspends=%v calls=%v", len(m.updateRequests), m.deletedActors, m.resumedActors, m.createdActors, m.suspendedActors, m.calls)
	}
}

func assertOnlyTemplateChanged(t *testing.T, before, req *ateapipb.Actor, templateAtespace, templateName string) {
	t.Helper()
	if req.GetActorTemplate().GetAtespace() != templateAtespace || req.GetActorTemplate().GetName() != templateName {
		t.Fatalf("template %s/%s, want %s/%s", req.GetActorTemplate().GetAtespace(), req.GetActorTemplate().GetName(), templateAtespace, templateName)
	}
	want := cloneActor(before)
	got := cloneActor(req)
	want.ActorTemplate = nil
	got.ActorTemplate = nil
	if !proto.Equal(want, got) {
		t.Fatalf("update changed a field other than ActorTemplate\nwant %v\ngot  %v", want, got)
	}
}

func assertStoredIdentity(t *testing.T, before, stored *ateapipb.Actor, templateAtespace, templateName string) {
	t.Helper()
	if stored.GetMetadata().GetUid() != before.GetMetadata().GetUid() || stored.GetMetadata().GetName() != before.GetMetadata().GetName() || stored.GetMetadata().GetAtespace() != before.GetMetadata().GetAtespace() {
		t.Fatalf("metadata changed: %v", stored.GetMetadata())
	}
	if stored.GetMetadata().GetVersion() != before.GetMetadata().GetVersion()+1 {
		t.Fatalf("version %d, want %d", stored.GetMetadata().GetVersion(), before.GetMetadata().GetVersion()+1)
	}
	if !proto.Equal(stored.GetSourceTag(), before.GetSourceTag()) {
		t.Fatalf("source_tag %v", stored.GetSourceTag())
	}
	if !proto.Equal(stored.GetWorkerSelector(), before.GetWorkerSelector()) {
		t.Fatalf("worker_selector %v", stored.GetWorkerSelector())
	}
	if !proto.Equal(stored.GetStatus().GetExternalSnapshot(), before.GetStatus().GetExternalSnapshot()) {
		t.Fatalf("snapshot %v", stored.GetStatus().GetExternalSnapshot())
	}
	if stored.GetActorTemplate().GetAtespace() != templateAtespace || stored.GetActorTemplate().GetName() != templateName {
		t.Fatalf("stored template %s/%s", stored.GetActorTemplate().GetAtespace(), stored.GetActorTemplate().GetName())
	}
}

// assertRepoint reconciles a suspended actor onto a new template, then resumes it.
func assertRepoint(t *testing.T, task *v1alpha1.Task) {
	t.Helper()
	mock := &mockControlServer{}
	reconciler := newTestReconciler(t, mock)
	before := fullSuspendedActor(task.GetMetadata().GetAtespace(), task.GetMetadata().GetName(), "old-template")
	mock.existingActor = before

	reconciled, err := reconciler.Reconcile(context.Background(), task)
	if err != nil {
		t.Fatalf("repoint: %v", err)
	}
	assertIdle(t, mock, 1)
	assertCallBefore(t, mock.calls, "CreateActorTemplate", "UpdateActor")
	assertCallBefore(t, mock.calls, "CreateActor", "UpdateActor")
	templateName := mock.lastTemplate.GetMetadata().GetName()
	templateAtespace := mock.lastTemplate.GetMetadata().GetAtespace()
	if templateName == "" || templateName == "old-template" {
		t.Fatalf("template %q", templateName)
	}
	assertOnlyTemplateChanged(t, before, mock.updateRequests[0], templateAtespace, templateName)
	assertStoredIdentity(t, before, mock.liveActor, templateAtespace, templateName)
	assertSuspendedTask(t, reconciled)
	if mock.liveActor.GetMetadata().GetUid() != repointUID {
		t.Fatalf("uid %s", mock.liveActor.GetMetadata().GetUid())
	}

	task.Spec.Suspend = false
	resumed, err := reconciler.Reconcile(context.Background(), task)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(mock.updateRequests) != 1 || len(mock.deletedActors) != 0 || len(mock.createdActors) != 0 {
		t.Fatalf("resume mutated the actor updates=%d deletes=%v creates=%v", len(mock.updateRequests), mock.deletedActors, mock.createdActors)
	}
	if len(mock.resumedActors) != 1 || mock.resumedActors[0] != task.GetMetadata().GetName() {
		t.Fatalf("resume target %v", mock.resumedActors)
	}
	if mock.liveActor.GetMetadata().GetUid() != repointUID {
		t.Fatalf("resume uid %s", mock.liveActor.GetMetadata().GetUid())
	}
	if mock.liveActor.GetActorTemplate().GetName() != templateName || mock.liveActor.GetActorTemplate().GetAtespace() != templateAtespace {
		t.Fatalf("resume template %s/%s", mock.liveActor.GetActorTemplate().GetAtespace(), mock.liveActor.GetActorTemplate().GetName())
	}
	if resumed.GetStatus().GetPhase() != "Running" {
		t.Fatalf("resume phase %q", resumed.GetStatus().GetPhase())
	}
}

func TestTaskReconciler_SuspendedTemplateUpdate(t *testing.T) {
	t.Run("ordinary image", func(t *testing.T) {
		assertRepoint(t, ordinaryImageTask("ghcr.io/example/app:b", true))
	})
	t.Run("supervisor image", func(t *testing.T) {
		task := supervisorTask()
		task.Spec.Suspend = true
		task.Spec.Image = supervisorImage("c")
		assertRepoint(t, task)
	})
	t.Run("env", func(t *testing.T) {
		task := envTask("changed")
		task.Spec.Suspend = true
		assertRepoint(t, task)
	})
	t.Run("unchanged template", func(t *testing.T) {
		task := ordinaryImageTask("ghcr.io/example/app:a", true)
		mock := &mockControlServer{}
		reconciler := newTestReconciler(t, mock)
		if _, err := reconciler.Reconcile(context.Background(), task); err != nil {
			t.Fatal(err)
		}
		templateName := mock.lastTemplate.GetMetadata().GetName()
		resetMockCounts(mock)
		mock.existingActor = fullSuspendedActor("default", "job", templateName)
		reconciled, err := reconciler.Reconcile(context.Background(), task)
		if err != nil {
			t.Fatal(err)
		}
		if len(mock.updateRequests) != 0 || len(mock.deletedActors) != 0 || len(mock.createdActors) != 0 || len(mock.resumedActors) != 0 {
			t.Fatalf("unchanged template mutated updates=%d deletes=%v creates=%v resumes=%v", len(mock.updateRequests), mock.deletedActors, mock.createdActors, mock.resumedActors)
		}
		assertSuspendedTask(t, reconciled)
	})
}

func TestTaskReconciler_TemplateUpdateGuard(t *testing.T) {
	rows := []struct {
		name   string
		state  ateapipb.ActorState
		status bool
		uid    string
		ver    int64
	}{
		{"running", ateapipb.ActorState_ACTOR_STATE_RUNNING, true, repointUID, repointVersion},
		{"crashed", ateapipb.ActorState_ACTOR_STATE_CRASHED, true, repointUID, repointVersion},
		{"deleting", ateapipb.ActorState_ACTOR_STATE_DELETING, true, repointUID, repointVersion},
		{"paused", ateapipb.ActorState_ACTOR_STATE_PAUSED, true, repointUID, repointVersion},
		{"resuming", ateapipb.ActorState_ACTOR_STATE_RESUMING, true, repointUID, repointVersion},
		{"suspending", ateapipb.ActorState_ACTOR_STATE_SUSPENDING, true, repointUID, repointVersion},
		{"pausing", ateapipb.ActorState_ACTOR_STATE_PAUSING, true, repointUID, repointVersion},
		{"reverting", ateapipb.ActorState_ACTOR_STATE_REVERTING, true, repointUID, repointVersion},
		{"unspecified", ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED, true, repointUID, repointVersion},
		{"unknown", ateapipb.ActorState(99), true, repointUID, repointVersion},
		{"absent status", 0, false, repointUID, repointVersion},
		{"absent uid", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, true, "", repointVersion},
		{"zero version", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, true, repointUID, 0},
	}
	for _, profile := range []string{"ordinary", "supervisor"} {
		for _, row := range rows {
			t.Run(profile+"/"+row.name, func(t *testing.T) {
				var task *v1alpha1.Task
				if profile == "supervisor" {
					task = supervisorTask()
					task.Spec.Suspend = true
				} else {
					task = ordinaryImageTask("ghcr.io/example/app:b", true)
				}
				actor := fullSuspendedActor(task.GetMetadata().GetAtespace(), task.GetMetadata().GetName(), "old-template")
				if row.status {
					actor.Status.State = row.state
				} else {
					actor.Status = nil
				}
				actor.Metadata.Uid = row.uid
				actor.Metadata.Version = row.ver
				mock := &mockControlServer{existingActor: actor}
				reconciler := newTestReconciler(t, mock)
				ctx := context.Background()
				cancel := func() {}
				if row.state == ateapipb.ActorState_ACTOR_STATE_DELETING {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, 3*time.Second)
					cancel = stop
				}
				reconciled, err := reconciler.Reconcile(ctx, task)
				cancel()
				if err == nil || !strings.Contains(err.Error(), "template update") {
					t.Fatalf("err %v", err)
				}
				assertIdle(t, mock, 0)
				if readyReason(reconciled) == "TaskSuspended" {
					t.Fatal("claimed TaskSuspended")
				}
			})
		}
	}

	t.Run("get actor failure", func(t *testing.T) {
		task := ordinaryImageTask("ghcr.io/example/app:b", true)
		mock := &mockControlServer{
			existingActor: fullSuspendedActor("default", "job", "old-template"),
			getActorErr:   status.Error(codes.Unavailable, "boom"),
		}
		reconciled, err := newTestReconciler(t, mock).Reconcile(context.Background(), task)
		if err == nil || !strings.Contains(err.Error(), "boom") || !strings.Contains(err.Error(), "template update") {
			t.Fatalf("err %v", err)
		}
		assertIdle(t, mock, 0)
		if readyReason(reconciled) == "TaskSuspended" {
			t.Fatal("claimed TaskSuspended")
		}
	})

	t.Run("nil actor", func(t *testing.T) {
		task := ordinaryImageTask("ghcr.io/example/app:b", true)
		mock := &mockControlServer{
			existingActor: fullSuspendedActor("default", "job", "old-template"),
			getActorNil:   true,
		}
		reconciled, err := newTestReconciler(t, mock).Reconcile(context.Background(), task)
		if err == nil || !strings.Contains(err.Error(), "no record") {
			t.Fatalf("err %v", err)
		}
		assertIdle(t, mock, 0)
		if readyReason(reconciled) == "TaskSuspended" {
			t.Fatal("claimed TaskSuspended")
		}
	})

	t.Run("empty template", func(t *testing.T) {
		task := ordinaryImageTask("ghcr.io/example/app:b", true)
		mock := &mockControlServer{
			existingActor:     fullSuspendedActor("default", "job", "old-template"),
			blankTemplateName: true,
		}
		reconciled, err := newTestReconciler(t, mock).Reconcile(context.Background(), task)
		if err == nil || !strings.Contains(err.Error(), "template update") {
			t.Fatalf("err %v", err)
		}
		assertIdle(t, mock, 0)
		if readyReason(reconciled) == "TaskSuspended" {
			t.Fatal("claimed TaskSuspended")
		}
	})

	t.Run("suspend false running ordinary", func(t *testing.T) {
		task := ordinaryImageTask("ghcr.io/example/app:b", false)
		actor := fullSuspendedActor("default", "job", "old-template")
		actor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		mock := &mockControlServer{existingActor: actor}
		reconciled, err := newTestReconciler(t, mock).Reconcile(context.Background(), task)
		if err != nil {
			t.Fatal(err)
		}
		if len(mock.updateRequests) != 0 || len(mock.deletedActors) != 0 || len(mock.createdActors) != 0 || len(mock.resumedActors) != 1 {
			t.Fatalf("updates=%d deletes=%v creates=%v resumes=%v", len(mock.updateRequests), mock.deletedActors, mock.createdActors, mock.resumedActors)
		}
		if reconciled.GetStatus().GetPhase() != "Running" {
			t.Fatalf("phase %q", reconciled.GetStatus().GetPhase())
		}
	})

	t.Run("suspend false crashed ordinary", func(t *testing.T) {
		task := ordinaryImageTask("ghcr.io/example/app:b", false)
		actor := fullSuspendedActor("default", "job", "old-template")
		actor.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		mock := &mockControlServer{existingActor: actor}
		if _, err := newTestReconciler(t, mock).Reconcile(context.Background(), task); err != nil {
			t.Fatal(err)
		}
		if len(mock.updateRequests) != 0 || len(mock.deletedActors) != 1 || len(mock.createdActors) != 1 {
			t.Fatalf("updates=%d deletes=%v creates=%v", len(mock.updateRequests), mock.deletedActors, mock.createdActors)
		}
	})

	t.Run("running supervisor", func(t *testing.T) {
		task := supervisorTask()
		actor := fullSuspendedActor(task.GetMetadata().GetAtespace(), task.GetMetadata().GetName(), "another-template")
		actor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		mock := &mockControlServer{existingActor: actor}
		_, err := newTestReconciler(t, mock).Reconcile(context.Background(), task)
		if err == nil || !strings.Contains(err.Error(), "another template") {
			t.Fatalf("err %v", err)
		}
		assertIdle(t, mock, 0)
	})

	t.Run("same template resumes", func(t *testing.T) {
		task := ordinaryImageTask("ghcr.io/example/app:a", false)
		mock := &mockControlServer{}
		reconciler := newTestReconciler(t, mock)
		if _, err := reconciler.Reconcile(context.Background(), task); err != nil {
			t.Fatal(err)
		}
		templateName := mock.lastTemplate.GetMetadata().GetName()
		resetMockCounts(mock)
		actor := fullSuspendedActor("default", "job", templateName)
		actor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		mock.existingActor = actor
		if _, err := reconciler.Reconcile(context.Background(), task); err != nil {
			t.Fatal(err)
		}
		if len(mock.updateRequests) != 0 || len(mock.resumedActors) != 1 || len(mock.deletedActors) != 0 {
			t.Fatalf("updates=%d resumes=%v deletes=%v", len(mock.updateRequests), mock.resumedActors, mock.deletedActors)
		}
	})

	t.Run("new task", func(t *testing.T) {
		mock := &mockControlServer{}
		task := ordinaryImageTask("ghcr.io/example/app:a", false)
		if _, err := newTestReconciler(t, mock).Reconcile(context.Background(), task); err != nil {
			t.Fatal(err)
		}
		if len(mock.updateRequests) != 0 || len(mock.createdActors) != 1 || len(mock.resumedActors) != 1 {
			t.Fatalf("updates=%d creates=%v resumes=%v", len(mock.updateRequests), mock.createdActors, mock.resumedActors)
		}
	})
}

func TestTaskReconciler_TemplateUpdateFailure(t *testing.T) {
	newMock := func() (*mockControlServer, *v1alpha1.Task) {
		task := ordinaryImageTask("ghcr.io/example/app:b", true)
		mock := &mockControlServer{existingActor: fullSuspendedActor("default", "job", "old-template")}
		return mock, task
	}

	t.Run("template create", func(t *testing.T) {
		mock, task := newMock()
		mock.rejectTemplates = status.Error(codes.InvalidArgument, "bad image")
		reconciled, err := newTestReconciler(t, mock).Reconcile(context.Background(), task)
		if err == nil || !strings.Contains(err.Error(), "bad image") {
			t.Fatalf("err %v", err)
		}
		assertIdle(t, mock, 0)
		if reconciled.GetStatus().GetPhase() != "Failed" || readyReason(reconciled) == "TaskSuspended" {
			t.Fatalf("phase %q reason %q", reconciled.GetStatus().GetPhase(), readyReason(reconciled))
		}
	})

	t.Run("aborted", func(t *testing.T) {
		mock, task := newMock()
		stale := cloneActor(mock.existingActor)
		stale.Metadata.Version = 1
		mock.existingActor.Metadata.Version = 9
		mock.getActorActor = stale
		reconciled, err := newTestReconciler(t, mock).Reconcile(context.Background(), task)
		if status.Code(err) != codes.Aborted && (err == nil || !strings.Contains(err.Error(), "conflict")) {
			t.Fatalf("err %v", err)
		}
		if len(mock.deletedActors) != 0 || len(mock.resumedActors) != 0 || len(mock.createdActors) != 0 || len(mock.suspendedActors) != 0 {
			t.Fatalf("deletes=%v resumes=%v creates=%v suspends=%v", mock.deletedActors, mock.resumedActors, mock.createdActors, mock.suspendedActors)
		}
		if mock.liveActor != nil || mock.existingActor.GetMetadata().GetVersion() != 9 || mock.existingActor.GetActorTemplate().GetName() != "old-template" {
			t.Fatal("conflict stored a new actor")
		}
		if readyReason(reconciled) == "TaskSuspended" {
			t.Fatal("claimed TaskSuspended")
		}
	})

	t.Run("transport", func(t *testing.T) {
		mock, task := newMock()
		mock.updateErr = status.Error(codes.Unavailable, "transport down")
		reconciled, err := newTestReconciler(t, mock).Reconcile(context.Background(), task)
		if err == nil || !strings.Contains(err.Error(), "transport down") {
			t.Fatalf("err %v", err)
		}
		if len(mock.updateRequests) != 1 || len(mock.deletedActors) != 0 || len(mock.resumedActors) != 0 || len(mock.createdActors) != 0 || len(mock.suspendedActors) != 0 {
			t.Fatalf("updates=%d deletes=%v resumes=%v creates=%v suspends=%v", len(mock.updateRequests), mock.deletedActors, mock.resumedActors, mock.createdActors, mock.suspendedActors)
		}
		if mock.liveActor != nil {
			t.Fatal("transport failure stored the actor")
		}
		if reconciled.GetStatus().GetPhase() != "Failed" || readyReason(reconciled) == "TaskSuspended" {
			t.Fatalf("phase %q reason %q", reconciled.GetStatus().GetPhase(), readyReason(reconciled))
		}
	})

	t.Run("empty update response", func(t *testing.T) {
		mock, task := newMock()
		mock.updateNil = true
		reconciled, err := newTestReconciler(t, mock).Reconcile(context.Background(), task)
		if err == nil || !strings.Contains(err.Error(), "empty response") {
			t.Fatalf("err %v", err)
		}
		if mock.liveActor != nil || len(mock.deletedActors) != 0 || len(mock.suspendedActors) != 0 || readyReason(reconciled) == "TaskSuspended" {
			t.Fatal("empty response was treated as success")
		}
	})

	t.Run("state changed on fresh read", func(t *testing.T) {
		mock, task := newMock()
		mock.updateErr = status.Error(codes.Unavailable, "transport down")
		reconciler := newTestReconciler(t, mock)
		if _, err := reconciler.Reconcile(context.Background(), task); err == nil {
			t.Fatal("first reconcile succeeded")
		}
		mock.updateErr = nil
		mock.updateRequests = nil
		mock.existingActor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		mock.existingActor.Metadata.Version = 8
		reconciled, err := reconciler.Reconcile(context.Background(), task)
		if err == nil || !strings.Contains(err.Error(), "template update") {
			t.Fatalf("err %v", err)
		}
		assertIdle(t, mock, 0)
		if mock.existingActor.GetMetadata().GetVersion() != 8 || readyReason(reconciled) == "TaskSuspended" {
			t.Fatal("fresh read applied a stale update")
		}
	})

	t.Run("lost response then matching template", func(t *testing.T) {
		mock, task := newMock()
		mock.loseUpdateResponse = true
		reconciler := newTestReconciler(t, mock)
		reconciled, err := reconciler.Reconcile(context.Background(), task)
		if err == nil || !strings.Contains(err.Error(), "lost update response") {
			t.Fatalf("err %v", err)
		}
		if readyReason(reconciled) == "TaskSuspended" || mock.liveActor.GetActorTemplate().GetName() == "old-template" {
			t.Fatal("lost response did not keep the server update")
		}
		if len(mock.updateRequests) != 1 || len(mock.deletedActors) != 0 || len(mock.suspendedActors) != 0 {
			t.Fatalf("updates=%d deletes=%v suspends=%v", len(mock.updateRequests), mock.deletedActors, mock.suspendedActors)
		}
		snapshot := cloneActor(mock.liveActor).GetStatus().GetExternalSnapshot()
		version := mock.liveActor.GetMetadata().GetVersion()
		reconciled, err = reconciler.Reconcile(context.Background(), task)
		if err != nil {
			t.Fatalf("repeat: %v", err)
		}
		if len(mock.updateRequests) != 1 || len(mock.suspendedActors) != 0 || len(mock.deletedActors) != 0 {
			t.Fatalf("repeat updates=%d suspends=%v deletes=%v", len(mock.updateRequests), mock.suspendedActors, mock.deletedActors)
		}
		if mock.liveActor.GetMetadata().GetVersion() != version || !proto.Equal(mock.liveActor.GetStatus().GetExternalSnapshot(), snapshot) {
			t.Fatal("repeat reconcile wrote the actor again")
		}
		assertSuspendedTask(t, reconciled)
	})
}

func TestTaskReconciler_TemplateUpdateIdempotent(t *testing.T) {
	for _, profile := range []string{"ordinary", "supervisor"} {
		t.Run(profile, func(t *testing.T) {
			var task *v1alpha1.Task
			if profile == "supervisor" {
				task = supervisorTask()
				task.Spec.Image = supervisorImage("c")
			} else {
				task = ordinaryImageTask("ghcr.io/example/app:b", true)
			}
			task.Spec.Suspend = true
			mock := &mockControlServer{}
			reconciler := newTestReconciler(t, mock)
			before := fullSuspendedActor(task.GetMetadata().GetAtespace(), task.GetMetadata().GetName(), "old-template")
			snapshot := cloneActor(before).GetStatus().GetExternalSnapshot()
			mock.existingActor = before
			reconciled, err := reconciler.Reconcile(context.Background(), task)
			if err != nil {
				t.Fatal(err)
			}
			assertSuspendedTask(t, reconciled)
			reconciled, err = reconciler.Reconcile(context.Background(), task)
			if err != nil {
				t.Fatal(err)
			}
			if len(mock.updateRequests) != 1 || len(mock.suspendedActors) != 0 || len(mock.deletedActors) != 0 {
				t.Fatalf("updates=%d suspends=%v deletes=%v", len(mock.updateRequests), mock.suspendedActors, mock.deletedActors)
			}
			if !proto.Equal(mock.liveActor.GetStatus().GetExternalSnapshot(), snapshot) {
				t.Fatal("snapshot replaced")
			}
			assertSuspendedTask(t, reconciled)
		})
	}

	t.Run("running same template still suspends", func(t *testing.T) {
		task := ordinaryImageTask("ghcr.io/example/app:a", false)
		mock := &mockControlServer{}
		reconciler := newTestReconciler(t, mock)
		if _, err := reconciler.Reconcile(context.Background(), task); err != nil {
			t.Fatal(err)
		}
		templateName := mock.lastTemplate.GetMetadata().GetName()
		resetMockCounts(mock)
		actor := fullSuspendedActor("default", "job", templateName)
		actor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		mock.existingActor = actor
		task.Spec.Suspend = true
		reconciled, err := reconciler.Reconcile(context.Background(), task)
		if err != nil {
			t.Fatal(err)
		}
		if len(mock.suspendedActors) != 1 || len(mock.updateRequests) != 0 {
			t.Fatalf("suspends=%v updates=%d", mock.suspendedActors, len(mock.updateRequests))
		}
		assertSuspendedTask(t, reconciled)
	})

	t.Run("suspend error propagates", func(t *testing.T) {
		task := ordinaryImageTask("ghcr.io/example/app:a", false)
		mock := &mockControlServer{}
		reconciler := newTestReconciler(t, mock)
		if _, err := reconciler.Reconcile(context.Background(), task); err != nil {
			t.Fatal(err)
		}
		templateName := mock.lastTemplate.GetMetadata().GetName()
		resetMockCounts(mock)
		actor := fullSuspendedActor("default", "job", templateName)
		actor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		mock.existingActor = actor
		mock.suspendErr = status.Error(codes.Internal, "disk full")
		task.Spec.Suspend = true
		reconciled, err := reconciler.Reconcile(context.Background(), task)
		if err == nil || !strings.Contains(err.Error(), "disk full") {
			t.Fatalf("err %v", err)
		}
		if len(mock.updateRequests) != 0 || readyReason(reconciled) == "TaskSuspended" || readyReason(reconciled) != "ActorSuspendFailed" {
			t.Fatalf("updates=%d reason %q", len(mock.updateRequests), readyReason(reconciled))
		}
	})
}
