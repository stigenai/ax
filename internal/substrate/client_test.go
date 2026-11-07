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

package substrate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockControlClient struct {
	ateapipb.ControlClient

	mu             sync.Mutex
	deleteCalls    []*ateapipb.DeleteActorRequest
	getCalls       []*ateapipb.GetActorRequest
	deleteErr      error
	actorRemaining int // Number of GetActor calls before returning NotFound
}

func (m *mockControlClient) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalls = append(m.deleteCalls, req)
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	return &ateapipb.Actor{}, nil
}

func (m *mockControlClient) GetActor(ctx context.Context, req *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCalls = append(m.getCalls, req)
	if m.actorRemaining > 0 {
		m.actorRemaining--
		return &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: req.GetActor().GetAtespace(),
				Name:     req.GetActor().GetName(),
			},
			Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
		}, nil
	}
	return nil, status.Error(codes.NotFound, "actor not found")
}

func TestClient_DeleteActor_AnyStateTrue(t *testing.T) {
	mock := &mockControlClient{
		actorRemaining: 0,
	}
	c := &Client{control: mock}
	ctx := context.Background()

	err := c.DeleteActor(ctx, "test-ns", "test-actor")
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.deleteCalls) != 1 {
		t.Fatalf("expected 1 DeleteActor call, got %d", len(mock.deleteCalls))
	}
	req := mock.deleteCalls[0]
	if !req.GetAnyState() {
		t.Errorf("expected AnyState to be true, got %v", req.GetAnyState())
	}
	if req.GetActor().GetAtespace() != "test-ns" || req.GetActor().GetName() != "test-actor" {
		t.Errorf("unexpected actor ref in request: %v", req.GetActor())
	}
}

func TestClient_DeleteActor_PollsUntilNotFound(t *testing.T) {
	oldInterval := actorDeletionPollInterval
	actorDeletionPollInterval = 10 * time.Millisecond
	defer func() { actorDeletionPollInterval = oldInterval }()

	mock := &mockControlClient{
		actorRemaining: 3,
	}
	c := &Client{control: mock}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.DeleteActor(ctx, "test-ns", "test-actor")
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	// 1 fast-path GetActor call + 3 polling calls (last one returns NotFound)
	if len(mock.getCalls) != 4 {
		t.Errorf("expected 4 GetActor calls before NotFound, got %d", len(mock.getCalls))
	}
}

func TestClient_DeleteActor_NotFoundIsIgnored(t *testing.T) {
	mock := &mockControlClient{
		deleteErr: status.Error(codes.NotFound, "actor already gone"),
	}
	c := &Client{control: mock}
	ctx := context.Background()

	err := c.DeleteActor(ctx, "test-ns", "test-actor")
	if err != nil {
		t.Fatalf("expected nil error when actor is NotFound, got: %v", err)
	}
}

func TestClient_DeleteActor_PropagatesRPCError(t *testing.T) {
	mock := &mockControlClient{
		deleteErr: errors.New("internal atelet failure"),
	}
	c := &Client{control: mock}
	ctx := context.Background()

	err := c.DeleteActor(ctx, "test-ns", "test-actor")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestBuildActorTemplate_SandboxDefaultsToGVisor(t *testing.T) {
	tmpl, err := BuildActorTemplate("ns", "t", "", nil, nil, "gs://b/env/", "", "")
	if err != nil {
		t.Fatalf("BuildActorTemplate: %v", err)
	}
	sc := tmpl.GetSandboxConfig()
	if sc.GetSandboxClass() != ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR || sc.GetConfigName() != "gvisor-default" {
		t.Errorf("got %v/%q, want gvisor/gvisor-default", sc.GetSandboxClass(), sc.GetConfigName())
	}
}

func TestBuildActorTemplate_Microvm(t *testing.T) {
	tmpl, err := BuildActorTemplate("ns", "t", "", nil, nil, "gs://b/env/", "microvm", "microvm-default")
	if err != nil {
		t.Fatalf("BuildActorTemplate: %v", err)
	}
	sc := tmpl.GetSandboxConfig()
	if sc.GetSandboxClass() != ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM || sc.GetConfigName() != "microvm-default" {
		t.Errorf("got %v/%q, want microvm/microvm-default", sc.GetSandboxClass(), sc.GetConfigName())
	}
}

func TestBuildActorTemplate_RejectsBadSandbox(t *testing.T) {
	for _, tc := range []struct{ class, config string }{
		{"firecracker", "x"}, // unknown class
		{"microvm", ""},      // non-default class needs a config name
	} {
		if _, err := BuildActorTemplate("ns", "t", "", nil, nil, "gs://b/env/", tc.class, tc.config); err == nil {
			t.Errorf("class %q config %q: want error", tc.class, tc.config)
		}
	}
}

func TestBuildActorTemplate_BucketHasNoDefault(t *testing.T) {
	t.Setenv("AX_SNAPSHOTS_BUCKET", "")
	if _, err := BuildActorTemplate("ns", "t", "", nil, nil, "", "", ""); err == nil {
		t.Fatal("want error when no snapshot bucket is set")
	}
	t.Setenv("AX_SNAPSHOTS_BUCKET", "gs://from-env/")
	tmpl, err := BuildActorTemplate("ns", "t", "", nil, nil, "", "", "")
	if err != nil {
		t.Fatalf("BuildActorTemplate: %v", err)
	}
	if got := tmpl.GetSnapshotsConfig().GetStorageLocation(); got != "gs://from-env/" {
		t.Errorf("bucket = %q, want gs://from-env/", got)
	}
}
