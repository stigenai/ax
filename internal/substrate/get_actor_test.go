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
	"net"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type getActorOnlyServer struct {
	ateapipb.UnimplementedControlServer
	actor *ateapipb.Actor
	err   error
}

func (s *getActorOnlyServer) GetActor(context.Context, *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	return s.actor, s.err
}

func newGetActorTestClient(t *testing.T, srv *getActorOnlyServer) *Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })
	grpcSrv := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	t.Cleanup(grpcSrv.Stop)

	client, err := NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// TestGetActorReturnsActor proves Client.GetActor passes through a found
// actor unchanged.
func TestGetActorReturnsActor(t *testing.T) {
	want := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: "task1", Atespace: "default"},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.244.3.47"},
		},
	}
	client := newGetActorTestClient(t, &getActorOnlyServer{actor: want})

	got, err := client.GetActor(context.Background(), "default", "task1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("got state %v, want RUNNING", got.GetStatus().GetState())
	}
	if got.GetStatus().GetWorkerAssignment().GetWorkerPodIp() != "10.244.3.47" {
		t.Fatalf("got worker pod ip %q, want %q", got.GetStatus().GetWorkerAssignment().GetWorkerPodIp(), "10.244.3.47")
	}
}

// TestGetActorReturnsNotFound proves Client.GetActor passes through a
// NotFound error unchanged, so a caller can check status.Code(err).
func TestGetActorReturnsNotFound(t *testing.T) {
	client := newGetActorTestClient(t, &getActorOnlyServer{err: status.Error(codes.NotFound, "actor not found")})

	_, err := client.GetActor(context.Background(), "default", "task1")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("got error %v, want codes.NotFound", err)
	}
}
