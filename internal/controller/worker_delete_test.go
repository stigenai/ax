package controller

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProtectedDeletionRejectsDelayedEventBeforeSubstrate(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	created := time.Now().UTC()
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "private", CreationTimestamp: timestamppb.New(created)}, Spec: &v1alpha1.TaskSpec{SupervisorProfile: "fabriek-native-v1"}, Status: &v1alpha1.TaskStatus{Phase: v1alpha1.PhaseTerminating}}
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	worker := &Worker{store: s}
	for _, precondition := range []store.TaskDeletePreconditions{{}, {ActorUID: "old-actor", CreationTimestamp: created.Add(-time.Second).Format(time.RFC3339Nano)}} {
		err := worker.processEvent(ctx, store.TaskEvent{Atespace: "private", Name: "job", Action: "delete", Deletion: precondition})
		if !errors.Is(err, store.ErrConflict) {
			t.Fatalf("stale event reached substrate: %v", err)
		}
	}
	// Queued reconcile after the deletion mark must not create or resume an actor.
	if err := worker.processEvent(ctx, store.TaskEvent{Atespace: "private", Name: "job", Action: "reconcile"}); err != nil {
		t.Fatal(err)
	}
}

type deletionControlServer struct {
	ateapipb.UnimplementedControlServer
	deletions chan *ateapipb.DeleteActorRequest
	templates chan *ateapipb.ListActorTemplatesRequest
}

func (s *deletionControlServer) DeleteActor(_ context.Context, request *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
	s.deletions <- request
	return &ateapipb.Actor{}, nil
}

func (s *deletionControlServer) ListActorTemplates(_ context.Context, request *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	s.templates <- request
	return &ateapipb.ListActorTemplatesResponse{}, nil
}

func TestMissingTaskDeletionPreservesOnlyLegacyCleanup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	control := &deletionControlServer{deletions: make(chan *ateapipb.DeleteActorRequest, 4), templates: make(chan *ateapipb.ListActorTemplatesRequest, 4)}
	server := grpc.NewServer()
	ateapipb.RegisterControlServer(server, control)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	client, err := substrate.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	worker := &Worker{store: memory.NewStore(), reconciler: NewTaskReconciler(client, "default-template", "ax-system")}
	created := time.Now().UTC().Format(time.RFC3339Nano)
	for _, test := range []struct {
		name         string
		precondition store.TaskDeletePreconditions
		wantConflict bool
	}{
		{"protected", store.TaskDeletePreconditions{ActorUID: "original-actor", CreationTimestamp: created}, false},
		{"actor-only", store.TaskDeletePreconditions{ActorUID: "original-actor"}, true},
		{"timestamp-only", store.TaskDeletePreconditions{CreationTimestamp: created}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := worker.processEvent(context.Background(), store.TaskEvent{Atespace: "private", Name: "missing", Action: "delete", Deletion: test.precondition})
			if test.wantConflict && !errors.Is(err, store.ErrConflict) || !test.wantConflict && err != nil {
				t.Fatalf("unexpected missing-task result: %v", err)
			}
			if len(control.deletions) != 0 || len(control.templates) != 0 {
				t.Fatal("bound missing-task event reached substrate")
			}
		})
	}
	if err := worker.processEvent(context.Background(), store.TaskEvent{Atespace: "private", Name: "missing", Action: "delete"}); err != nil {
		t.Fatal(err)
	}
	if len(control.deletions) != 1 || len(control.templates) != 1 {
		t.Fatalf("legacy cleanup skipped: actor calls=%d template calls=%d", len(control.deletions), len(control.templates))
	}
	request := <-control.deletions
	if request.GetActor().GetAtespace() != "private" || request.GetActor().GetName() != "missing" || request.GetOptions().GetUid() != "" {
		t.Fatalf("unexpected legacy actor deletion: %v", request)
	}
	if request := <-control.templates; request.GetAtespace() != "private" {
		t.Fatalf("unexpected legacy template lookup: %v", request)
	}
}
