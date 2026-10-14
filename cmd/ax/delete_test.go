package main

import (
	"context"
	"net"
	"testing"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type deletionServer struct {
	v1alpha1.UnimplementedAXServer
	requests chan *v1alpha1.DeleteTaskRequest
}

func (s *deletionServer) DeleteTask(_ context.Context, req *v1alpha1.DeleteTaskRequest) (*v1alpha1.DeleteTaskResponse, error) {
	s.requests <- proto.Clone(req).(*v1alpha1.DeleteTaskRequest)
	return &v1alpha1.DeleteTaskResponse{}, nil
}

func (*deletionServer) GetTask(context.Context, *v1alpha1.GetTaskRequest) (*v1alpha1.Task, error) {
	return nil, status.Error(codes.NotFound, "deleted fixture")
}

func TestDeleteCLIForwardsExactIdentity(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	fixture := &deletionServer{requests: make(chan *v1alpha1.DeleteTaskRequest, 1)}
	srv := grpc.NewServer()
	v1alpha1.RegisterAXServer(srv, fixture)
	go srv.Serve(listener)
	defer srv.Stop()
	const uid = "b8fe2249-1732-4d5a-b539-a394ad10f93d"
	const created = "2026-10-14T19:00:00.000000123Z"
	if err := runDelete(listener.Addr().String(), "private", []string{"task", "job", "--actor-uid", uid, "--creation-timestamp", created}); err != nil {
		t.Fatal(err)
	}
	req := <-fixture.requests
	if req.GetActorUid() != uid || req.GetCreationTimestamp() != created || req.GetName() != "job" || req.GetAtespace() != "private" {
		t.Fatal("CLI lost exact identity")
	}
	for _, args := range [][]string{
		{"task", "job", "--actor-uid", uid},
		{"task", "job", "--creation-timestamp", created},
		{"task", "job", "--actor-uid", uid, "--creation-timestamp", "invalid"},
		{"workspace", "job", "--actor-uid", uid, "--creation-timestamp", created},
		{"task", "job", "--unknown", "value"},
	} {
		if err := runDelete(listener.Addr().String(), "private", args); err == nil {
			t.Fatal("invalid CLI options accepted")
		}
	}
	if len(fixture.requests) != 0 {
		t.Fatal("invalid request reached server")
	}
}
