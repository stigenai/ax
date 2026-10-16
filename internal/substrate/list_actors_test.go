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
	"fmt"
	"net"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// listActorsServer replays one ListActorsResponse per PageToken value,
// so a test can check that Client.ListActors follows NextPageToken until
// it is empty.
type listActorsServer struct {
	ateapipb.UnimplementedControlServer
	pages map[string]*ateapipb.ListActorsResponse
	err   error

	requests []*ateapipb.ListActorsRequest
}

func (s *listActorsServer) ListActors(_ context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	s.requests = append(s.requests, req)
	if s.err != nil {
		return nil, s.err
	}
	resp, ok := s.pages[req.GetPageToken()]
	if !ok {
		return nil, fmt.Errorf("unexpected page token %q", req.GetPageToken())
	}
	return resp, nil
}

func newListActorsTestClient(t *testing.T, srv *listActorsServer) *Client {
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

// TestListActorsFollowsPaging proves Client.ListActors keeps requesting the
// next page until NextPageToken is empty, and returns every actor from
// every page in order.
func TestListActorsFollowsPaging(t *testing.T) {
	page1 := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "a1"}}
	page2 := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "a2"}}
	srv := &listActorsServer{pages: map[string]*ateapipb.ListActorsResponse{
		"":      {Actors: []*ateapipb.Actor{page1}, NextPageToken: "page2"},
		"page2": {Actors: []*ateapipb.Actor{page2}},
	}}
	client := newListActorsTestClient(t, srv)

	got, err := client.ListActors(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].GetMetadata().GetName() != "a1" || got[1].GetMetadata().GetName() != "a2" {
		t.Fatalf("got %v, want [a1 a2]", got)
	}
	if len(srv.requests) != 2 {
		t.Fatalf("got %d requests, want 2", len(srv.requests))
	}
	if srv.requests[0].GetAtespace() != "default" || srv.requests[0].GetPageSize() != 500 {
		t.Fatalf("unexpected first request: %+v", srv.requests[0])
	}
	if srv.requests[1].GetPageToken() != "page2" {
		t.Fatalf("second request did not carry the next page token: %+v", srv.requests[1])
	}
}

// TestListActorsEmptyAtespaceListsAll proves an empty atespace argument
// reaches Substrate as an empty Atespace field, which lists across every
// atespace (ListActorsRequest's doc comment).
func TestListActorsEmptyAtespaceListsAll(t *testing.T) {
	srv := &listActorsServer{pages: map[string]*ateapipb.ListActorsResponse{"": {}}}
	client := newListActorsTestClient(t, srv)

	if _, err := client.ListActors(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if len(srv.requests) != 1 || srv.requests[0].GetAtespace() != "" {
		t.Fatalf("unexpected request: %+v", srv.requests)
	}
}

// TestListActorsPropagatesError proves a mid-paging error stops and
// returns, instead of returning a partial list silently.
func TestListActorsPropagatesError(t *testing.T) {
	srv := &listActorsServer{err: status.Error(codes.Unavailable, "boom")}
	client := newListActorsTestClient(t, srv)

	_, err := client.ListActors(context.Background(), "default")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("got error %v, want codes.Unavailable", err)
	}
}
