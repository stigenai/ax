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
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type taskTagAdapter interface {
	CreateTaskTag(context.Context, string, string, string) (*TaskTag, error)
	GetTaskTag(context.Context, string, string) (*TaskTag, error)
	ListTaskTags(context.Context, string) ([]*TaskTag, error)
}

type taskTagControl struct {
	ateapipb.ControlClient
	call func() error
}

type cancelAtCheck struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelAtCheck) Err() error {
	c.checks--
	if c.checks == 0 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestListTaskTagsCanceledProjection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	controlled := &cancelAtCheck{Context: ctx, cancel: cancel, checks: 4}
	client := &Client{control: taskTagControl{call: func() error { return nil }}}
	a, ok := any(client).(taskTagAdapter)
	if !ok {
		t.Fatal("Client lacks the task tag adapter contract")
	}
	got, err := a.ListTaskTags(controlled, "space")
	if got != nil || status.Code(err) != codes.Canceled {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

func (c taskTagControl) CreateTag(context.Context, *ateapipb.CreateTagRequest, ...grpc.CallOption) (*ateapipb.Tag, error) {
	return completedTaskTag(), c.call()
}
func (c taskTagControl) GetTag(context.Context, *ateapipb.GetTagRequest, ...grpc.CallOption) (*ateapipb.Tag, error) {
	return completedTaskTag(), c.call()
}
func (c taskTagControl) ListTags(context.Context, *ateapipb.ListTagsRequest, ...grpc.CallOption) (*ateapipb.ListTagsResponse, error) {
	return &ateapipb.ListTagsResponse{Tags: []*ateapipb.Tag{completedTaskTag()}}, c.call()
}

func TestTaskTagCanceledResponse(t *testing.T) {
	for _, op := range []string{"create", "get", "list"} {
		for _, before := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%v", op, before), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				client := &Client{control: taskTagControl{call: func() error {
					calls++
					cancel()
					return nil
				}}}
				a, ok := any(client).(taskTagAdapter)
				if !ok {
					t.Fatal("Client lacks the task tag adapter contract")
				}
				if before {
					cancel()
				}
				err := tagOperations(a)[op](ctx, "space", "actor", "tag")
				want := 1
				if before {
					want = 0
				}
				if status.Code(err) != codes.Canceled || calls != want {
					t.Fatalf("err=%v calls=%d", err, calls)
				}
			})
		}
	}
}

type taskTagServer struct {
	ateapipb.UnimplementedControlServer
	call  func(context.Context, string, any) (any, error)
	mu    sync.Mutex
	calls map[string]int
}

func (s *taskTagServer) CreateTag(ctx context.Context, req *ateapipb.CreateTagRequest) (*ateapipb.Tag, error) {
	v, err := s.call(ctx, "CreateTag", req)
	if err != nil {
		return nil, err
	}
	return v.(*ateapipb.Tag), nil
}
func (s *taskTagServer) GetTag(ctx context.Context, req *ateapipb.GetTagRequest) (*ateapipb.Tag, error) {
	v, err := s.call(ctx, "GetTag", req)
	if err != nil {
		return nil, err
	}
	return v.(*ateapipb.Tag), nil
}
func (s *taskTagServer) ListTags(ctx context.Context, req *ateapipb.ListTagsRequest) (*ateapipb.ListTagsResponse, error) {
	v, err := s.call(ctx, "ListTags", req)
	if err != nil {
		return nil, err
	}
	return v.(*ateapipb.ListTagsResponse), nil
}
func (s *taskTagServer) count(method string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if method != "" {
		return s.calls[method]
	}
	n := 0
	for _, v := range s.calls {
		n += v
	}
	return n
}

func taskTagClient(t *testing.T, s *taskTagServer) (*Client, taskTagAdapter) {
	t.Helper()
	s.calls = map[string]int{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		method := info.FullMethod[strings.LastIndex(info.FullMethod, "/")+1:]
		s.mu.Lock()
		s.calls[method]++
		s.mu.Unlock()
		return handler(ctx, req)
	}))
	ateapipb.RegisterControlServer(srv, s)
	go srv.Serve(lis)
	t.Cleanup(func() {
		srv.Stop()
		lis.Close()
		s.mu.Lock()
		defer s.mu.Unlock()
		for method, n := range s.calls {
			if method != "CreateTag" && method != "GetTag" && method != "ListTags" {
				t.Errorf("unexpected RPC %s: %d", method, n)
			}
		}
	})
	c, err := NewClientWithOptions(ClientOptions{Target: lis.Addr().String(), Plaintext: true}, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	a, ok := any(c).(taskTagAdapter)
	if !ok {
		t.Fatal("Client lacks the task tag adapter contract")
	}
	return c, a
}

func completedTaskTag() *ateapipb.Tag {
	return &ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: "space", Name: "tag", Uid: "tag-uid", CreateTime: timestamppb.New(time.Unix(123, 456))},
		Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: &ateapipb.ObjectRef{Atespace: "space", Name: "actor"},
		Status:      &ateapipb.TagStatus{ActorTemplateUid: "template-uid", StorageLocation: "s3://secret-location", Snapshot: &ateapipb.ExternalSnapshot{SnapshotUri: "s3://secret-copy", ActorTemplateUid: "template-uid"}},
	}
}

func tagOperations(a taskTagAdapter) map[string]func(context.Context, string, string, string) error {
	return map[string]func(context.Context, string, string, string) error{
		"create": func(ctx context.Context, space, actor, tag string) error {
			_, err := a.CreateTaskTag(ctx, space, actor, tag)
			return err
		},
		"get": func(ctx context.Context, space, actor, tag string) error {
			_, err := a.GetTaskTag(ctx, space, tag)
			return err
		},
		"list": func(ctx context.Context, space, actor, tag string) error {
			_, err := a.ListTaskTags(ctx, space)
			return err
		},
	}
}

func TestTaskTagInputBoundary(t *testing.T) {
	invalid := []string{"", strings.Repeat("a", 64), "A", "a.b", "a/b", " a", "a ", "a\n", "é", "-a", "a-", "*", "s3://bucket", "a: b", "$(id)", "a\x00"}
	for _, op := range []string{"create", "get", "list"} {
		for _, field := range []string{"space", "actor", "tag"} {
			if (op == "list" && field != "space") || (op == "get" && field == "actor") {
				continue
			}
			values := append([]string{}, invalid...)
			if field == "space" {
				values = append(values, "ate-golden")
			}
			for _, value := range values {
				t.Run(op+"/"+field+"/"+fmt.Sprintf("%q", value), func(t *testing.T) {
					s := &taskTagServer{call: func(context.Context, string, any) (any, error) {
						return nil, status.Error(codes.Internal, "unexpected")
					}}
					_, a := taskTagClient(t, s)
					space, actor, tag := "space", "actor", "tag"
					switch field {
					case "space":
						space = value
					case "actor":
						actor = value
					case "tag":
						tag = value
					}
					err := tagOperations(a)[op](context.Background(), space, actor, tag)
					if status.Code(err) != codes.InvalidArgument || s.count("") != 0 {
						t.Fatalf("error=%v calls=%d", err, s.count(""))
					}
				})
			}
		}
	}
	for _, name := range []string{"a", "0", "a-b", strings.Repeat("a", 63)} {
		t.Run("valid/"+name, func(t *testing.T) {
			s := &taskTagServer{call: func(_ context.Context, method string, req any) (any, error) {
				return nil, status.Error(codes.NotFound, "absent")
			}}
			_, a := taskTagClient(t, s)
			for op, call := range tagOperations(a) {
				if err := call(context.Background(), name, name, name); status.Code(err) != codes.NotFound {
					t.Fatalf("%s: %v", op, err)
				}
			}
			if s.count("") != 3 {
				t.Fatal(s.count(""))
			}
		})
	}
}

func TestCreateTaskTagScope(t *testing.T) {
	s := &taskTagServer{call: func(_ context.Context, method string, req any) (any, error) {
		want := &ateapipb.CreateTagRequest{Tag: &ateapipb.Tag{Metadata: &ateapipb.ResourceMetadata{Atespace: "space", Name: "tag"}, SourceActor: &ateapipb.ObjectRef{Atespace: "space", Name: "actor"}, Scope: ateapipb.TagScope_TAG_SCOPE_ATESPACE}}
		if method != "CreateTag" || !proto.Equal(req.(*ateapipb.CreateTagRequest), want) {
			t.Errorf("unexpected request: %v", req)
		}
		return completedTaskTag(), nil
	}}
	_, a := taskTagClient(t, s)
	got, err := a.CreateTaskTag(context.Background(), "space", "actor", "tag")
	if err != nil || got == nil || got.UID != "tag-uid" || s.count("") != 1 {
		t.Fatalf("got=%v err=%v calls=%d", got, err, s.count(""))
	}
}

func TestTaskTagReadiness(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ateapipb.Tag)
		code   codes.Code
		omit   bool
	}{
		{"complete", func(*ateapipb.Tag) {}, codes.OK, false},
		{"complete-without-nested-template", func(v *ateapipb.Tag) { v.Status.Snapshot.ActorTemplateUid = "" }, codes.OK, false},
		{"pending", func(v *ateapipb.Tag) { v.Status.Snapshot = nil }, codes.FailedPrecondition, true},
		{"empty-uri", func(v *ateapipb.Tag) { v.Status.Snapshot.SnapshotUri = "" }, codes.FailedPrecondition, true},
		{"published", func(v *ateapipb.Tag) { v.Scope = ateapipb.TagScope_TAG_SCOPE_PUBLISHED }, codes.NotFound, true},
		{"scope", func(v *ateapipb.Tag) { v.Scope = 99 }, codes.DataLoss, false},
		{"unspecified-scope", func(v *ateapipb.Tag) { v.Scope = 0 }, codes.DataLoss, false},
		{"space", func(v *ateapipb.Tag) { v.Metadata.Atespace = "other" }, codes.DataLoss, false},
		{"source-space", func(v *ateapipb.Tag) { v.SourceActor.Atespace = "other" }, codes.DataLoss, false},
		{"source-name", func(v *ateapipb.Tag) { v.SourceActor.Name = "BAD" }, codes.DataLoss, false},
		{"name", func(v *ateapipb.Tag) { v.Metadata.Name = "BAD" }, codes.DataLoss, false},
		{"uid", func(v *ateapipb.Tag) { v.Metadata.Uid = "" }, codes.DataLoss, false},
		{"template", func(v *ateapipb.Tag) { v.Status.ActorTemplateUid = "" }, codes.DataLoss, false},
		{"snapshot-template", func(v *ateapipb.Tag) { v.Status.Snapshot.ActorTemplateUid = "other" }, codes.DataLoss, false},
		{"timestamp", func(v *ateapipb.Tag) { v.Metadata.CreateTime = &timestamppb.Timestamp{Nanos: -1} }, codes.DataLoss, false},
		{"no-timestamp", func(v *ateapipb.Tag) { v.Metadata.CreateTime = nil }, codes.OK, false},
		{"no-metadata", func(v *ateapipb.Tag) { v.Metadata = nil }, codes.DataLoss, false},
		{"no-source", func(v *ateapipb.Tag) { v.SourceActor = nil }, codes.DataLoss, false},
		{"no-status", func(v *ateapipb.Tag) { v.Status = nil }, codes.DataLoss, false},
		{"pending-invalid-uid", func(v *ateapipb.Tag) { v.Status.Snapshot = nil; v.Metadata.Uid = "" }, codes.DataLoss, false},
		{"published-invalid-source", func(v *ateapipb.Tag) {
			v.Scope = ateapipb.TagScope_TAG_SCOPE_PUBLISHED
			v.SourceActor.Atespace = "other"
		}, codes.DataLoss, false},
	} {
		for _, op := range []string{"create", "get", "list"} {
			t.Run(tc.name+"/"+op, func(t *testing.T) {
				row := completedTaskTag()
				tc.change(row)
				s := &taskTagServer{call: func(_ context.Context, method string, req any) (any, error) {
					if method == "GetTag" && !proto.Equal(req.(*ateapipb.GetTagRequest), &ateapipb.GetTagRequest{Tag: &ateapipb.ObjectRef{Atespace: "space", Name: "tag"}}) {
						t.Errorf("bad get: %v", req)
					}
					if method == "ListTags" {
						return &ateapipb.ListTagsResponse{Tags: []*ateapipb.Tag{completedTaskTag(), row}}, nil
					}
					return row, nil
				}}
				_, a := taskTagClient(t, s)
				want := tc.code
				if op == "create" && tc.name == "published" {
					want = codes.DataLoss
				}
				if op == "list" && tc.omit {
					want = codes.OK
				}
				if op == "list" {
					got, err := a.ListTaskTags(context.Background(), "space")
					if status.Code(err) != want || (err != nil && got != nil) || (err == nil && len(got) != map[bool]int{true: 1, false: 2}[tc.omit]) {
						t.Fatalf("got=%v err=%v want=%v", got, err, want)
					}
				} else {
					var got *TaskTag
					var err error
					if op == "create" {
						got, err = a.CreateTaskTag(context.Background(), "space", "actor", "tag")
					} else {
						got, err = a.GetTaskTag(context.Background(), "space", "tag")
					}
					if status.Code(err) != want || (err != nil && got != nil) {
						t.Fatalf("got=%v err=%v want=%v", got, err, want)
					}
				}
			})
		}
	}
	for _, field := range []string{"name", "source"} {
		for _, op := range []string{"create", "get"} {
			if op == "get" && field == "source" {
				continue
			}
			t.Run("mismatch/"+field+"/"+op, func(t *testing.T) {
				row := completedTaskTag()
				if field == "name" {
					row.Metadata.Name = "other"
				} else {
					row.SourceActor.Name = "other"
				}
				_, a := taskTagClient(t, &taskTagServer{call: func(context.Context, string, any) (any, error) { return row, nil }})
				if err := tagOperations(a)[op](context.Background(), "space", "actor", "tag"); status.Code(err) != codes.DataLoss {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestTaskTagCleanupInventory(t *testing.T) {
	pending, published := completedTaskTag(), completedTaskTag()
	pending.Status.Snapshot = nil
	published.Scope = ateapipb.TagScope_TAG_SCOPE_PUBLISHED
	c, _ := taskTagClient(t, &taskTagServer{call: func(_ context.Context, _ string, req any) (any, error) {
		if req.(*ateapipb.ListTagsRequest).Atespace != "" {
			t.Error("cleanup scope changed")
		}
		return &ateapipb.ListTagsResponse{Tags: []*ateapipb.Tag{pending, published}}, nil
	}})
	got, err := c.ListTags(context.Background(), "")
	if err != nil || len(got) != 2 || !proto.Equal(got[0], pending) || !proto.Equal(got[1], published) {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

func TestTaskTagFailure(t *testing.T) {
	for _, code := range []codes.Code{codes.Canceled, codes.DeadlineExceeded, codes.NotFound, codes.AlreadyExists, codes.FailedPrecondition, codes.Unauthenticated, codes.PermissionDenied, codes.Unavailable, codes.ResourceExhausted, codes.Internal, codes.Unknown, codes.Aborted, codes.DataLoss, codes.InvalidArgument} {
		for _, op := range []string{"create", "get", "list"} {
			t.Run(code.String()+"/"+op, func(t *testing.T) {
				backend, _ := status.New(code, "s3://secret-copy zqsecret").WithDetails(&ateapipb.ObjectRef{Name: "zqsecret"})
				s := &taskTagServer{call: func(context.Context, string, any) (any, error) { return nil, backend.Err() }}
				_, a := taskTagClient(t, s)
				err := tagOperations(a)[op](context.Background(), "space", "actor", "tag")
				want := code
				if code == codes.Unknown || code == codes.Aborted || code == codes.DataLoss || code == codes.InvalidArgument {
					want = codes.Internal
				}
				if status.Code(err) != want || strings.Contains(fmt.Sprint(err), "secret") || len(status.Convert(err).Details()) != 0 || s.count("") != 1 {
					t.Fatalf("err=%v calls=%d", err, s.count(""))
				}
			})
		}
	}
	for _, op := range []string{"create", "get", "list"} {
		for _, before := range []bool{false, true} {
			t.Run(fmt.Sprintf("cancel/%s/%v", op, before), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				finished := make(chan struct{})
				s := &taskTagServer{call: func(ctx context.Context, method string, _ any) (any, error) {
					cancel()
					<-ctx.Done()
					close(finished)
					return nil, status.Error(codes.Canceled, "zqsecret")
				}}
				_, a := taskTagClient(t, s)
				if before {
					cancel()
				}
				err := tagOperations(a)[op](ctx, "space", "actor", "tag")
				if status.Code(err) != codes.Canceled {
					t.Fatal(err)
				}
				if before {
					if s.count("") != 0 {
						t.Fatal("RPC before entry")
					}
				} else {
					select {
					case <-finished:
					case <-time.After(5 * time.Second):
						t.Fatal("RPC did not stop")
					}
					if s.count("") != 1 {
						t.Fatal(s.count(""))
					}
				}
			})
		}
	}
}

func TestCreateTaskTagCollision(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			row := completedTaskTag()
			if pending {
				row.Status.Snapshot = nil
			}
			before := proto.Clone(row)
			s := &taskTagServer{call: func(context.Context, string, any) (any, error) {
				return nil, status.Error(codes.AlreadyExists, "s3://secret-copy")
			}}
			_, a := taskTagClient(t, s)
			got, err := a.CreateTaskTag(context.Background(), "space", "actor", "tag")
			if got != nil || status.Code(err) != codes.AlreadyExists || s.count("CreateTag") != 1 || s.count("") != 1 || !proto.Equal(row, before) {
				t.Fatalf("got=%v err=%v", got, err)
			}
		})
	}
}

func TestCreateTaskTagCopyFailure(t *testing.T) {
	row := completedTaskTag()
	row.Status.Snapshot = nil
	before := proto.Clone(row)
	s := &taskTagServer{call: func(_ context.Context, method string, _ any) (any, error) {
		if method == "CreateTag" {
			return nil, status.Error(codes.Internal, "copy failed: s3://secret-copy zqsecret")
		}
		return row, nil
	}}
	_, a := taskTagClient(t, s)
	got, err := a.CreateTaskTag(context.Background(), "space", "actor", "tag")
	if got != nil || status.Code(err) != codes.Internal || strings.Contains(fmt.Sprint(err), "secret") {
		t.Fatalf("got=%v err=%v", got, err)
	}
	got, err = a.GetTaskTag(context.Background(), "space", "tag")
	if got != nil || status.Code(err) != codes.FailedPrecondition || !proto.Equal(row, before) || s.count("CreateTag") != 1 || s.count("") != 2 {
		t.Fatalf("got=%v err=%v calls=%d", got, err, s.count(""))
	}
}

func TestListTaskTagsPages(t *testing.T) {
	for _, mode := range []string{"complete", "repeat", "cycle", "error", "malformed", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tokens := []string{"", "opaque/one", "opaque two", "opaque:three"}
			calls := 0
			s := &taskTagServer{call: func(_ context.Context, _ string, req any) (any, error) {
				r := req.(*ateapipb.ListTagsRequest)
				if calls >= len(tokens) {
					return nil, status.Error(codes.Internal, "too many pages")
				}
				if r.Atespace != "space" || r.PageSize != 500 || r.PageToken != tokens[calls] {
					t.Errorf("bad page: %v", r)
				}
				calls++
				resp := &ateapipb.ListTagsResponse{}
				if calls < 4 {
					resp.NextPageToken = tokens[calls]
				}
				if calls == 2 {
					row := completedTaskTag()
					row.Status.Snapshot = nil
					resp.Tags = []*ateapipb.Tag{row}
				}
				if calls == 3 {
					resp.Tags = []*ateapipb.Tag{completedTaskTag()}
				}
				if calls == 4 {
					switch mode {
					case "repeat":
						resp.NextPageToken = tokens[3]
					case "cycle":
						resp.NextPageToken = tokens[1]
					case "error":
						return nil, status.Error(codes.PermissionDenied, "zqsecret")
					case "malformed":
						resp.Tags = []*ateapipb.Tag{{}}
					case "cancel":
						cancel()
					}
				}
				return resp, nil
			}}
			_, a := taskTagClient(t, s)
			got, err := a.ListTaskTags(ctx, "space")
			if mode == "complete" {
				if err != nil || len(got) != 1 || got[0].Name != "tag" {
					t.Fatalf("got=%v err=%v", got, err)
				}
			} else if err == nil || got != nil {
				t.Fatalf("partial list: %v err=%v", got, err)
			}
			if calls != 4 {
				t.Fatal(calls)
			}
		})
	}
}

func TestListTaskTagsEmpty(t *testing.T) {
	s := &taskTagServer{call: func(_ context.Context, _ string, req any) (any, error) {
		if !proto.Equal(req.(*ateapipb.ListTagsRequest), &ateapipb.ListTagsRequest{Atespace: "space", PageSize: 500}) {
			t.Errorf("unexpected request: %v", req)
		}
		return &ateapipb.ListTagsResponse{}, nil
	}}
	_, a := taskTagClient(t, s)
	got, err := a.ListTaskTags(context.Background(), "space")
	if err != nil || len(got) != 0 || s.count("") != 1 {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

func TestTaskTagProjection(t *testing.T) {
	if reflect.TypeOf(TaskTag{}).NumField() != 6 {
		t.Fatal("unexpected result fields")
	}
	row := completedTaskTag()
	row.Metadata.Version = 987
	row.Metadata.UpdateTime = timestamppb.Now()
	want := &TaskTag{Atespace: "space", Name: "tag", UID: "tag-uid", SourceActorName: "actor", ActorTemplateUID: "template-uid", CreateTime: time.Unix(123, 456).UTC()}
	_, a := taskTagClient(t, &taskTagServer{call: func(_ context.Context, method string, _ any) (any, error) {
		if method == "ListTags" {
			return &ateapipb.ListTagsResponse{Tags: []*ateapipb.Tag{row}}, nil
		}
		return row, nil
	}})
	created, e1 := a.CreateTaskTag(context.Background(), "space", "actor", "tag")
	got, e2 := a.GetTaskTag(context.Background(), "space", "tag")
	list, e3 := a.ListTaskTags(context.Background(), "space")
	if e1 != nil || e2 != nil || e3 != nil || len(list) != 1 {
		t.Fatalf("errors: %v %v %v", e1, e2, e3)
	}
	for _, v := range []*TaskTag{created, got, list[0]} {
		if !reflect.DeepEqual(v, want) || strings.Contains(fmt.Sprint(v), "secret") {
			t.Fatalf("unsafe projection: %v", v)
		}
	}
}

type tagListClient struct {
	ateapipb.ControlClient
	list func(context.Context, *ateapipb.ListTagsRequest) (*ateapipb.ListTagsResponse, error)
}

func (c tagListClient) ListTags(ctx context.Context, req *ateapipb.ListTagsRequest, _ ...grpc.CallOption) (*ateapipb.ListTagsResponse, error) {
	return c.list(ctx, req)
}

func TestListTagsPages(t *testing.T) {
	for _, space := range []string{"", "explicit"} {
		for _, count := range []int{0, 1, 2, 4} {
			t.Run(space+"/"+string(rune('0'+count)), func(t *testing.T) {
				tokens := []string{"", "opaque/one", "opaque two", "opaque:three"}
				calls := 0
				client := &Client{control: tagListClient{list: func(ctx context.Context, req *ateapipb.ListTagsRequest) (*ateapipb.ListTagsResponse, error) {
					if req.GetAtespace() != space || req.GetPageSize() != 500 || calls >= len(tokens) || req.GetPageToken() != tokens[calls] {
						t.Fatalf("unexpected request: %v", req)
					}
					calls++
					resp := &ateapipb.ListTagsResponse{}
					if count > 0 && calls == 1 {
						resp.Tags = []*ateapipb.Tag{{Metadata: &ateapipb.ResourceMetadata{Name: "first"}}}
					}
					if count > 2 && calls == 3 {
						resp.Tags = []*ateapipb.Tag{{Metadata: &ateapipb.ResourceMetadata{Name: "last"}}}
					}
					if calls < count {
						resp.NextPageToken = tokens[calls]
					}
					return resp, nil
				}}}
				tags, err := client.ListTags(context.Background(), space)
				want := 0
				if count > 0 {
					want = 1
				}
				if count > 2 {
					want = 2
				}
				if err != nil || len(tags) != want || calls != max(1, count) {
					t.Fatalf("tags=%v calls=%d error=%v", tags, calls, err)
				}
				if want > 0 && tags[0].GetMetadata().GetName() != "first" {
					t.Fatal("lost first page")
				}
				if want > 1 && tags[1].GetMetadata().GetName() != "last" {
					t.Fatal("lost later page")
				}
			})
		}
	}
}

func TestListTagsFailure(t *testing.T) {
	for _, mode := range []string{"RPC", "cancel", "canceled before call", "repeat", "cycle"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "canceled before call" {
				cancel()
			}
			calls := 0
			client := &Client{control: tagListClient{list: func(ctx context.Context, req *ateapipb.ListTagsRequest) (*ateapipb.ListTagsResponse, error) {
				if mode == "canceled before call" {
					t.Fatal("RPC on canceled context")
				}
				calls++
				if calls > 3 {
					t.Fatal("did not stop after repeated token")
				}
				if calls == 2 && mode == "RPC" {
					return nil, status.Error(codes.PermissionDenied, "RPC failed")
				}
				if mode == "cancel" {
					cancel()
					return &ateapipb.ListTagsResponse{Tags: []*ateapipb.Tag{{}}}, nil
				}
				token := "one"
				if mode == "cycle" && calls == 2 {
					token = "two"
				}
				return &ateapipb.ListTagsResponse{Tags: []*ateapipb.Tag{{}}, NextPageToken: token}, nil
			}}}
			tags, err := client.ListTags(ctx, "")
			if err == nil || tags != nil {
				t.Fatalf("want error without partial inventory, got %v, %v", tags, err)
			}
			if mode == "RPC" && status.Code(err) != codes.PermissionDenied {
				t.Fatalf("lost RPC status: %v", err)
			}
			if (mode == "cancel" || mode == "canceled before call") && status.Code(err) != codes.Canceled {
				t.Fatalf("lost cancellation: %v", err)
			}
		})
	}
}

// limitQuantity returns the quantity string for a named limit, or "" if the
// limit is absent.
func limitQuantity(limits []*ateapipb.Limits, name string) string {
	for _, l := range limits {
		if l.GetName() == name {
			return l.GetQuantity()
		}
	}
	return ""
}

func TestBuildActorTemplateResources(t *testing.T) {
	t.Run("ordinary task with memory limit sets top-level Resources only", func(t *testing.T) {
		resources := &v1alpha1.ResourceReqs{
			Limits: &v1alpha1.ResourceList{Memory: "4Gi"},
		}
		tmpl := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", "", resources, nil)

		if tmpl.GetResources() == nil {
			t.Fatalf("want top-level Resources set, got nil")
		}
		got := tmpl.GetResources().GetLimits()
		if len(got) != 1 || limitQuantity(got, "memory") != "4Gi" {
			t.Fatalf("want top-level Resources.Limits == [{memory 4Gi}], got %v", got)
		}
		if tmpl.Containers[0].GetResources() != nil {
			t.Fatalf("want Containers[0].Resources nil for an ordinary task, got %v", tmpl.Containers[0].GetResources())
		}
	})

	t.Run("nil resources gives nil top-level Resources", func(t *testing.T) {
		tmpl := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", "", nil, nil)
		if tmpl.GetResources() != nil {
			t.Fatalf("want nil top-level Resources, got %v", tmpl.GetResources())
		}
	})

	t.Run("cpu only sets only the cpu limit", func(t *testing.T) {
		resources := &v1alpha1.ResourceReqs{
			Limits: &v1alpha1.ResourceList{Cpu: "2"},
		}
		tmpl := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", "", resources, nil)

		got := tmpl.GetResources().GetLimits()
		if len(got) != 1 || limitQuantity(got, "cpu") != "2" {
			t.Fatalf("want top-level Resources.Limits == [{cpu 2}], got %v", got)
		}
	})

	t.Run("supervisor task sets both top-level and container Resources", func(t *testing.T) {
		resources := &v1alpha1.ResourceReqs{
			Limits: &v1alpha1.ResourceList{Cpu: "1", Memory: "2Gi"},
		}
		tmpl := BuildActorTemplate("atespace", "name", "image", nil, nil, "", v1alpha1.FabriekSupervisorProfile, "", resources, nil)

		gotTop := tmpl.GetResources().GetLimits()
		if len(gotTop) != 2 || limitQuantity(gotTop, "cpu") != "1" || limitQuantity(gotTop, "memory") != "2Gi" {
			t.Fatalf("want top-level Resources.Limits == [{cpu 1} {memory 2Gi}], got %v", gotTop)
		}
		gotContainer := tmpl.Containers[0].GetResources().GetLimits()
		if len(gotContainer) != 2 || limitQuantity(gotContainer, "cpu") != "1" || limitQuantity(gotContainer, "memory") != "2Gi" {
			t.Fatalf("want Containers[0].Resources.Limits == [{cpu 1} {memory 2Gi}], got %v", gotContainer)
		}
	})
}

func TestBuildActorTemplateSandboxClass(t *testing.T) {
	for _, tc := range []struct {
		sandboxClass string
		wantClass    ateapipb.SandboxClass
		wantConfig   string
	}{
		{"", ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, "gvisor-default"},
		{v1alpha1.SandboxClassGVisor, ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, "gvisor-default"},
		{v1alpha1.SandboxClassMicroVM, ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, "microvm"},
	} {
		got := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", tc.sandboxClass, nil, nil).GetSandboxConfig()
		if got.GetSandboxClass() != tc.wantClass || got.GetConfigName() != tc.wantConfig {
			t.Errorf("sandboxClass %q: got %v, want %v/%s", tc.sandboxClass, got, tc.wantClass, tc.wantConfig)
		}
	}
}
