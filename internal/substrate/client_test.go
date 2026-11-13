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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
		tmpl := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", "", nil, resources, nil)

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
		tmpl := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", "", nil, nil, nil)
		if tmpl.GetResources() != nil {
			t.Fatalf("want nil top-level Resources, got %v", tmpl.GetResources())
		}
	})

	t.Run("cpu only sets only the cpu limit", func(t *testing.T) {
		resources := &v1alpha1.ResourceReqs{
			Limits: &v1alpha1.ResourceList{Cpu: "2"},
		}
		tmpl := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", "", nil, resources, nil)

		got := tmpl.GetResources().GetLimits()
		if len(got) != 1 || limitQuantity(got, "cpu") != "2" {
			t.Fatalf("want top-level Resources.Limits == [{cpu 2}], got %v", got)
		}
	})

	t.Run("supervisor task sets both top-level and container Resources", func(t *testing.T) {
		resources := &v1alpha1.ResourceReqs{
			Limits: &v1alpha1.ResourceList{Cpu: "1", Memory: "2Gi"},
		}
		tmpl := BuildActorTemplate("atespace", "name", "image", nil, nil, "", v1alpha1.FabriekSupervisorProfile, "", nil, resources, nil)

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
		got := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", tc.sandboxClass, nil, nil, nil).GetSandboxConfig()
		if got.GetSandboxClass() != tc.wantClass || got.GetConfigName() != tc.wantConfig {
			t.Errorf("sandboxClass %q: got %v, want %v/%s", tc.sandboxClass, got, tc.wantClass, tc.wantConfig)
		}
	}
}

// A guest device makes the template select the pools that carry its label.
func TestBuildActorTemplateGuestDevices(t *testing.T) {
	if got := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", v1alpha1.SandboxClassMicroVM, nil, nil, nil).GetWorkerSelector(); got != nil {
		t.Errorf("no guest devices: WorkerSelector = %v, want nil", got)
	}
	got := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", v1alpha1.SandboxClassMicroVM, []string{v1alpha1.GuestDeviceFUSE}, nil, nil).GetWorkerSelector().GetMatchLabels()
	if len(got) != 1 || got["ate.dev/guest-device-fuse"] != "true" {
		t.Errorf("fuse: MatchLabels = %v, want only ate.dev/guest-device-fuse=true", got)
	}
}
