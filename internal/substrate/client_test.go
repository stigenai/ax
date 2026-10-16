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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

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
		tmpl := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", resources, nil)

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
		tmpl := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", nil, nil)
		if tmpl.GetResources() != nil {
			t.Fatalf("want nil top-level Resources, got %v", tmpl.GetResources())
		}
	})

	t.Run("cpu only sets only the cpu limit", func(t *testing.T) {
		resources := &v1alpha1.ResourceReqs{
			Limits: &v1alpha1.ResourceList{Cpu: "2"},
		}
		tmpl := BuildActorTemplate("atespace", "name", "image", nil, nil, "", "", resources, nil)

		got := tmpl.GetResources().GetLimits()
		if len(got) != 1 || limitQuantity(got, "cpu") != "2" {
			t.Fatalf("want top-level Resources.Limits == [{cpu 2}], got %v", got)
		}
	})

	t.Run("supervisor task sets both top-level and container Resources", func(t *testing.T) {
		resources := &v1alpha1.ResourceReqs{
			Limits: &v1alpha1.ResourceList{Cpu: "1", Memory: "2Gi"},
		}
		tmpl := BuildActorTemplate("atespace", "name", "image", nil, nil, "", v1alpha1.FabriekSupervisorProfile, resources, nil)

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
