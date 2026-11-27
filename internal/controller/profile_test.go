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
	"maps"
	"strings"
	"testing"

	"github.com/google/ax/internal/apivalidation"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
)

func TestEffectiveProfile(t *testing.T) {
	empty := controller.EffectiveProfile(nil, nil)
	if empty.GetImage() != "" || empty.GetResources() != nil || empty.GetAteTemplate().GetName() != "" {
		t.Fatalf("EffectiveProfile(nil, nil) = %+v, want empty Profile", empty)
	}

	wsImageProfile := &v1alpha1.Profile{
		Image: "ws/image@sha256:aaa",
		Resources: &v1alpha1.ResourcesSpec{
			Limits: &v1alpha1.ResourceList{Cpu: "4", Memory: "8Gi"},
		},
	}

	// Scenario 1: nil or empty taskProfile falls back to workspaceProfile as a whole.
	for _, emptyTaskProfile := range []*v1alpha1.Profile{
		nil,
		{},
		{Resources: &v1alpha1.ResourcesSpec{}},
		{Resources: &v1alpha1.ResourcesSpec{Limits: &v1alpha1.ResourceList{}}},
	} {
		got := controller.EffectiveProfile(wsImageProfile, emptyTaskProfile)
		if got.GetImage() != "ws/image@sha256:aaa" ||
			got.GetResources().GetLimits().GetCpu() != "4" ||
			got.GetResources().GetLimits().GetMemory() != "8Gi" {
			t.Fatalf("EffectiveProfile(wsImageProfile, %+v) = %+v, want wsImageProfile", emptyTaskProfile, got)
		}
		// Mutating returned profile must not mutate workspaceProfile.
		got.Resources.Limits.Cpu = "8"
		if wsImageProfile.Resources.Limits.Cpu != "4" {
			t.Fatal("EffectiveProfile mutated workspaceProfile.Resources")
		}
	}

	// Workspace ateTemplate.name is also inherited when taskProfile is empty.
	inheritedTpl := controller.EffectiveProfile(&v1alpha1.Profile{AteTemplate: &v1alpha1.ObjectRef{Name: "gpu-base"}}, &v1alpha1.Profile{})
	if inheritedTpl.GetAteTemplate().GetName() != "gpu-base" {
		t.Fatalf("inherited ateTemplate.name = %+v, want gpu-base", inheritedTpl)
	}

	// Scenario 2: taskProfile sets resources only -> used as-is; does NOT inherit
	// workspaceProfile's image or CPU limit (omitted fields fall back to server defaults).
	taskResourcesOnly := &v1alpha1.Profile{
		Resources: &v1alpha1.ResourcesSpec{
			Limits: &v1alpha1.ResourceList{Memory: "16Gi"},
		},
	}
	gotResourcesOnly := controller.EffectiveProfile(wsImageProfile, taskResourcesOnly)
	if gotResourcesOnly.GetImage() != "" {
		t.Errorf("image = %q, want empty (must not inherit workspace image)", gotResourcesOnly.GetImage())
	}
	if gotResourcesOnly.GetResources().GetLimits().GetCpu() != "" {
		t.Errorf("cpu = %q, want empty (must not inherit workspace cpu)", gotResourcesOnly.GetResources().GetLimits().GetCpu())
	}
	if gotResourcesOnly.GetResources().GetLimits().GetMemory() != "16Gi" {
		t.Errorf("memory = %q, want 16Gi", gotResourcesOnly.GetResources().GetLimits().GetMemory())
	}
	// Mutating returned profile must not mutate taskProfile.
	gotResourcesOnly.Resources.Limits.Memory = "32Gi"
	if taskResourcesOnly.Resources.Limits.Memory != "16Gi" {
		t.Fatal("EffectiveProfile mutated taskProfile.Resources")
	}

	// Scenario 3: taskProfile sets image only -> used as-is; does NOT inherit
	// workspaceProfile's CPU/memory limits.
	gotImageOnly := controller.EffectiveProfile(wsImageProfile, &v1alpha1.Profile{Image: "task/image@sha256:bbb"})
	if gotImageOnly.GetImage() != "task/image@sha256:bbb" || gotImageOnly.GetResources() != nil {
		t.Errorf("EffectiveProfile(wsImageProfile, image-only) = %+v, want image only with nil resources", gotImageOnly)
	}

	// Scenario 4: workspaceProfile uses ateTemplate.name, while taskProfile sets
	// resources only, image, or another ateTemplate.name -> taskProfile wins cleanly
	// without conflicting with workspaceProfile's ateTemplate.name.
	wsTemplateProfile := &v1alpha1.Profile{AteTemplate: &v1alpha1.ObjectRef{Name: "gpu-base"}}
	gotResourcesOverTpl := controller.EffectiveProfile(wsTemplateProfile, taskResourcesOnly)
	if gotResourcesOverTpl.GetAteTemplate().GetName() != "" ||
		gotResourcesOverTpl.GetImage() != "" ||
		gotResourcesOverTpl.GetResources().GetLimits().GetMemory() != "16Gi" {
		t.Errorf("EffectiveProfile(wsTemplateProfile, taskResourcesOnly) = %+v, want taskResourcesOnly as-is", gotResourcesOverTpl)
	}

	switchedTpl := controller.EffectiveProfile(wsTemplateProfile, &v1alpha1.Profile{AteTemplate: &v1alpha1.ObjectRef{Name: "tpu-base"}})
	if switchedTpl.GetAteTemplate().GetName() != "tpu-base" {
		t.Errorf("switched ateTemplate.name = %+v, want tpu-base", switchedTpl)
	}

	gotTplOverWsImage := controller.EffectiveProfile(wsImageProfile, &v1alpha1.Profile{AteTemplate: &v1alpha1.ObjectRef{Name: "tpu-base"}})
	if gotTplOverWsImage.GetAteTemplate().GetName() != "tpu-base" || gotTplOverWsImage.GetImage() != "" || gotTplOverWsImage.GetResources() != nil {
		t.Errorf("EffectiveProfile(wsImageProfile, tpu-base) = %+v, want tpu-base only", gotTplOverWsImage)
	}
}

func TestResolveProfileTemplate(t *testing.T) {
	p1 := &v1alpha1.Profile{
		Image: "img@sha256:abc",
		Resources: &v1alpha1.ResourcesSpec{
			Limits: &v1alpha1.ResourceList{Cpu: "1", Memory: "1Gi"},
		},
	}
	p2 := &v1alpha1.Profile{
		Image: "img@sha256:abc",
		Resources: &v1alpha1.ResourcesSpec{
			Limits: &v1alpha1.ResourceList{Cpu: "1000m", Memory: "1024Mi"},
		},
	}

	_, n1, tmpl1, err := controller.ResolveProfileTemplate("default", p1, "runner@sha256:r1")
	if err != nil {
		t.Fatalf("ResolveProfileTemplate(p1): %v", err)
	}
	_, n2, _, err := controller.ResolveProfileTemplate("default", p2, "runner@sha256:r1")
	if err != nil {
		t.Fatalf("ResolveProfileTemplate(p2): %v", err)
	}
	if n1 != n2 {
		t.Fatalf("equivalent profiles yielded different template names: %q vs %q", n1, n2)
	}
	if !strings.HasPrefix(n1, "ax-") || len(n1) != 19 {
		t.Fatalf("unexpected template name format %q", n1)
	}
	if err := apivalidation.ValidateName(n1); err != nil {
		t.Fatalf("template name %q is not a valid RFC 1123 name: %v", n1, err)
	}
	if tmpl1.GetSnapshotConfig().GetPreferredFidelity() != controller.DefaultProfileSnapshotFidelity {
		t.Errorf("fidelity = %v, want %v", tmpl1.GetSnapshotConfig().GetPreferredFidelity(), controller.DefaultProfileSnapshotFidelity)
	}

	// Omitted CPU/memory equivalent to Substrate's defaults (1000m, 512Mi) hash identically.
	_, nOmitted, _, err := controller.ResolveProfileTemplate("default", &v1alpha1.Profile{Image: "img@sha256:abc"}, "runner@sha256:r1")
	if err != nil {
		t.Fatal(err)
	}
	_, nExplicitDefault, _, err := controller.ResolveProfileTemplate("default", &v1alpha1.Profile{
		Image:     "img@sha256:abc",
		Resources: &v1alpha1.ResourcesSpec{Limits: &v1alpha1.ResourceList{Cpu: "1000m", Memory: "512Mi"}},
	}, "runner@sha256:r1")
	if err != nil {
		t.Fatal(err)
	}
	if nOmitted != nExplicitDefault {
		t.Errorf("omitted defaults (%q) != explicit defaults (%q)", nOmitted, nExplicitDefault)
	}

	// A profile without image or ateTemplate.name is rejected.
	if _, _, _, err := controller.ResolveProfileTemplate("default", nil, "runner@sha256:r1"); err == nil {
		t.Error("ResolveProfileTemplate(nil profile) succeeded, want an error")
	}
	if _, _, _, err := controller.ResolveProfileTemplate("default", &v1alpha1.Profile{Resources: &v1alpha1.ResourcesSpec{Limits: &v1alpha1.ResourceList{Cpu: "2"}}}, "runner@sha256:r1"); err == nil {
		t.Error("ResolveProfileTemplate(resources-only profile) succeeded, want an error")
	}

	// Changing runner volume image produces a distinct template hash.
	_, nRunner2, _, err := controller.ResolveProfileTemplate("default", p1, "runner@sha256:r2")
	if err != nil {
		t.Fatal(err)
	}
	if nRunner2 == n1 {
		t.Errorf("expected distinct template name when runnerImage differs, got %q", nRunner2)
	}

	// Explicit ateTemplate.name returns that name with nil template spec.
	directAtespace, directName, directTmpl, err := controller.ResolveProfileTemplate("default", &v1alpha1.Profile{AteTemplate: &v1alpha1.ObjectRef{Name: "gpu-base"}}, "runner@sha256:r1")
	if err != nil || directAtespace != "default" || directName != "gpu-base" || directTmpl != nil {
		t.Fatalf("ResolveProfileTemplate(ateTemplate.name) = (%q, %q, %v, %v), want (\"default\", \"gpu-base\", nil, nil)", directAtespace, directName, directTmpl, err)
	}

	// ateTemplate.atespace points the lookup at another atespace.
	crossAtespace, crossName, crossTmpl, err := controller.ResolveProfileTemplate("default", &v1alpha1.Profile{AteTemplate: &v1alpha1.ObjectRef{Name: "gpu-base", Atespace: "shared"}}, "runner@sha256:r1")
	if err != nil || crossAtespace != "shared" || crossName != "gpu-base" || crossTmpl != nil {
		t.Fatalf("ResolveProfileTemplate(ateTemplate.atespace) = (%q, %q, %v, %v), want (\"shared\", \"gpu-base\", nil, nil)", crossAtespace, crossName, crossTmpl, err)
	}
}

func TestProfileTemplateAtespace(t *testing.T) {
	if got := controller.ProfileTemplateAtespace("team", &v1alpha1.Profile{AteTemplate: &v1alpha1.ObjectRef{Name: "t"}}); got != "team" {
		t.Errorf("ProfileTemplateAtespace(team, no ateTemplate.atespace) = %q, want team", got)
	}
	if got := controller.ProfileTemplateAtespace("", &v1alpha1.Profile{AteTemplate: &v1alpha1.ObjectRef{Name: "t"}}); got != v1alpha1.DefaultAtespace {
		t.Errorf("ProfileTemplateAtespace(\"\", no ateTemplate.atespace) = %q, want %q", got, v1alpha1.DefaultAtespace)
	}
	if got := controller.ProfileTemplateAtespace("team", &v1alpha1.Profile{AteTemplate: &v1alpha1.ObjectRef{Name: "t", Atespace: "shared"}}); got != "shared" {
		t.Errorf("ProfileTemplateAtespace(team, ateTemplate.atespace=shared) = %q, want shared", got)
	}
}

// A runner image is mounted read-only into the Profile template and run from
// there; without one the template runs the runner baked into the image.
func TestResolveProfileTemplate_MountsRunnerVolume(t *testing.T) {
	const runner = "ghcr.io/google/ax/ax-runner-volume@sha256:r1"
	p := &v1alpha1.Profile{Image: "ghcr.io/agent@sha256:a"}

	_, _, tmpl, err := controller.ResolveProfileTemplate("default", p, runner)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tmpl.GetContainers()[0].GetCommand(), v1alpha1.RunnerMountPath+"/ax-runner"; len(got) != 1 || got[0] != want {
		t.Errorf("command = %v, want [%s]", got, want)
	}
	if !hasImageVolume(tmpl.GetVolumes(), runner) {
		t.Errorf("volumes = %v, want an image volume of %q", tmpl.GetVolumes(), runner)
	}

	_, _, tmpl, err = controller.ResolveProfileTemplate("default", p, "")
	if err != nil {
		t.Fatal(err)
	}
	if hasImageVolume(tmpl.GetVolumes(), "") {
		t.Errorf("volumes = %v, want no image volume without a runner image", tmpl.GetVolumes())
	}
	if got := tmpl.GetContainers()[0].GetCommand(); len(got) != 1 || got[0] != substrate.DefaultGuestCommand {
		t.Errorf("command = %v, want [%s]", got, substrate.DefaultGuestCommand)
	}
}

// hasImageVolume reports whether vols has an image volume of ref, or of any
// image when ref is empty.
func hasImageVolume(vols []*ateapipb.Volume, ref string) bool {
	for _, v := range vols {
		if img := v.GetImage(); img != nil && (ref == "" || img.GetReference() == ref) {
			return true
		}
	}
	return false
}

func TestResolveProfileTemplate_WorkerLabels(t *testing.T) {
	const runner = "runner@sha256:r1"
	base := &v1alpha1.Profile{Image: "img@sha256:abc"}

	// A profile with no selector keeps the template name it had before
	// workerLabels existed, so upgrading does not recreate templates.
	_, plain, plainTmpl, err := controller.ResolveProfileTemplate("default", base, runner)
	if err != nil {
		t.Fatal(err)
	}
	if want := "ax-67f389856900aad7"; plain != want {
		t.Errorf("template name without selector = %q, want unchanged %q", plain, want)
	}
	if plainTmpl.GetWorkerSelector() != nil {
		t.Errorf("WorkerLabels = %v, want nil without a selector", plainTmpl.GetWorkerSelector())
	}
	_, empty, _, err := controller.ResolveProfileTemplate("default", &v1alpha1.Profile{Image: "img@sha256:abc", WorkerLabels: map[string]string{}}, runner)
	if err != nil || empty != plain {
		t.Errorf("empty selector map: name %q, err %v; want %q", empty, err, plain)
	}

	sel := map[string]string{"ate.dev/sandboxClass": "gvisor", "zone": "a"}
	_, withSel, tmpl, err := controller.ResolveProfileTemplate("default", &v1alpha1.Profile{Image: "img@sha256:abc", WorkerLabels: sel}, runner)
	if err != nil {
		t.Fatal(err)
	}
	if withSel == plain {
		t.Errorf("selector did not change the template name %q", plain)
	}
	if got := tmpl.GetWorkerSelector().GetMatchLabels(); !maps.Equal(sel, got) {
		t.Errorf("WorkerLabels.MatchLabels mismatch = %v, want %v", got, sel)
	}
	sel["zone"] = "mutated"
	if tmpl.GetWorkerSelector().GetMatchLabels()["zone"] != "a" {
		t.Error("template shares the profile's selector map")
	}

	_, other, _, _ := controller.ResolveProfileTemplate("default", &v1alpha1.Profile{Image: "img@sha256:abc", WorkerLabels: map[string]string{"ate.dev/sandboxClass": "gvisor", "zone": "b"}}, runner)
	if other == withSel {
		t.Error("different selector values produced the same template name")
	}

	// ateTemplate is used as-is: a selector does not apply to it.
	_, name, direct, err := controller.ResolveProfileTemplate("default", &v1alpha1.Profile{AteTemplate: &v1alpha1.ObjectRef{Name: "gpu-base"}, WorkerLabels: map[string]string{"a": "b"}}, runner)
	if err != nil || name != "gpu-base" || direct != nil {
		t.Errorf("ateTemplate with selector = (%q, %v, %v), want (\"gpu-base\", nil, nil)", name, direct, err)
	}
}

func TestEffectiveProfile_WorkerLabels(t *testing.T) {
	ws := &v1alpha1.Profile{Image: "ws@sha256:a", WorkerLabels: map[string]string{"pool": "ws"}}

	// The task profile replaces the workspace profile as a whole: its
	// selector is used, and an omitted selector is not inherited.
	got := controller.EffectiveProfile(ws, &v1alpha1.Profile{Image: "t@sha256:b", WorkerLabels: map[string]string{"pool": "task"}})
	if got := got.GetWorkerLabels(); !maps.Equal(map[string]string{"pool": "task"}, got) {
		t.Errorf("task selector = %v, want %v", got, map[string]string{"pool": "task"})
	}
	got = controller.EffectiveProfile(ws, &v1alpha1.Profile{Image: "t@sha256:b"})
	if len(got.GetWorkerLabels()) != 0 {
		t.Errorf("task without selector inherited %v from the workspace", got.GetWorkerLabels())
	}

	// With no task profile, the workspace selector applies, as a copy.
	got = controller.EffectiveProfile(ws, nil)
	if got := got.GetWorkerLabels(); !maps.Equal(map[string]string{"pool": "ws"}, got) {
		t.Errorf("workspace selector = %v, want %v", got, map[string]string{"pool": "ws"})
	}
	got.WorkerLabels["pool"] = "changed"
	if ws.WorkerLabels["pool"] != "ws" {
		t.Error("EffectiveProfile mutated workspaceProfile.WorkerLabels")
	}
}

func TestWithDefaultWorkerLabels(t *testing.T) {
	const runner = "runner@sha256:r1"
	defaults := map[string]string{"workload": "ax"}
	base := &v1alpha1.Profile{Image: "img@sha256:abc"}

	// The default applies to an image profile that sets no labels, reaches the
	// template's selector, and does not mutate the input.
	withDefault := controller.WithDefaultWorkerLabels(base, defaults)
	if got := withDefault.GetWorkerLabels(); !maps.Equal(defaults, got) {
		t.Fatalf("default labels = %v, want %v", got, defaults)
	}
	if len(base.GetWorkerLabels()) != 0 {
		t.Error("WithDefaultWorkerLabels mutated its input")
	}
	_, nDefault, tmpl, err := controller.ResolveProfileTemplate("default", withDefault, runner)
	if err != nil {
		t.Fatal(err)
	}
	if got := tmpl.GetWorkerSelector().GetMatchLabels(); !maps.Equal(defaults, got) {
		t.Errorf("template selector = %v, want %v", got, defaults)
	}
	_, nPlain, _, _ := controller.ResolveProfileTemplate("default", base, runner)
	if nDefault == nPlain {
		t.Error("default labels did not change the template name")
	}

	// An empty default changes nothing, so existing template names are kept.
	for _, empty := range []map[string]string{nil, {}} {
		got := controller.WithDefaultWorkerLabels(base, empty)
		_, name, _, _ := controller.ResolveProfileTemplate("default", got, runner)
		if name != "ax-67f389856900aad7" {
			t.Errorf("empty default %v: template name %q, want ax-67f389856900aad7", empty, name)
		}
	}

	// Explicit labels win over the default, as a replacement.
	explicit := &v1alpha1.Profile{Image: "img@sha256:abc", WorkerLabels: map[string]string{"zone": "a"}}
	if got := controller.WithDefaultWorkerLabels(explicit, defaults).GetWorkerLabels(); !maps.Equal(explicit.WorkerLabels, got) {
		t.Errorf("explicit labels = %v, want %v", got, explicit.WorkerLabels)
	}

	// An ateTemplate profile is used as-is.
	ate := &v1alpha1.Profile{AteTemplate: &v1alpha1.ObjectRef{Name: "gpu-base"}}
	if got := controller.WithDefaultWorkerLabels(ate, defaults); len(got.GetWorkerLabels()) != 0 {
		t.Errorf("ateTemplate profile got default labels %v", got.GetWorkerLabels())
	}
}
