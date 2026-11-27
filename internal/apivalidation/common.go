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

package apivalidation

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/google/ax/internal/resources"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// Resource names.
//
// A resource's name and atespace become Substrate resource names (the task's
// actor and ActorTemplate, and the atespace itself), which must be lowercase
// RFC 1123 labels. Checking them at apply time reports a bad name before
// Agent Substrate refuses it.

// MaxNameLength is the longest name or atespace a resource may have.
const MaxNameLength = 63

var nameRegexp = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// ValidateName reports whether s can be used as a resource name or atespace: a
// lowercase RFC 1123 label of at most MaxNameLength characters.
func ValidateName(s string) error {
	if s == "" {
		return errors.New("must not be empty")
	}
	if len(s) > MaxNameLength || !nameRegexp.MatchString(s) {
		return fmt.Errorf("must be a lowercase RFC 1123 label: at most %d lowercase alphanumeric characters or '-', starting and ending with an alphanumeric character", MaxNameLength)
	}
	return nil
}

// ValidateObjectMeta checks a resource's name and, when set, its atespace. An
// empty atespace is fine here; the API server defaults it.
func ValidateObjectMeta(meta *v1alpha1.ObjectMeta) error {
	if err := ValidateName(meta.GetName()); err != nil {
		return fmt.Errorf("metadata.name: invalid value %q: %w", meta.GetName(), err)
	}
	if atespace := meta.GetAtespace(); atespace != "" {
		if err := ValidateName(atespace); err != nil {
			return fmt.Errorf("metadata.atespace: invalid value %q: %w", atespace, err)
		}
	}
	return nil
}

// Resource requirements.
//
// spec.resources.limits is copied onto the Substrate ActorTemplate, which
// enforces the same rules as below: cpu and memory only, each quantity greater
// than zero, and a cpu limit below 1000 cores. Checking them here means a bad
// value fails at apply time instead of when the template is created.

// ValidateResources reports the first problem with a profile's resource
// requirements. A nil or empty value is valid: omitted cpu and memory limits
// default to 1 CPU (1000m) and 512Mi when the ActorTemplate is built.
func ValidateResources(reqs *v1alpha1.ResourcesSpec) error {
	return validateResources("spec.profile.resources", reqs)
}

func validateResources(field string, reqs *v1alpha1.ResourcesSpec) error {
	limits := reqs.GetLimits()
	if cpu := limits.GetCpu(); cpu != "" {
		if _, err := resources.ParseCPUMillicores(cpu); err != nil {
			return fmt.Errorf("%s.limits.cpu: %w", field, err)
		}
	}
	if memory := limits.GetMemory(); memory != "" {
		if _, err := resources.ParseMemoryBytes(memory); err != nil {
			return fmt.Errorf("%s.limits.memory: %w", field, err)
		}
	}
	return nil
}

// ValidateResourceCaps reports whether a profile's cpu and memory limits fit
// within the largest task a worker can hold. A zero cap disables its check,
// and an omitted limit passes, because the default limits are small. A limit
// that does not parse also passes: ValidateResources reports it.
func ValidateResourceCaps(reqs *v1alpha1.ResourcesSpec, maxCPUMillicores, maxMemoryBytes int64) error {
	limits := reqs.GetLimits()
	if cpu := limits.GetCpu(); cpu != "" && maxCPUMillicores > 0 {
		if v, err := resources.ParseCPUMillicores(cpu); err == nil && v > maxCPUMillicores {
			return fmt.Errorf("spec.profile.resources.limits.cpu: %q exceeds the maximum of %dm", cpu, maxCPUMillicores)
		}
	}
	if memory := limits.GetMemory(); memory != "" && maxMemoryBytes > 0 {
		if v, err := resources.ParseMemoryBytes(memory); err == nil && v > maxMemoryBytes {
			return fmt.Errorf("spec.profile.resources.limits.memory: %q exceeds the maximum of %d bytes", memory, maxMemoryBytes)
		}
	}
	return nil
}

// validateProfile reports the first problem with a profile. A nil or empty
// profile is valid and means the profile is omitted. A profile that sets any
// field must name the container image to run, through image or
// ateTemplate.name, because there is no server-wide default image.
func validateProfile(field string, p *v1alpha1.Profile) error {
	if p == nil {
		return nil
	}
	if ate := p.GetAteTemplate(); ate != nil {
		if ate.GetName() == "" {
			return fmt.Errorf("%s.ateTemplate.name: must be set", field)
		}
		if err := ValidateName(ate.GetName()); err != nil {
			return fmt.Errorf("%s.ateTemplate.name: invalid value %q: %w", field, ate.GetName(), err)
		}
		if atespace := ate.GetAtespace(); atespace != "" {
			if err := ValidateName(atespace); err != nil {
				return fmt.Errorf("%s.ateTemplate.atespace: invalid value %q: %w", field, atespace, err)
			}
		}
		if p.GetImage() != "" || p.GetResources() != nil {
			return fmt.Errorf("%s.ateTemplate: cannot be combined with image or resources", field)
		}
		if len(p.GetWorkerLabels()) > 0 {
			return fmt.Errorf("%s.ateTemplate: cannot be combined with workerLabels", field)
		}
		return nil
	}
	if err := validateWorkerLabels(field+".workerLabels", p.GetWorkerLabels()); err != nil {
		return err
	}
	if err := validateResources(field+".resources", p.GetResources()); err != nil {
		return err
	}
	if p.GetImage() == "" && (p.GetResources() != nil || len(p.GetWorkerLabels()) > 0) {
		return fmt.Errorf("%s: image or ateTemplate.name is required", field)
	}
	return nil
}

// Substrate matches a worker selector against WorkerPool labels and accepts at
// most maxWorkerLabelsLabels of them, each a valid Kubernetes label.
const maxWorkerLabelsLabels = 10

var (
	labelNameRegexp  = regexp.MustCompile(`^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`)
	labelValueRegexp = regexp.MustCompile(`^([A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?)?$`)
	dnsSubdomain     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
)

func validateWorkerLabels(field string, labels map[string]string) error {
	if len(labels) > maxWorkerLabelsLabels {
		return fmt.Errorf("%s: at most %d labels are allowed", field, maxWorkerLabelsLabels)
	}
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		if !validLabelKey(k) {
			return fmt.Errorf("%s: invalid label key %q: must be a Kubernetes label key", field, k)
		}
		if v := labels[k]; len(v) > 63 || !labelValueRegexp.MatchString(v) {
			return fmt.Errorf("%s[%q]: invalid label value %q: must be a Kubernetes label value", field, k, v)
		}
	}
	return nil
}

// ParseWorkerLabels parses a comma-separated key=value list, such as
// "workload=ax,zone=a", into worker labels and validates them like
// spec.profile.workerLabels. An empty string yields nil.
func ParseWorkerLabels(s string) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	labels := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok {
			return nil, fmt.Errorf("invalid worker label %q: want key=value", pair)
		}
		if _, dup := labels[k]; dup {
			return nil, fmt.Errorf("duplicate worker label key %q", k)
		}
		labels[k] = v
	}
	if err := validateWorkerLabels("worker labels", labels); err != nil {
		return nil, err
	}
	return labels, nil
}

// validLabelKey reports whether k is a Kubernetes label key: a name of at most
// 63 characters, optionally preceded by a DNS subdomain prefix of at most 253
// characters and a slash.
func validLabelKey(k string) bool {
	prefix, name, hasPrefix := strings.Cut(k, "/")
	if !hasPrefix {
		prefix, name = "", k
	}
	if len(name) > 63 || !labelNameRegexp.MatchString(name) {
		return false
	}
	return !hasPrefix || (len(prefix) <= 253 && dnsSubdomain.MatchString(prefix))
}

// ProfileNamesImage reports whether p names the container image to run, either
// directly or through an existing ActorTemplate. After validateProfile, it is
// true exactly when p is set.
func ProfileNamesImage(p *v1alpha1.Profile) bool {
	return p.GetImage() != "" || p.GetAteTemplate().GetName() != ""
}

func validateGatewayRef(field string, ref *v1alpha1.GatewayRef) error {
	if ref == nil {
		return nil
	}
	if err := ValidateName(ref.GetName()); err != nil {
		return fmt.Errorf("%s.name: invalid value %q: %w", field, ref.GetName(), err)
	}
	return nil
}
