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

package main

import (
	"testing"
	"time"
)

// TestDefaultTaskMemoryFlagDefault pins --default-task-memory's default at
// 7680Mi (ax-9qu raised it from 4Gi: see defaultTaskMemoryDefault's comment).
func TestDefaultTaskMemoryFlagDefault(t *testing.T) {
	if defaultTaskMemoryDefault != "7680Mi" {
		t.Fatalf("defaultTaskMemoryDefault = %q, want 7680Mi", defaultTaskMemoryDefault)
	}
	got, err := resolveDefaultTaskMemory(defaultTaskMemoryDefault)
	if err != nil {
		t.Fatalf("resolveDefaultTaskMemory(%q) returned an unexpected error: %v", defaultTaskMemoryDefault, err)
	}
	if got != "7680Mi" {
		t.Fatalf("resolveDefaultTaskMemory(%q) = %q, want 7680Mi", defaultTaskMemoryDefault, got)
	}
}

func TestResolveDefaultTaskMemory(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    string
		wantErr bool
	}{
		{name: "the 7680Mi default parses and passes through", value: "7680Mi", want: "7680Mi"},
		{name: "an explicit override parses and passes through", value: "8Gi", want: "8Gi"},
		{name: "0 disables the default", value: "0", want: ""},
		{name: "empty stays disabled", value: "", want: ""},
		{name: "a value that does not parse as a quantity is an error", value: "not-a-quantity", wantErr: true},
		{name: "a negative quantity is an error", value: "-1Gi", wantErr: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveDefaultTaskMemory(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveDefaultTaskMemory(%q) = %q, nil; want an error", tt.value, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveDefaultTaskMemory(%q) returned an unexpected error: %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("resolveDefaultTaskMemory(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

// TestLivenessIntervalFlagDefault pins --liveness-interval's default at 30s.
func TestLivenessIntervalFlagDefault(t *testing.T) {
	if livenessIntervalDefault != "30s" {
		t.Fatalf("livenessIntervalDefault = %q, want 30s", livenessIntervalDefault)
	}
	got, err := resolveLivenessInterval(livenessIntervalDefault)
	if err != nil {
		t.Fatalf("resolveLivenessInterval(%q) returned an unexpected error: %v", livenessIntervalDefault, err)
	}
	if got != 30*time.Second {
		t.Fatalf("resolveLivenessInterval(%q) = %v, want 30s", livenessIntervalDefault, got)
	}
}

func TestResolveLivenessInterval(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{name: "the 30s default parses", value: "30s", want: 30 * time.Second},
		{name: "an explicit override parses", value: "1m", want: time.Minute},
		{name: "0 disables the monitor", value: "0", want: 0},
		{name: "a value that does not parse as a duration is an error", value: "not-a-duration", wantErr: true},
		{name: "empty is an error", value: "", wantErr: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveLivenessInterval(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveLivenessInterval(%q) = %v, nil; want an error", tt.value, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveLivenessInterval(%q) returned an unexpected error: %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("resolveLivenessInterval(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}
