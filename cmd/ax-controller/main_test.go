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

	"k8s.io/apimachinery/pkg/api/resource"
)

// TestDefaultTaskMemoryFlagDefault pins --default-task-memory's default at
// 8Gi (ax-9qu raised it from 4Gi: see defaultTaskMemoryDefault's comment).
func TestDefaultTaskMemoryFlagDefault(t *testing.T) {
	if defaultTaskMemoryDefault != "8Gi" {
		t.Fatalf("defaultTaskMemoryDefault = %q, want 8Gi", defaultTaskMemoryDefault)
	}
	got, err := resolveDefaultTaskMemory(defaultTaskMemoryDefault)
	if err != nil {
		t.Fatalf("resolveDefaultTaskMemory(%q) returned an unexpected error: %v", defaultTaskMemoryDefault, err)
	}
	if got != "8Gi" {
		t.Fatalf("resolveDefaultTaskMemory(%q) = %q, want 8Gi", defaultTaskMemoryDefault, got)
	}
}

func TestResolveDefaultTaskMemory(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    string
		wantErr bool
	}{
		{name: "the 8Gi default parses and passes through", value: "8Gi", want: "8Gi"},
		{name: "an explicit override parses and passes through", value: "6Gi", want: "6Gi"},
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

func TestResolveMaxTaskMemory(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    string
		wantErr bool
	}{
		{name: "empty disables the cap", value: "", want: "0"},
		{name: "0 disables the cap", value: "0", want: "0"},
		{name: "a quantity parses and passes through", value: "16Gi", want: "16Gi"},
		{name: "a value that does not parse as a quantity is an error", value: "not-a-quantity", wantErr: true},
		{name: "a negative quantity is an error", value: "-1Gi", wantErr: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveMaxTaskMemory(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveMaxTaskMemory(%q) = %v, nil; want an error", tt.value, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveMaxTaskMemory(%q) returned an unexpected error: %v", tt.value, err)
			}
			want := resource.MustParse(tt.want)
			if got.Cmp(want) != 0 {
				t.Fatalf("resolveMaxTaskMemory(%q) = %v, want %v", tt.value, got.String(), want.String())
			}
		})
	}
}

func TestCheckDefaultWithinMax(t *testing.T) {
	cases := []struct {
		name              string
		defaultTaskMemory string
		max               string
		wantErr           bool
	}{
		{name: "no default is always fine", defaultTaskMemory: "", max: "16Gi"},
		{name: "no max is always fine", defaultTaskMemory: "32Gi", max: ""},
		{name: "default equal to max passes", defaultTaskMemory: "16Gi", max: "16Gi"},
		{name: "default under max passes", defaultTaskMemory: "4Gi", max: "16Gi"},
		{name: "default over max is an error", defaultTaskMemory: "32Gi", max: "16Gi", wantErr: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var max resource.Quantity
			if tt.max != "" {
				max = resource.MustParse(tt.max)
			}
			err := checkDefaultWithinMax(tt.defaultTaskMemory, max)
			if tt.wantErr && err == nil {
				t.Fatalf("checkDefaultWithinMax(%q, %v) = nil; want an error", tt.defaultTaskMemory, max)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("checkDefaultWithinMax(%q, %v) returned an unexpected error: %v", tt.defaultTaskMemory, max, err)
			}
		})
	}
}

// TestSandboxCheckIntervalFlagDefault pins --sandbox-check-interval's
// default at 30s.
func TestSandboxCheckIntervalFlagDefault(t *testing.T) {
	if sandboxCheckIntervalDefault != "30s" {
		t.Fatalf("sandboxCheckIntervalDefault = %q, want 30s", sandboxCheckIntervalDefault)
	}
	got, err := resolveSandboxCheckInterval(sandboxCheckIntervalDefault)
	if err != nil {
		t.Fatalf("resolveSandboxCheckInterval(%q) returned an unexpected error: %v", sandboxCheckIntervalDefault, err)
	}
	if got != 30*time.Second {
		t.Fatalf("resolveSandboxCheckInterval(%q) = %v, want 30s", sandboxCheckIntervalDefault, got)
	}
}

func TestResolveSandboxCheckInterval(t *testing.T) {
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
			got, err := resolveSandboxCheckInterval(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveSandboxCheckInterval(%q) = %v, nil; want an error", tt.value, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveSandboxCheckInterval(%q) returned an unexpected error: %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("resolveSandboxCheckInterval(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestResolveTaskLivenessInterval(t *testing.T) {
	cases := []struct {
		name      string
		flagValue time.Duration
		envValue  string
		want      time.Duration
		wantErr   bool
	}{
		{name: "no env keeps the flag default", flagValue: time.Minute, envValue: "", want: time.Minute},
		{name: "an env override parses and replaces the flag value", flagValue: time.Minute, envValue: "30s", want: 30 * time.Second},
		{name: "env 0 disables the sweep", flagValue: time.Minute, envValue: "0", want: 0},
		{name: "an env value that does not parse as a duration is an error", flagValue: time.Minute, envValue: "not-a-duration", wantErr: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveTaskLivenessInterval(tt.flagValue, tt.envValue)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveTaskLivenessInterval(%v, %q) = %v, nil; want an error", tt.flagValue, tt.envValue, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveTaskLivenessInterval(%v, %q) returned an unexpected error: %v", tt.flagValue, tt.envValue, err)
			}
			if got != tt.want {
				t.Fatalf("resolveTaskLivenessInterval(%v, %q) = %v, want %v", tt.flagValue, tt.envValue, got, tt.want)
			}
		})
	}
}

func TestTaskLivenessSweepEnabled(t *testing.T) {
	cases := []struct {
		name       string
		routerAddr string
		interval   time.Duration
		want       bool
	}{
		{name: "a router address and a positive interval enable the sweep", routerAddr: "atenet-router:80", interval: time.Minute, want: true},
		{name: "no router address disables the sweep", routerAddr: "", interval: time.Minute, want: false},
		{name: "a zero interval disables the sweep", routerAddr: "atenet-router:80", interval: 0, want: false},
		{name: "neither set disables the sweep", routerAddr: "", interval: 0, want: false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := taskLivenessSweepEnabled(tt.routerAddr, tt.interval); got != tt.want {
				t.Fatalf("taskLivenessSweepEnabled(%q, %v) = %v, want %v", tt.routerAddr, tt.interval, got, tt.want)
			}
		})
	}
}
