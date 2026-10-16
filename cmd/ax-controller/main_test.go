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

func TestResolveDefaultTaskMemory(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    string
		wantErr bool
	}{
		{name: "the 4Gi default parses and passes through", value: "4Gi", want: "4Gi"},
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
