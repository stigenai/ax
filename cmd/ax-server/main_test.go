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
	"strings"
	"testing"
)

func TestResolveAuthToken(t *testing.T) {
	const token = "test-auth-token-for-unit-tests-only"
	for _, tt := range []struct {
		name    string
		value   string
		want    string
		wantErr bool
	}{
		{name: "unset is off", value: ""},
		{name: "long enough is on", value: token, want: token},
		{name: "value is trimmed", value: "  " + token + "\n", want: token},
		{name: "short is an error", value: token[:minAuthTokenLen-1], wantErr: true},
		{name: "whitespace only is an error", value: "   \n", wantErr: true},
		{name: "short after trim is an error", value: " " + token[:minAuthTokenLen-1] + " ", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveAuthToken(tt.value)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Fatalf("resolveAuthToken(%q) = %q, %v; want %q, error %v", tt.value, got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestValidateRunnerVolumeImage(t *testing.T) {
	for _, tt := range []struct {
		image   string
		wantErr bool
	}{
		{image: ""},
		{image: "ghcr.io/google/ax/ax-runner-volume@sha256:" + strings.Repeat("a", 64)},
		{image: "ghcr.io/google/ax/ax-runner-volume:latest", wantErr: true},
		{image: "ghcr.io/google/ax/ax-runner-volume", wantErr: true},
	} {
		if err := validateRunnerVolumeImage(tt.image); (err != nil) != tt.wantErr {
			t.Errorf("validateRunnerVolumeImage(%q) = %v, want error: %v", tt.image, err, tt.wantErr)
		}
	}
}

func TestValidateRouterURL(t *testing.T) {
	for _, tt := range []struct {
		routerURL string
		wantErr   bool
	}{
		{routerURL: "https://atenet-router.ate-system.svc.cluster.local:443"},
		{routerURL: "http://127.0.0.1:8080"},
		{routerURL: "", wantErr: true},
		{routerURL: "atenet-router.ate-system.svc.cluster.local:443", wantErr: true},
	} {
		if err := validateRouterURL(tt.routerURL); (err != nil) != tt.wantErr {
			t.Errorf("validateRouterURL(%q) = %v, want error: %v", tt.routerURL, err, tt.wantErr)
		}
	}
}

func TestDescribeStore(t *testing.T) {
	tests := []struct {
		name string
		conn string
		want string
	}{
		{
			name: "password in userinfo",
			conn: "postgres://ax:secret@db:5432/ax?sslmode=disable",
			want: "ax@db:5432/ax",
		},
		{
			name: "password in query",
			conn: "postgres://ax@db:5432/ax?sslmode=disable&password=secret",
			want: "ax@db:5432/ax",
		},
		{
			name: "keyword DSN",
			conn: "host=db port=5432 user=ax password=secret dbname=ax sslmode=disable",
			want: "ax@db:5432/ax",
		},
		{
			name: "unparseable",
			conn: "postgres://ax:secret@db:notaport/ax",
			want: "<unparseable>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := describeStore(tt.conn)
			if strings.Contains(got, "secret") {
				t.Fatalf("describeStore(%q) = %q, leaks the password", tt.conn, got)
			}
			if got != tt.want {
				t.Errorf("describeStore(%q) = %q, want %q", tt.conn, got, tt.want)
			}
		})
	}
}

func TestParseTaskCaps(t *testing.T) {
	for _, tt := range []struct {
		name       string
		cpu, mem   string
		wantCPU    int64
		wantMem    int64
		wantErrSub string
	}{
		{name: "empty disables both"},
		{name: "both set", cpu: "6", mem: "16Gi", wantCPU: 6000, wantMem: 16 << 30},
		{name: "millicores", cpu: "500m", wantCPU: 500},
		{name: "only memory", mem: "512Mi", wantMem: 512 << 20},
		{name: "cpu not a quantity", cpu: "six", wantErrSub: "max task cpu"},
		{name: "cpu zero", cpu: "0", wantErrSub: "max task cpu"},
		{name: "memory negative", mem: "-1Gi", wantErrSub: "max task memory"},
		{name: "memory not a quantity", mem: "lots", wantErrSub: "max task memory"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cpu, mem, err := parseTaskCaps(tt.cpu, tt.mem)
			if tt.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrSub) {
					t.Fatalf("parseTaskCaps(%q, %q) error = %v, want it to contain %q", tt.cpu, tt.mem, err, tt.wantErrSub)
				}
				return
			}
			if err != nil || cpu != tt.wantCPU || mem != tt.wantMem {
				t.Fatalf("parseTaskCaps(%q, %q) = %d, %d, %v; want %d, %d", tt.cpu, tt.mem, cpu, mem, err, tt.wantCPU, tt.wantMem)
			}
		})
	}
}
