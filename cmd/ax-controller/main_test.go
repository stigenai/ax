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

import "testing"

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
