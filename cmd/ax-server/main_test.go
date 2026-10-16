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

	"k8s.io/apimachinery/pkg/api/resource"
)

func TestResolveMaxQuantity(t *testing.T) {
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
		{name: "zero spelled Gi is an error", value: "0Gi", wantErr: true},
		{name: "a cpu quantity parses and passes through", value: "6", want: "6"},
		{name: "a cpu quantity in millicores parses and passes through", value: "6000m", want: "6000m"},
		{name: "a negative cpu quantity is an error", value: "-1", wantErr: true},
		{name: "a cpu value abc does not parse as a quantity", value: "abc", wantErr: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveMaxQuantity(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveMaxQuantity(%q) = %v, nil; want an error", tt.value, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveMaxQuantity(%q) returned an unexpected error: %v", tt.value, err)
			}
			want := resource.MustParse(tt.want)
			if got.Cmp(want) != 0 {
				t.Fatalf("resolveMaxQuantity(%q) = %v, want %v", tt.value, got.String(), want.String())
			}
		})
	}
}
