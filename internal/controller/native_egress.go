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

package controller

import (
	"fmt"

	"github.com/google/ax/internal/substrate"
)

// ConfigureNativeTLS opts the dedicated private controller into policy-before-
// resume. Shared namespaces, endpoints and insecure transports are refused.
func (r *TaskReconciler) ConfigureNativeTLS(destination, namespace, endpoint, authority string, insecure bool) error {
	if namespace != "fabriek-native" || endpoint != "api.fabriek-native.svc.cluster.local:443" || authority != "api.fabriek-native.svc" || insecure {
		return fmt.Errorf("native TLS policy requires the private verified Substrate API")
	}
	policy, err := substrate.NativeTLSPolicy(destination)
	if err != nil {
		return err
	}
	r.nativeTLSPolicy = policy
	return nil
}
