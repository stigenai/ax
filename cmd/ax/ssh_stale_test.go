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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/ax/internal/guest"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

var errStale = fmt.Errorf("%w: rpc error: code = Unknown desc = unexpected HTTP status code received from server: 421 (Misdirected Request)", guest.ErrStartProcess)

func taskInState(state v1alpha1.TaskState) *v1alpha1.Task {
	return &v1alpha1.Task{Status: &v1alpha1.TaskStatus{
		State:      state,
		Conditions: []*v1alpha1.Condition{{Type: v1alpha1.ConditionReady, Reason: "ActorLost", Message: "worker restarted"}},
	}}
}

func TestIsStaleAssignment(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"421 at start", errStale, true},
		{"other error at start", fmt.Errorf("%w: connection refused", guest.ErrStartProcess), false},
		{"421 text after the process started", errors.New("receiving output stream: 421 (Misdirected Request)"), false},
	} {
		if got := isStaleAssignment(tc.err); got != tc.want {
			t.Errorf("%s: isStaleAssignment() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestExecRetryStale(t *testing.T) {
	running := func() (*v1alpha1.Task, error) { return taskInState(v1alpha1.TaskState_TASK_STATE_RUNNING), nil }
	otherErr := errors.New("boom")

	for _, tc := range []struct {
		name      string
		results   []error // result of each exec call
		refetch   func() (*v1alpha1.Task, error)
		wantCalls int
		wantErr   string // substring, empty for success
	}{
		{"success", []error{nil}, running, 1, ""},
		{"other error is not retried", []error{otherErr}, running, 1, "boom"},
		{"stale then success", []error{errStale, nil}, running, 2, ""},
		{"stale twice", []error{errStale, errStale}, running, 2, "after one retry; delete and re-create the task"},
		{"stale then other error", []error{errStale, otherErr}, running, 2, "boom"},
		{
			"task no longer running", []error{errStale},
			func() (*v1alpha1.Task, error) { return taskInState(v1alpha1.TaskState_TASK_STATE_CRASHED), nil },
			1, `is Crashed, not Running (Ready ActorLost: worker restarted)`,
		},
		{
			"refetch fails", []error{errStale},
			func() (*v1alpha1.Task, error) { return nil, errors.New("server down") },
			1, "fetching it again failed: server down",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			exec := func() (int, error) {
				err := tc.results[calls]
				calls++
				return 0, err
			}
			_, err := execRetryStale(exec, tc.refetch, "t1", 0)
			if calls != tc.wantCalls {
				t.Errorf("exec called %d times, want %d", calls, tc.wantCalls)
			}
			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("error = %v, want nil", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}
