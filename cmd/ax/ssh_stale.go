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
	"time"

	"github.com/google/ax/internal/guest"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// sshRetryDelay is the wait before ax ssh retries a command that atenet-router
// rejected as a stale assignment.
const sshRetryDelay = 2 * time.Second

// staleAssignment421 is the text grpc-go puts in the error for an HTTP 421
// response. atenet-router answers 421 for an actor that its worker no longer
// holds, for example after the worker restarted. The reply has no body, and
// grpc-go keeps no header of it, so the error text is all there is to match.
const staleAssignment421 = "421 (Misdirected Request)"

// isStaleAssignment reports whether err is the error for a 421 reply to the
// call that starts the process. No process ran and no input was read, so
// the same command can run again.
func isStaleAssignment(err error) bool {
	return errors.Is(err, guest.ErrStartProcess) && strings.Contains(err.Error(), staleAssignment421)
}

// execRetryStale calls exec. If atenet-router answers 421, it fetches the task
// again. A task that is not Running is reported with its Ready reason and
// message, because the controller may already have failed it. A Running task
// gets one more exec after delay.
func execRetryStale(exec func() (int, error), refetch func() (*v1alpha1.Task, error), taskName string, delay time.Duration) (int, error) {
	code, err := exec()
	if !isStaleAssignment(err) {
		return code, err
	}
	task, gerr := refetch()
	if gerr != nil {
		return 1, fmt.Errorf("task %q has a stale assignment (HTTP 421), and fetching it again failed: %w", taskName, gerr)
	}
	if task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_RUNNING {
		return 1, fmt.Errorf("task %q is %s, not Running%s", taskName, v1alpha1.FormatTaskState(task), readyDetail(task))
	}
	time.Sleep(delay)
	code, err = exec()
	if isStaleAssignment(err) {
		return code, fmt.Errorf("task %q has a stale assignment (HTTP 421) after one retry; delete and re-create the task: %w", taskName, err)
	}
	return code, err
}

// readyDetail returns " (Ready reason: message)" from the task's Ready
// condition, or "" when the task has none.
func readyDetail(task *v1alpha1.Task) string {
	for _, c := range task.GetStatus().GetConditions() {
		if c.GetType() == v1alpha1.ConditionReady {
			return fmt.Sprintf(" (Ready %s: %s)", c.GetReason(), c.GetMessage())
		}
	}
	return ""
}
