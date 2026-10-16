# Networking

Tasks do not get a Kubernetes Service or Ingress of their own. Every request to a task goes through Agent Substrate's **atenet router**, the `atenet-router` Service in the `ate-system` namespace. The router reads a single header, `ate-target-actor`, resolves the actor to the worker it is running on, resumes it first if it was suspended, and proxies the request there. `Host` and `:authority` are left alone for your application; the header alone selects the target.

The header value is `<atespace>/<task>`. The controller always names a task's actor after the task, so `default/task123` reaches the task `task123` in the `default` atespace.

## From inside the cluster

Use the Service DNS name and add the header. This is exactly how the controller polls a task's readiness.

```bash
curl -H "ate-target-actor: default/task123" \
  http://atenet-router.ate-system.svc.cluster.local/metadata/v1alpha1/ax/task
```

## From your machine

Port-forward the router, then talk to it the same way.

```bash
kubectl -n ate-system port-forward svc/atenet-router 8001:80
curl -H "ate-target-actor: default/task123" http://localhost:8001/readyz
```

## gRPC request routing

Send the header as outgoing metadata under the lowercase key. This is what `ax ssh` does to reach the guest services.

```go
ctx = metadata.AppendToOutgoingContext(ctx, "ate-target-actor", "default/task123")
resp, err := client.SomeMethod(ctx, req)
```

## Stale routing after a worker container restarts in place

A worker pod's `ateom` container can restart in place after an OOMKill.
Kubernetes keeps the same pod UID and IP. Agent Substrate's worker sync keys
workers by pod UID. It does not crash the actors that run on that pod.

Each actor stays `ACTOR_STATE_RUNNING` on its old assignment. The restarted
`atunnel` has an empty activation map, so atenet-router sends requests for
that actor to the same pod.

The pod answers every request with HTTP 421 (Misdirected Request) and the
header `X-Ate-Assignment-Stale: true`.

`ax ssh` and `ax exec` never dial a task's cached `Status.WorkerIp` directly.
Each call resolves the worker through atenet-router. So, the route always
follows the task's current worker, not a cached address. On a 421 with
`X-Ate-Assignment-Stale: true`, `ax ssh` fetches the task again:

- If the task is no longer `Running`, `ax ssh` fails.
  - The error names the task's phase.
  - The error names the reason and message of the task's `Ready` condition.
- If the task is still `Running`, `ax ssh` retries once after 2 seconds. If
  the retry fails, `ax ssh` fails.
  - The error names 421, "stale assignment", and the actor.
  - The error tells the user to delete and re-create the task.

`ax ssh` makes at most 2 router attempts per call. If the router accepts the
connection but does not answer, `StartProcess` fails within 30 seconds
(`guest.ExecOptions.StartTimeout`). It does not hang.

## The controller's two sweeps

`ax-controller` runs two sweeps over every `Running` task: LivenessChecker and TaskMonitor. Each sweep can fail the same task, for a different reason and on a different timeline. See "Which sweep fails a task first" below.

A third sweep, DeletionSweeper, checks `Terminating` tasks and `DELETING` Substrate actors. It does not check `Running` tasks. See [docs/stuck-deletions.md](stuck-deletions.md) for its runbook.

## LivenessChecker: stale routing after a worker restarts in place

`ax-controller` probes the actor of each `Running` task through atenet-router's
`/readyz`. It probes once per `--task-liveness-interval` (default `1m`; env
`AX_TASK_LIVENESS_INTERVAL`; `0` turns the sweep off). A probe counts as a
stale miss only when both of these are true:

- The router answers 421 with `X-Ate-Assignment-Stale: true`.
- A follow-up call to Substrate's `GetActor` checks the actor's state. The
  actor still shows `RUNNING` on the same worker IP that the probe rejected.
  The task fails with reason `ActorLost`.
  - If `GetActor` shows that the actor no longer exists, the task fails with
    reason `ActorMissing`.

Any other result resets the task's miss count. So, these results never fail a
task:

- a 502
- a timeout
- a connection error
- a 421 with no stale header
- a `GetActor` result that shows a different worker or a state other than
  `RUNNING`. Substrate already moved the actor on its own.

After `--task-liveness-threshold` (default `3`) consecutive stale misses, the
controller fails the task. At the default interval, this takes about 3 minutes.

The controller sets the task's phase to `Failed`, clears `Status.WorkerIp`, and
sets condition `Ready=False` with the reason above. The message names the
actor, the worker IP and the miss count.

The write only applies when the task's phase, actor and `WorkerIp` still match
what the probe saw. So, a task that changed in between (a new reconcile, a
suspend, a delete) is never overwritten with stale information.

Pending, Suspended, Terminating and Failed tasks get no probes. If
`ATENET_ROUTER_ADDR` is unset, the controller logs "task liveness sweep
disabled" and does not start the sweep.

## TaskMonitor: a dead actor or an unresponsive runner

TaskMonitor sweeps every `Running` task once per `--sandbox-check-interval`. The default interval is `30s`. The environment variable is `AX_SANDBOX_CHECK_INTERVAL`. Set it to `0` to disable the sweep.

For each task, TaskMonitor calls Substrate's `GetActor`, with a 5s timeout. It then checks the result:

- If `GetActor` returns `NotFound`, Substrate no longer knows the actor, and the task fails immediately with reason `ActorMissing`.
- If the actor's state is `CRASHED`, the sandbox died, and the task fails immediately. The reason is `OOMKilled` if the crash message names an out-of-memory kill. Otherwise, the reason is `ActorCrashed`.
- Any other error is transient, so TaskMonitor leaves the task alone until the next sweep.
- If the actor's state is not `RUNNING` (for example `SUSPENDED`), TaskMonitor does not probe it and resets the miss count for this task.

TaskMonitor probes the runner's `/readyz` only when Substrate still reports the actor `RUNNING`, through atenet-router with the `ate-target-actor` header. Do not dial the runner directly. A direct dial has no such header, and Substrate's atunnel ingress returns 421 for every request without one. The probe needs `ATENET_ROUTER_ADDR`. If `ATENET_ROUTER_ADDR` is unset, this check stays off and the miss count resets.

A 200 response means the runner is alive. A 503 that names workspace setup as the cause also means alive. Both results reset the miss count.

Some failures never reach atenet-router at all. Examples are a refused dial and a router that is down. The check can also have a request-build error. TaskMonitor treats these failures as inconclusive. It does not change the miss count. A router outage must not count as proof that every RUNNING task's sandbox died (ax-9qu.15).

A different failure happens after a connection to atenet-router exists. The probe hits a timeout, or a connection reset, with no HTTP response. The router itself answered the connection, but the runner behind it did not answer the request. TaskMonitor counts this the same as a dead HTTP response, below (ax-9qu.18). Before this fix, every such timeout was inconclusive too. A hung runner then never grew the miss count, and the task stayed `Running` forever.

Any other result is a real HTTP response from the router. Each such response counts as a miss. The only exception is a 421 with `X-Ate-Assignment-Stale: true`. That 421 is neither alive nor a miss (see "Which sweep fails a task first" below). After 3 consecutive misses that span at least 90s, the task fails with reason `SandboxUnresponsive`.

When TaskMonitor fails a task, it sets the phase to `Failed`. It clears `Status.WorkerIp`. It sets condition `Ready=False` with the reason and a message, in the same format as the LivenessChecker message. TaskMonitor writes only if the task still has the same `status.id` and is still `Running`. A task that changed in between is never overwritten with stale information.

After each full sweep, TaskMonitor drops the miss count for each task it did not see as `Running`. LivenessChecker does the same. So a task that left `Running` and later returned to it starts counting misses from zero. This applies even when the `status.id` is the same. The task does not keep a stale count from before the gap.

## Which sweep fails a task first

LivenessChecker and TaskMonitor both probe atenet-router. They answer different questions, on different timelines:

- LivenessChecker asks whether routing is stale, because a worker restarted in place and the router still sends it requests. Its threshold is 3 consecutive stale misses over `--task-liveness-interval` (default `1m`), about 3 minutes end to end.
- TaskMonitor asks whether the actor is gone, or whether the runner inside it has stopped answering at all. Its threshold is 3 consecutive misses that span at least 90s, checked every `--sandbox-check-interval` (default `30s`).

LivenessChecker's sweep is active when `ATENET_ROUTER_ADDR` is set and `--task-liveness-interval` is above `0`. When it is active, LivenessChecker alone handles a router response of 421 with `X-Ate-Assignment-Stale: true`. TaskMonitor sees the same response during its own `/readyz` probe. TaskMonitor does not count that response as alive or as a miss. It resets its own miss count instead. It lets LivenessChecker decide the outcome. Without this deferral, TaskMonitor's shorter window would fail the task first, for the same stale-routing cause. The task would then get the wrong reason: `SandboxUnresponsive`, not `ActorLost` or `ActorMissing`.

LivenessChecker's sweep does not run when `ATENET_ROUTER_ADDR` is unset, or when `--task-liveness-interval` is `0`. In that case, no other sweep catches a task with stale routing. TaskMonitor must count the 421 as a miss, like any other dead probe. It uses its own `SandboxUnresponsive` threshold. Otherwise, the task stays `Running` forever (ax-9qu.17).

## Manual check on gtr

A person runs these by hand against the `gtr` cluster. No factory station
runs them. They restart a live worker pod.

**Check A: delete a worker pod.**

```bash
kubectl --context gtr -n ax-system delete pod <worker-pod>
```

Substrate crashes that pod's actors. It restores each actor on another worker
at the next routed request. Expect `ax --context gtr -a factory ssh <task>
-- true` to reach the new worker, or to fail with a clear error. It must
never answer an endless 421.

**Check B: restart the `ateom` container in place. This is the real
test for the bug that this document describes.**

```bash
kubectl --context gtr -n ax-system exec <worker-pod> -c ateom -- kill 1
# or wait for a real OOMKill
ax --context gtr -a factory ssh <task> -- true
```

Expect the first `ssh` to fail with a clear error. The error names "421 stale
assignment". It makes at most 2 attempts, and it does not hang.

Within about 3 minutes, `ax --context gtr -a factory get task <task>` must show
phase `Failed` and condition `Ready` reason `ActorLost`. A further `ax ssh <task>`
must report that phase.

This check also confirms that the cluster's ingress passes the
`X-Ate-Assignment-Stale` header through unchanged.
