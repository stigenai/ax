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

## The controller's liveness sweep

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
