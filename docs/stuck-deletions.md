# Runbook: stuck deletions

This runbook covers two related alerts:

- `AxStuckDeletions`: The periodic sweep (`DeletionSweeper` in `ax-controller`, see
  `internal/controller/delete_sweep.go`) finds a `Terminating` task or an
  `ACTOR_STATE_DELETING` Substrate actor older than 15 minutes.
- `AxDeleteSweepMissing`: No line from the sweep appears in the log. The ticker can
  stop, or `ax-controller` can be down.

Background: on 2026-10-15 18:00Z to 2026-10-16 02:32Z, 20 Substrate actors
stayed in `ACTOR_STATE_DELETING` for about 8.5 hours.

No alert found it. A person found the actors by chance with
`kubectl ate get actors -A`.

Each stuck actor books 4Gi (now 8Gi) of its worker's memory. A leak like this
can fill the worker pool. New tasks can then stay `Pending` with no end.

## 1. Check the sweep's own signal

The sweep logs one line per tick. The default tick is 5 minutes (flag
`--delete-sweep-interval`, env `AX_DELETE_SWEEP_INTERVAL`).

Open Grafana Explore. Pick the `loki` datasource. Switch to Logs mode. Run
this query to read the raw lines:

```
{namespace="ax-system",container="controller"} |= "delete sweep: stuck deletions"
```

Read the `stuck` field in each line:

- `stuck=0` on every line: nothing is stuck.
  - If `AxStuckDeletions` still fires, check the alert's query and time
    range in Grafana.
- `stuck>0`: the line also has these fields: `stuck_tasks`, `stuck_actors`,
  `protected`, `requeued`, `requeue_failed`, `actor_list_error`, and
  `min_age`.
  - `requeued` counts tasks for which the sweep already sent a delete.
  - Wait one more tick (the sweep's interval). Then check again before you
    act further.
- No lines appear in the last 30 minutes: `AxDeleteSweepMissing` fires in
  this case. Go to step 4.

The `AxStuckDeletions` alert rule reads the `stuck` value as a number over
time. To see the same number, switch Grafana Explore to Metrics mode and
run:

```
max_over_time({namespace="ax-system",container="controller"} |= "delete sweep: stuck deletions" | logfmt | unwrap stuck [15m])
```

This form needs the `[15m]` range on the end. Without a range, Loki
rejects `unwrap` with a syntax error.

## 2. Look at the actors and workers directly

```bash
kubectl ate --context gtr get actors -A
kubectl ate --context gtr get workers
```

`get actors -A` lists every actor across all atespaces. Each row shows the
`STATE` and `WORKER POD` of an actor. Look for `ACTOR_STATE_DELETING` entries
and note their `AGE`.

`get workers` shows the booked `MEMORY` of each worker, for example
`15Gi/16Gi`.

- The cap after the slash is `16Gi` per worker (6 workers), set by
  `limits.memory` in `deploy/stigenai/workerpool.yaml`.
- Each task books `AX_DEFAULT_TASK_MEMORY` against that cap. The default is
  `8Gi` as of ax-9qu.1/.4.
- Read both values live. The deploy manifest and the code default can
  disagree.

A worker near its cap, with old `DELETING` actors on it, shows a leak. The
leak fills the pool.

## 3. Find why a delete is stuck

Substrate's own log gives the reason that a `DeleteActor` call failed to
release a worker. Query the `loki` datasource:

```
{namespace="ate-system",container="ate-api-server"} |= "skipping releasing worker"
```

Each line names the stuck actor (`actor.name`) and the underlying error
(`atletTerminatedErr` or `volumesDetachedErr`). For example, a `runsc delete`
fails right after a worker OOM kill. The ax-8qz postmortem used this same
signal.

## 4. Fix

The PR #9 retry in `ax-controller`
(<https://github.com/stigenai/ax/pull/9>) already retries a failed
`DeleteActor` with backoff.

The periodic `DeletionSweeper` (this runbook's feature, ax-706) publishes the
delete again for any `Terminating` task or `DELETING` actor. It does this for
each task or actor that the retry still misses.

Follow these steps if one of these conditions is true:

- The sweep's own log shows `requeued` counts that go up. The stuck count does
  not drop after a few ticks.
- `AxDeleteSweepMissing` fires.

1. Restart `ax-controller`. Its startup path
   (`Worker.requeueStrandedDeletions`, `internal/controller/worker.go:115-117`)
   runs the same requeue sweep once at boot, before the ticker starts. A restart
   alone often clears a backlog that the ticker cannot.
2. If actors are still `DELETING` after a restart, check whether the
   underlying Substrate actor is dead.
   - Substrate never marks a crashed sandbox on its own. The actor stays
     `RUNNING` or `DELETING` until something retries the delete.
   - Compare `kubectl ate get actors -A` with the pod that the `WORKER POD`
     column names.
   - A missing or restarted pod with a stale actor assignment is the known
     `ssh` 421 failure mode (see ax-lox). The sweep does not fix this failure
     mode.
3. The sweep counts a protected task (one with `spec.supervisorProfile` set) as
   `stuck`. It logs the task at `protected`, but it never requeues the task. An
   empty-precondition requeue fails `TaskDeletePreconditions.Check` for a
   supervisor task. Re-run `ax delete` with that task's identity by hand.

## What the sweep does not do

- The sweep never calls `DeleteActor` on a `DELETING` actor.
- The sweep only lists actors (`internal/substrate/client.go` `Client.ListActors`).
- The sweep has no actor UID or ownership check. A person makes that call.
- The sweep does not fix lost Redis stream events from an `ax-controller`
  rolling update (tracked separately as ax-bsl).
