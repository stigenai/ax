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

Every line has the same fields: `stuck`, `stuck_tasks`, `stuck_actors`,
`protected`, `requeued`, `requeue_failed`, `task_list_error`,
`actor_list_error`, and `min_age`. `task_list_error` and `actor_list_error`
are on every line, not only on a line where `stuck` is greater than 0.

Read the fields in this order:

- Check `task_list_error` and `actor_list_error` first.
  - If `task_list_error=true` or `actor_list_error=true`, that side's
    list failed this tick.
  - The side that did not fail still lists tasks or actors as normal.
    It keeps a valid count. `stuck` stays the sum of both sides.
  - A line with `stuck=0` and `task_list_error=true` or
    `actor_list_error=true` is NOT an all-clear. The failed side can
    hide a real count.
  - A line with `stuck>0` and `task_list_error=true` or
    `actor_list_error=true` needs action now. Go to step 3 and check
    whether Substrate or the task store is reachable.
  - If `stuck=0` and an error field stays `true` for many ticks, go to
    step 3 and check whether Substrate or the task store is reachable.
- `stuck=0` and `task_list_error=false` and `actor_list_error=false`:
  nothing is stuck. This is the only all-clear case.
  - If `AxStuckDeletions` still fires, check the alert's query and time
    range in Grafana.
- `stuck>0`: `requeued` counts tasks for which the sweep already sent a
  delete.
  - Wait one more tick (the sweep's interval). Then check again before
    you act further.
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
delete again for a stuck `Terminating` task that the retry still misses. For
a stuck `DELETING` actor, the sweep only counts and logs it (see "What the
sweep does not do" below); step 2 and step 3 cover what to do about one by
hand.

Follow these steps if one of these conditions is true:

- The sweep's own log shows `requeued` counts that go up. The stuck count does
  not drop after a few ticks.
- `AxDeleteSweepMissing` fires.

1. Restart `ax-controller`. Its startup path calls the
   `Worker.requeueStrandedDeletions` function (`internal/controller/worker.go`,
   called from `Worker.Run`). This runs the same requeue sweep once at boot,
   before the ticker starts.
   - `requeueStrandedDeletions` sends the same empty-precondition requeue the
     ticker sweep does. A restart does not publish a different delete.
   - A restart drops every in-memory retry chain (`scheduleDeleteRetry`) and
     starts a fresh Redis stream consumer. Since ax-bsl, `XAUTOCLAIM`
     reclaim (see "Pending stream entries" below) also redelivers a stuck
     delivery that no worker has acknowledged, so a restart is no longer
     the only way to recover one.
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

## 5. Pending stream entries

`ax-controller` reads the task event stream (`ax:stream:tasks`) through the
Redis consumer group `ax-controllers`. A pod reads an event and dies before it
acks the event. The entry stays pending under the consumer name of that pod,
which is now dead. Since ax-bsl, another consumer reclaims the entry with
`XAUTOCLAIM`. This happens after the entry is idle for more than
`--stream-claim-min-idle` / `AX_STREAM_CLAIM_MIN_IDLE` (default 5 minutes).

Check for stuck pending entries:

```bash
kubectl --context gtr -n ax-system exec -it deploy/ax-redis -- \
  redis-cli XPENDING ax:stream:tasks ax-controllers - + 20
```

Each row lists `id`, `consumer`, `idle` (ms) and `deliveries`. After a rollout,
wait for `--stream-claim-min-idle` (default 5 minutes) plus a little more time.
Then read the result.

`Subscription.Close` never calls `XGROUP DELCONSUMER`, so a consumer stays
listed forever, long after its pod is gone. Every entry's consumer name always
matches one in `XINFO CONSUMERS`, whether that pod is live or dead. So this
check cannot flag a dead owner.

Do not use `XINFO CONSUMERS ax:stream:tasks ax-controllers` to tell if an owner
is live or dead. Compare the consumer name with a live pod instead:

```bash
kubectl --context gtr -n ax-system get pods -l app.kubernetes.io/name=ax-controller
```

A consumer name has the form `<pod-name>-<digits>`. `NewWorker` in
`internal/controller/worker.go` makes this form when
`--redis-consumer`/`AX_REDIS_CONSUMER` is unset. It appends
`-<a number up to 4 digits>` to the pod's hostname.

Remove the trailing `-<digits>` from the consumer name. Then compare what
remains with the live pod names above:

- If the name without the suffix matches a live pod, that pod still owns the entry. The entry is not stuck, unless `idle` also exceeds `--stream-claim-min-idle` by a wide margin. A wide margin shows a wedged worker, not a dead one.

- If the name without the suffix matches no live pod, and `idle` is past `--stream-claim-min-idle`, reclaim must have already run. Check these causes:
  - Reclaim is off. `AX_STREAM_CLAIM_MIN_IDLE` is set to a negative value.
  - `ax-controller` has not run long enough yet to scan the entry.

  To find which cause applies, read `claimMinIdle` in the pod's `starting ax-controller` startup log line.

- A `level=error` line says `dropping task event after too many deliveries`. This means that a poison event reached the retry cap (`MaxDeliveries`, default 5). The controller drops the event and does not reclaim it again. The id, atespace, name and action fields name the event.

## What the sweep does not do

- The sweep never calls `DeleteActor` on a `DELETING` actor.
- The sweep only lists actors (`internal/substrate/client.go` `Client.ListActors`).
- The sweep has no actor UID or ownership check. A person makes that call.
- The sweep itself does not reclaim a lost Redis stream event from an
  `ax-controller` rolling update. `XAUTOCLAIM` reclaim (ax-bsl) does that.
  See "Pending stream entries" above.
