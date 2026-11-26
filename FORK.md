# Fork Carry

This file lists each fork carry: a change that `google/ax` does not have.
Principle V in `.specify/memory/constitution.md` gives the rule.
Each entry has a `carry` bead. The bead and this file MUST agree.

- After each upstream sync, the tests of each entry MUST pass.
- If upstream merges an equal change, remove the carry and its entry at the next sync.
- The weekly `upstream-sync` workflow does a trial merge of `google/ax` `main` and runs `make test`.

Entries are in this order: upstream status, then the number of upstream files edited (most first).

## Task resource limits to ActorTemplate.Resources, quantity validation

**Bead:** `ax-347` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-merged`

**Upstream files edited:**

- `go.mod`
- `go.sum`
- `internal/substrate/client.go`
- `internal/substrate/client_test.go`
- `pkg/apis/v1alpha1/types.go`
- `pkg/apis/v1alpha1/types_test.go`

**Tests:**

- `internal/substrate/client_test.go`
- `pkg/apis/v1alpha1/types_test.go`

**Reason:** Upstream forwards limits to ActorTemplate.Resources and validates them, so the next sync can drop this carry.

**Removal condition:** Remove the carry and this entry at the next sync.

## Default task image from AX_DEFAULT_TASK_IMAGE

**Bead:** `ax-7at` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-merged`

**Upstream files edited:**

- `cmd/ax-controller/main.go`
- `cmd/ax-server/main.go`
- `pkg/apis/v1alpha1/types.go`

**New files:**

- `deploy/stigenai/kustomization.yaml`
- `deploy/stigenai/workerpool.yaml`

**Tests:**

- none

**Reason:** Upstream lets the operator set the default task image. The fork reads it in ax-controller (7bf9cd0 moved it there from ax-server), and upstream reconciles in ax-server (cmd/ax-server/main.go:113).

**Removal condition:** Remove the carry and this entry at the next sync.

## Fail a task whose template Substrate refuses

**Bead:** `ax-v42` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-merged`

**Upstream files edited:**

- `internal/controller/reconciler.go`
- `internal/controller/reconciler_test.go`

**Tests:**

- `internal/controller/reconciler_test.go`

**Reason:** Upstream fails the task when Substrate refuses its template, so the next sync can drop this part.

**Removal condition:** Remove the carry and this entry at the next sync.

## Snapshot bucket with no hard-coded default

**Bead:** `ax-c34` · **Branch:** `main` · **Upstream status:** `upstream-merged`

**Upstream files edited:**

- `internal/substrate/client.go`

**Tests:**

- none

**Reason:** Merge 0a37ac8 took the upstream form, so main keeps no snapshot bucket code; the f10898e error for an empty AX_SNAPSHOTS_BUCKET is gone.

**Removal condition:** Remove the carry and this entry at the next sync.

## substrate Client.ListActors with paging

**Bead:** `ax-jvl` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-merged`

**Upstream files edited:**

- `internal/substrate/client.go`

**New files:**

- `internal/substrate/list_actors_test.go`

**Tests:**

- `internal/substrate/list_actors_test.go`

**Reason:** Upstream has the same paged ListActors, so the next sync can drop this carry.

**Removal condition:** Remove the carry and this entry at the next sync.

## Actor loss detection (GetActor, missing or crashed actor)

**Bead:** `ax-ipp` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-merged`

**Upstream files edited:**

- `internal/substrate/client.go`

**New files:**

- `internal/controller/liveness.go`
- `internal/substrate/get_actor_test.go`

**Tests:**

- `internal/substrate/get_actor_test.go`

**Reason:** Upstream reads the actor on each status read and fails a task whose actor is gone or crashed.

**Removal condition:** Remove the carry and this entry at the next sync.

## Task sandboxClass to a WorkerSelector label on Substrate 1.0

**Bead:** `ax-d9n` · **Branch:** `main` · **Upstream status:** `upstream-candidate`

**Upstream files edited:**

- `docs/manifests.md`
- `internal/controller/reconciler.go`
- `internal/controller/reconciler_test.go`
- `internal/substrate/client.go`
- `internal/substrate/client_test.go`
- `pkg/apis/v1alpha1/ax.pb.go`
- `pkg/apis/v1alpha1/ax.proto`
- `pkg/apis/v1alpha1/types.go`
- `pkg/apis/v1alpha1/types_test.go`

**Tests:**

- `internal/controller/reconciler_test.go`
- `internal/substrate/client_test.go`
- `pkg/apis/v1alpha1/types_test.go`

**Reason:** Generic and about 30 non-test lines; merge 0a37ac8 dropped the pre-1.0 code of f10898e (see ax-6so), and 78753de and e213b19 port it to Substrate 1.0.

**Removal condition:** Remove the carry when upstream merges an equal change.

## Task memory and cpu caps in pkg/apis and ax-server

**Bead:** `ax-m1q` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-candidate`

**Upstream files edited:**

- `cmd/ax-server/main.go`
- `cmd/ax-server/main_test.go`
- `internal/server/server.go`
- `internal/server/server_test.go`
- `pkg/apis/v1alpha1/types.go`
- `pkg/apis/v1alpha1/types_test.go`

**Tests:**

- `cmd/ax-server/main_test.go`
- `internal/server/server_test.go`
- `pkg/apis/v1alpha1/types_test.go`

**Reason:** Generic after a port drops the SupervisorProfile branch and uses ParseMemoryBytes and ParseCPUMillicores; about 148 lines.

**Removal condition:** Remove the carry when upstream merges an equal change.

## Task spec.capabilities to the container securityContext

**Bead:** `ax-i6v` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-candidate`

**Upstream files edited:**

- `docs/manifests.md`
- `internal/controller/reconciler.go`
- `internal/controller/reconciler_test.go`
- `internal/substrate/client.go`
- `pkg/apis/v1alpha1/ax.pb.go`
- `pkg/apis/v1alpha1/ax.proto`

**Tests:**

- `internal/controller/reconciler_test.go`

**Reason:** Generic and about 51 non-pb lines. Substrate v1.0.0-rc.1 has Container.SecurityContext.Capabilities (api/ateapi/v1/ateapi.pb.go:3327,3437). A port needs a new TaskSpec field number, because upstream uses 1-7.

**Removal condition:** Remove the carry when upstream merges an equal change.

## Bearer-token caller auth (AX_AUTH_TOKEN)

**Bead:** `ax-cyt` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-candidate`

**Upstream files edited:**

- `README.md`
- `cmd/ax-server/main.go`
- `cmd/ax-server/main_test.go`
- `cmd/ax/main.go`
- `internal/server/server.go`

**New files:**

- `cmd/ax/auth_test.go`
- `internal/server/auth_test.go`

**Tests:**

- `cmd/ax-server/main_test.go`
- `cmd/ax/auth_test.go`
- `internal/server/auth_test.go`

**Reason:** Generic and about 111 lines; the CLI sends the token only over TLS or loopback, so a port needs the CLI TLS unit first.

**Removal condition:** Remove the carry when upstream merges an equal change.

## ax ssh through atenet-router, one retry on 421, AX_ROUTER

**Bead:** `ax-ifv` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-candidate`

**Upstream files edited:**

- `cmd/ax/main.go`
- `go.mod`
- `internal/guest/client.go`
- `internal/guest/client_test.go`

**New files:**

- `cmd/ax/ssh_stale_test.go`
- `cmd/ax/ssh_test.go`

**Tests:**

- `cmd/ax/ssh_stale_test.go`
- `cmd/ax/ssh_test.go`
- `internal/guest/client_test.go`

**Reason:** Generic and about 247 lines; a port must keep upstream's mTLS dialRouterTLS (cmd/ax/main.go:1152).

**Removal condition:** Remove the carry when upstream merges an equal change.

## 90 s grace for a task command after SIGTERM

**Bead:** `ax-wtb` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-candidate`

**Upstream files edited:**

- `runner/runner.go`
- `runner/runner_test.go`

**Tests:**

- `runner/runner_test.go`

**Reason:** Generic, about 24 lines in runner/runner.go with a test.

**Removal condition:** Remove the carry when upstream merges an equal change.

## ax ssh dials AX_ROUTER directly on main

**Bead:** `ax-p6s` · **Branch:** `main` · **Upstream status:** `upstream-candidate`

**Upstream files edited:**

- `cmd/ax/main.go`
- `cmd/ax/main_test.go`

**Tests:**

- `cmd/ax/main_test.go`

**Reason:** Generic and about 10 non-test lines, but it dials plain h2c (guest.DialTarget), so a port must also offer upstream's mTLS; stigenai/deploy carries it as e4b3948 in ax-ifv, with TLS from 7bc8741 (ax-u1x).

**Removal condition:** Remove the carry when upstream merges an equal change.

## ax CLI dials ax-server with TLS

**Bead:** `ax-u1x` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-candidate`

**Upstream files edited:**

- `cmd/ax/main.go`
- `internal/guest/client.go`

**New files:**

- `cmd/ax/tls_test.go`
- `internal/guest/dialtarget_tls_test.go`
- `internal/guest/transport.go`
- `internal/guest/transport_test.go`

**Tests:**

- `cmd/ax/tls_test.go`
- `internal/guest/dialtarget_tls_test.go`
- `internal/guest/transport_test.go`

**Reason:** Generic and about 75 lines. The router dial part of 7bc8741 has an upstream equivalent (tunnel.RouterTLSConfig, cmd/ax/main.go:1153).

**Removal condition:** Remove the carry when upstream merges an equal change.

## ax-server TLS listener with cert reload, --plain-addr

**Bead:** `ax-bo9` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-candidate`

**Upstream files edited:**

- `cmd/ax-server/main.go`

**New files:**

- `cmd/ax-server/tls.go`
- `cmd/ax-server/tls_test.go`
- `deploy/stigenai/kustomization.yaml`

**Tests:**

- `cmd/ax-server/tls_test.go`

**Reason:** Generic and about 117 lines in cmd/ax-server.

**Removal condition:** Remove the carry when upstream merges an equal change.

## Keep spec.env out of AX_TASK_YAML

**Bead:** `ax-d49` · **Branch:** `stigenai/deploy` · **Upstream status:** `upstream-candidate`

**Upstream files edited:**

- `internal/controller/reconciler.go`

**Tests:**

- none

**Reason:** A 4-line generic fix: env goes to the container env, so AX_TASK_YAML does not need it and stays under the size limit.

**Removal condition:** Remove the carry when upstream merges an equal change.

## Protected Fabriek supervisor template

**Bead:** `ax-7d8` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `README.md`
- `cmd/ax/main.go`
- `go.mod`
- `go.sum`
- `internal/controller/reconciler.go`
- `internal/controller/reconciler_test.go`
- `internal/controller/worker.go`
- `internal/server/server.go`
- `internal/server/server_test.go`
- `internal/store/memory/store.go`
- `internal/store/redis/store.go`
- `internal/store/store.go`
- `internal/substrate/client.go`
- `pkg/apis/v1alpha1/ax.pb.go`
- `pkg/apis/v1alpha1/ax.proto`
- `pkg/apis/v1alpha1/types.go`
- `pkg/apis/v1alpha1/types_test.go`

**New files:**

- `cmd/ax/delete_test.go`
- `deploy/stigenai/workerpool.yaml`
- `docs/fabriek-supervisor.md`
- `internal/controller/worker_delete_test.go`
- `internal/store/memory/delete_test.go`
- `internal/store/redis/delete_test.go`

**Tests:**

- `cmd/ax/delete_test.go`
- `internal/controller/reconciler_test.go`
- `internal/controller/worker_delete_test.go`
- `internal/server/server_test.go`
- `internal/store/memory/delete_test.go`
- `internal/store/redis/delete_test.go`
- `pkg/apis/v1alpha1/types_test.go`

**Reason:** The supervisor profile, its FACTORY_* env and bootstrap pools serve Fabriek only.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Task spec.workerLabels pin to worker labels

**Bead:** `ax-6tw` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `cmd/ax-controller/main.go`
- `docs/manifests.md`
- `internal/controller/reconciler.go`
- `internal/controller/reconciler_test.go`
- `internal/substrate/client.go`
- `internal/substrate/client_test.go`
- `pkg/apis/v1alpha1/ax.pb.go`
- `pkg/apis/v1alpha1/ax.proto`
- `pkg/apis/v1alpha1/types.go`
- `pkg/apis/v1alpha1/types_test.go`

**New files:**

- `deploy/stigenai/kustomization.yaml`

**Tests:**

- `internal/controller/reconciler_test.go`
- `internal/substrate/client_test.go`
- `pkg/apis/v1alpha1/types_test.go`

**Reason:** Adds a proto field; Principle III needs a design talk with upstream first. 5df4093 keeps workerLabels out of AX_TASK_YAML.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## microvm sandbox class on the pre-1.0 Substrate API

**Bead:** `ax-6so` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `docs/manifests.md`
- `internal/controller/reconciler.go`
- `internal/controller/reconciler_test.go`
- `internal/substrate/client.go`
- `internal/substrate/client_test.go`
- `pkg/apis/v1alpha1/ax.pb.go`
- `pkg/apis/v1alpha1/ax.proto`
- `pkg/apis/v1alpha1/types.go`
- `pkg/apis/v1alpha1/types_test.go`

**Tests:**

- `internal/controller/reconciler_test.go`
- `internal/substrate/client_test.go`
- `pkg/apis/v1alpha1/types_test.go`

**Reason:** origin/main e213b19 and f10898e hold the Substrate 1.0 port of this idea.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## LivenessChecker and TaskMonitor, UpdateTaskStatusIf

**Bead:** `ax-gwd` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `cmd/ax-controller/main.go`
- `docs/networking.md`
- `internal/controller/reconciler.go`
- `internal/controller/reconciler_test.go`
- `internal/store/memory/store.go`
- `internal/store/redis/store.go`
- `internal/store/store.go`
- `internal/substrate/client.go`

**New files:**

- `cmd/ax-controller/main_test.go`
- `internal/controller/` (5 files)
- `internal/store/memory/update_if_test.go`
- `internal/store/redis/update_if_test.go`
- `internal/substrate/get_actor_test.go`

**Tests:**

- `cmd/ax-controller/main_test.go`
- `internal/controller/liveness_test.go`
- `internal/controller/reconciler_test.go`
- `internal/controller/task_monitor_sweep_test.go`
- `internal/controller/task_monitor_test.go`
- `internal/store/memory/update_if_test.go`
- `internal/store/redis/update_if_test.go`
- `internal/substrate/get_actor_test.go`

**Reason:** The stale-worker probe needs a background loop, and upstream has no ax-controller to run it.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Repoint a suspended actor when its image changes

**Bead:** `ax-i8z` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `internal/controller/reconciler.go`
- `internal/controller/reconciler_test.go`
- `internal/metadata/server_test.go`
- `internal/store/memory/store.go`
- `internal/store/redis/store.go`
- `internal/store/store.go`
- `internal/substrate/client.go`

**New files:**

- `internal/controller/BUILD.bazel`
- `internal/controller/template_update_test.go`
- `internal/store/memory/BUILD.bazel`
- `internal/store/memory/delete_test.go`
- `internal/store/redis/BUILD.bazel`
- `internal/store/redis/delete_test.go`

**Tests:**

- `internal/controller/reconciler_test.go`
- `internal/controller/template_update_test.go`
- `internal/metadata/server_test.go`
- `internal/store/memory/delete_test.go`
- `internal/store/redis/delete_test.go`

**Reason:** Uses the store and reconciler that upstream removed.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## WaitingForCapacity on ResourceExhausted with requeue

**Bead:** `ax-16h` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `internal/controller/reconciler.go`
- `internal/controller/reconciler_test.go`
- `internal/controller/worker.go`
- `internal/controller/worker_test.go`
- `internal/store/memory/store.go`
- `internal/store/redis/store.go`
- `internal/store/store.go`

**New files:**

- `internal/store/redis/requeue_test.go`

**Tests:**

- `internal/controller/reconciler_test.go`
- `internal/controller/worker_test.go`
- `internal/store/redis/requeue_test.go`

**Reason:** Upstream provisions in the request and marks ResourceExhausted Failed (internal/server/server.go:356-366); only the idea could port.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## ax stdio: local stdin, live_output, stream-reset resume

**Bead:** `ax-eah` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `cmd/ax/main.go`
- `go.mod`
- `go.sum`
- `internal/guest/client.go`
- `internal/guest/client_test.go`

**New files:**

- `internal/guest/BUILD.bazel`

**Tests:**

- `internal/guest/client_test.go`

**Reason:** Port of PR #52 to deploy. Upstream 92d297e (#413) forwards stdin in ax ssh; check the overlap at the re-port (ax-mvu).

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Terminating kept for every task, atomic Suspend write

**Bead:** `ax-8km` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `internal/server/server.go`
- `internal/server/server_test.go`
- `internal/store/memory/store.go`
- `internal/store/redis/store.go`
- `internal/store/store.go`

**New files:**

- `internal/controller/worker_delete_test.go`
- `internal/store/memory/delete_test.go`
- `internal/store/redis/delete_test.go`

**Tests:**

- `internal/controller/worker_delete_test.go`
- `internal/server/server_test.go`
- `internal/store/memory/delete_test.go`
- `internal/store/redis/delete_test.go`

**Reason:** Upstream takes a per-task lock instead (internal/server/server.go:231,271) and clears Terminating on purpose (server.go:296-298).

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Task delete retry with backoff and jitter

**Bead:** `ax-lok` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `internal/controller/reconciler_test.go`
- `internal/controller/worker.go`
- `internal/store/memory/store.go`
- `internal/store/redis/store.go`
- `internal/store/store.go`

**New files:**

- `internal/controller/delete_sweep_test.go`
- `internal/controller/worker_delete_test.go`
- `internal/store/memory/requeue_delete_test.go`
- `internal/store/redis/requeue_delete_test.go`

**Tests:**

- `internal/controller/delete_sweep_test.go`
- `internal/controller/reconciler_test.go`
- `internal/controller/worker_delete_test.go`
- `internal/store/memory/requeue_delete_test.go`
- `internal/store/redis/requeue_delete_test.go`

**Reason:** Upstream deletes in the request (internal/server/server.go:220-257), so it has no delete event to retry.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Safe task tag adapter

**Bead:** `ax-b9l` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `go.mod`
- `go.sum`
- `internal/substrate/client.go`
- `internal/substrate/client_test.go`

**New files:**

- `internal/substrate/BUILD.bazel`

**Tests:**

- `internal/substrate/client_test.go`

**Reason:** Adapter for the Substrate tag API on the deploy line.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Retain source templates for snapshot tags

**Bead:** `ax-9gj` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `internal/controller/reconciler.go`
- `internal/controller/reconciler_test.go`
- `internal/substrate/client.go`
- `internal/substrate/client_test.go`

**Tests:**

- `internal/controller/reconciler_test.go`
- `internal/substrate/client_test.go`

**Reason:** Upstream rewrote snapshot tags in 9467a1f (#586) and 84ab0c7 (#592); check at the re-port (ax-mvu).

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Task caps in ax-controller, skip for a Running task

**Bead:** `ax-4k7` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `cmd/ax-controller/main.go`
- `internal/controller/reconciler.go`
- `internal/controller/reconciler_test.go`
- `internal/controller/worker_test.go`

**New files:**

- `cmd/ax-controller/main_test.go`

**Tests:**

- `cmd/ax-controller/main_test.go`
- `internal/controller/reconciler_test.go`
- `internal/controller/worker_test.go`

**Reason:** The fork reconciles a Running task again in worker.go; upstream does not, so the pkg/apis and ax-server cap unit covers it.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## ax-redis on a PVC with AOF, orphan actor report

**Bead:** `ax-d35` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `Makefile`
- `deploy/redis.yaml`
- `docs/manifests.md`

**New files:**

- `.github/workflows/deploy-ci.yml`
- `deploy/stigenai/kustomization.yaml`
- `deploy/stigenai/redis-persistence-test.sh`
- `docs/redis-persistence.md`
- `docs/stuck-deletions.md`
- `internal/controller/BUILD.bazel`
- `internal/controller/delete_sweep.go`
- `internal/controller/delete_sweep_orphan_test.go`

**Tests:**

- `deploy/stigenai/redis-persistence-test.sh`
- `internal/controller/delete_sweep_orphan_test.go`

**Reason:** Upstream removed Redis in 2bfbcb8 (#515).

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Redis stream XAUTOCLAIM reclaim and poison drop

**Bead:** `ax-2g8` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `cmd/ax-controller/main.go`
- `internal/store/redis/store.go`
- `internal/store/store.go`

**New files:**

- `cmd/ax-controller/main_test.go`
- `docs/stuck-deletions.md`
- `internal/store/redis/claim_test.go`

**Tests:**

- `cmd/ax-controller/main_test.go`
- `internal/store/redis/claim_test.go`

**Reason:** The reclaim works on the Redis stream that upstream removed.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Default task memory booking you can configure

**Bead:** `ax-7wo` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `cmd/ax-controller/main.go`
- `internal/controller/reconciler.go`
- `internal/controller/reconciler_test.go`

**New files:**

- `cmd/ax-controller/main_test.go`
- `deploy/stigenai/kustomization.yaml`
- `deploy/stigenai/workerpool.yaml`
- `internal/controller/task_monitor.go`
- `internal/controller/task_monitor_sweep_test.go`

**Tests:**

- `cmd/ax-controller/main_test.go`
- `internal/controller/reconciler_test.go`
- `internal/controller/task_monitor_sweep_test.go`

**Reason:** Upstream books a fixed 512Mi (internal/substrate/client.go:294-298); the configurable value lives in ax-controller.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Private native egress policy before actor resume

**Bead:** `ax-2cp` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `README.md`
- `cmd/ax-controller/main.go`
- `internal/controller/reconciler.go`

**New files:**

- `internal/controller/native_egress.go`
- `internal/controller/native_egress_test.go`
- `internal/substrate/native_egress.go`

**Tests:**

- `internal/controller/native_egress_test.go`

**Reason:** The egress policy names Fabriek endpoints and needs the supervisor profile.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## DeletionSweeper and stuck-deletions runbook

**Bead:** `ax-1xl` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `cmd/ax-controller/main.go`
- `docs/networking.md`

**New files:**

- `cmd/ax-controller/main_test.go`
- `docs/stuck-deletions.md`
- `internal/controller/delete_sweep.go`
- `internal/controller/delete_sweep_test.go`

**Tests:**

- `cmd/ax-controller/main_test.go`
- `internal/controller/delete_sweep_test.go`

**Reason:** The sweeper republishes delete events, and upstream has no event stream.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Beads and agent tool files

**Bead:** `ax-not` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `.gitignore`
- `AGENTS.md`

**New files:**

- `CLAUDE.md`
- `.agents/skills/beads/SKILL.md`
- `.agents/skills/beads/agents/openai.yaml`
- `.beads/` (4 files)
- `.claude/settings.json`
- `.codex/config.toml`
- `.codex/hooks.json`
- `.cursor/hooks.json`
- `.cursor/rules/beads.mdc`

**Tests:**

- none

**Reason:** The fork tracks work with bd; upstream does not.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## stigenai deploy overlay

**Bead:** `ax-h83` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `README.md`
- `cmd/ax-controller/main.go`

**New files:**

- `deploy/stigenai/kustomization.yaml`
- `deploy/stigenai/workerpool.yaml`

**Tests:**

- none

**Reason:** The overlay pins the fleet registry, image digests, env values and the ax-server-tls and ax-server-auth mounts for the stigenai deploy.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## LAP Pi task image and turn server

**Bead:** `ax-tzl` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `.dockerignore`

**New files:**

- `.github/workflows/lap-pi-task.yml`
- `images/lap-pi-task/` (8 files)

**Tests:**

- `images/lap-pi-task/pi.test.mjs`
- `images/lap-pi-task/server.test.mjs`

**Reason:** stigenai task image in images/lap-pi-task; new files and a workflow only.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## devenv with the CI toolbelt

**Bead:** `ax-lcb` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `.gitignore`

**New files:**

- `devenv.lock`
- `devenv.nix`
- `devenv.yaml`
- `.github/scripts/devenv-check.sh`
- `.github/workflows/devenv.yml`

**Tests:**

- none

**Reason:** The devenv serves the fork's CI and workers only.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Bazel build with BuildBuddy

**Bead:** `ax-k2q` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `.gitignore`

**New files:**

- `./` (5 files)
- `.github/workflows/bazel.yml`
- `cmd/ax/BUILD.bazel`
- `cmd/ax-controller/BUILD.bazel`
- `cmd/ax-server/BUILD.bazel`
- `cmd/ax-task-runner/BUILD.bazel`
- `internal/controller/BUILD.bazel`
- `internal/guest/BUILD.bazel`
- `internal/metadata/BUILD.bazel`
- `internal/model/BUILD.bazel`
- `internal/server/BUILD.bazel`
- `internal/store/BUILD.bazel`
- `internal/store/memory/BUILD.bazel`
- `internal/store/redis/BUILD.bazel`
- `internal/substrate/BUILD.bazel`
- `internal/tunnel/BUILD.bazel`
- `internal/workspace/BUILD.bazel`
- `pkg/apis/v1alpha1/BUILD.bazel`
- `runner/BUILD.bazel`

**Tests:**

- none

**Reason:** Upstream builds with go and make, not Bazel.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Fork process files (constitution, FORK.md, upstream-sync workflow)

**Bead:** `ax-9e2` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- none

**New files:**

- `.github/workflows/upstream-sync.yml`
- `.specify/.gitignore`
- `.specify/init-options.json`
- `.specify/integration.json`
- `.specify/integrations/claude.manifest.json`
- `.specify/integrations/speckit.manifest.json`
- `.specify/memory/.constitution-template.json`
- `.specify/memory/constitution.md`
- `.specify/scripts/bash/` (6 files)
- `.specify/templates/` (5 files)
- `.specify/workflows/workflow-registry.json`
- `.specify/workflows/speckit/workflow.yml`
- `skills/speckit-analyze/SKILL.md`
- `skills/speckit-checklist/SKILL.md`
- `skills/speckit-clarify/SKILL.md`
- `skills/speckit-constitution/SKILL.md`
- `skills/speckit-converge/SKILL.md`
- `skills/speckit-implement/SKILL.md`
- `skills/speckit-plan/SKILL.md`
- `skills/speckit-specify/SKILL.md`
- `skills/speckit-tasks/SKILL.md`
- `skills/speckit-taskstoissues/SKILL.md`

**Tests:**

- none

**Reason:** The fork rules, the carry list and the weekly trial merge exist only for the fork.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Deploy CI workflow

**Bead:** `ax-brx` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- none

**New files:**

- `.github/workflows/deploy-ci.yml`

**Tests:**

- none

**Reason:** The workflow guards the fork's deploy branch only.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## ax WorkerPool sizing and measurement scripts

**Bead:** `ax-rrr` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- none

**New files:**

- `deploy/stigenai/measure-workerpool-cpu.sh`
- `deploy/stigenai/measure-workerpool.sh`
- `deploy/stigenai/workerpool.yaml`

**Tests:**

- none

**Reason:** The WorkerPool size, its resources and the measure-workerpool scripts fit the gtr cluster only.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

