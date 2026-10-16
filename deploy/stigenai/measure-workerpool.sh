#!/usr/bin/env bash
# Read-only measurement recipe for sizing the ax WorkerPool (ax-ci8).
#
# This script reads the live gtr cluster. It prints 4 reports.
# Report 1 is the WorkerPool's live spec: replicas, requests, limits.
# Report 2 is each worker pod's node, restart count and top usage now.
# Report 3 is each worker pod's last OOMKilled event. It gives the reason,
# the exit code, and the start and finish time of the killed instance.
# Use these times to date the known OOM incidents. Do not guess the times.
# Report 4 covers every node with the pool's nodeSelector. It gives the
# allocatable memory, the top usage now, and the scheduler's view
# (Allocated resources: Requests and Limits). Use it to check a resize
# still fits a node with room to spare.
#
# Every command is read-only: get, top, or describe. Run the script again
# at any time to refresh the numbers. It does not change cluster state.
#
# Usage: deploy/stigenai/measure-workerpool.sh [context]
set -euo pipefail

CTX="${1:-gtr}"
NS="ax-system"
POOL="ax"

echo "## WorkerPool ${POOL} spec (context ${CTX})"
kubectl --context "$CTX" get workerpool "$POOL" -n "$NS" -o json \
  | jq '{replicas: .spec.replicas, resources: .spec.template.resources, status: .status}'

echo
echo "## Worker pods"
kubectl --context "$CTX" get pods -n "$NS" -l "ate.dev/worker-pool=${POOL}" \
  -o custom-columns='NAME:.metadata.name,NODE:.spec.nodeName,RESTARTS:.status.containerStatuses[0].restartCount,AGE:.metadata.creationTimestamp'

echo
echo "## Current top (point-in-time; low under light load, not peak)"
kubectl --context "$CTX" top pods -n "$NS" -l "ate.dev/worker-pool=${POOL}" 2>&1 || true

echo
echo "## Last OOMKilled instance per pod (dates the known incidents)"
for pod in $(kubectl --context "$CTX" get pods -n "$NS" -l "ate.dev/worker-pool=${POOL}" -o jsonpath='{.items[*].metadata.name}'); do
  kubectl --context "$CTX" get pod "$pod" -n "$NS" -o json | jq -r --arg pod "$pod" '
    .status.containerStatuses[0].lastState.terminated as $t
    | if $t == null then "\($pod): no terminated state recorded"
      else "\($pod): reason=\($t.reason) exitCode=\($t.exitCode) started=\($t.startedAt) finished=\($t.finishedAt)"
      end'
done

echo
echo "## Nodes carrying the pool's nodeSelector"
SELECTOR=$(kubectl --context "$CTX" get workerpool "$POOL" -n "$NS" -o jsonpath='{.spec.template.nodeSelector}' | jq -r 'to_entries[] | "\(.key)=\(.value)"' | paste -sd, -)
kubectl --context "$CTX" get nodes -l "$SELECTOR" -o custom-columns='NAME:.metadata.name,ALLOC_CPU:.status.allocatable.cpu,ALLOC_MEM:.status.allocatable.memory'

echo
echo "## Node top (actual usage, point-in-time)"
for node in $(kubectl --context "$CTX" get nodes -l "$SELECTOR" -o jsonpath='{.items[*].metadata.name}'); do
  kubectl --context "$CTX" top node "$node" 2>&1 || true
done

echo
echo "## Node allocated resources (what the scheduler sees: requests vs limits)"
for node in $(kubectl --context "$CTX" get nodes -l "$SELECTOR" -o jsonpath='{.items[*].metadata.name}'); do
  echo "-- ${node} --"
  kubectl --context "$CTX" describe node "$node" 2>&1 | sed -n '/Allocated resources/,/^Events:/p' | grep -E 'Resource|cpu |memory '
done
