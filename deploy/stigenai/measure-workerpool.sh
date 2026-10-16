#!/usr/bin/env bash
# Read-only measurement recipe for sizing the ax WorkerPool (ax-ci8).
#
# This script reads the live gtr cluster. It prints 7 sections under
# their own "## " headings:
#   1. WorkerPool spec: the live replicas, requests and limits.
#   2. Worker pods: each pod's node, restart count and age.
#   3. Current top: each pod's memory and CPU use now.
#   4. Last OOMKilled instance per pod: the reason, the exit code, and
#      the start and finish time of the killed instance.
#   5. Nodes carrying the pool's nodeSelector: the allocatable memory of
#      each node.
#   6. Node top: each node's memory and CPU use now.
#   7. Node allocated resources: the scheduler's view of each node
#      (Requests and Limits).
#
# Use section 4's times to date the known OOM incidents. Do not guess
# the times.
#
# Use section 7 to check a resize still fits a node with room to spare.
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
