#!/usr/bin/env bash
# Read-only measurement recipe for CPU sizing of the ax WorkerPool (ax-1iw).
#
# ax-ci8's measure-workerpool.sh sizes memory. This script sizes CPU.
# It samples each worker pod's CPU use several times over a short window.
# The script also shows the task count that each pod has booked now.
# This helps you tell a sustained pod near its cpu limit (throttle risk)
# from a single noisy sample.
#
# The script prints 5 sections. Each section has its own "## " heading.
#   1. WorkerPool cpu spec: the live cpu request and limit per worker.
#   2. Task density now: each worker's booked actor count and memory, from
#      `kubectl ate get workers` (the real density, not the cpu limit math).
#   3. CPU samples: N snapshots of `kubectl top pods`, SAMPLE_INTERVAL
#      seconds apart, one column per sample.
#   4. Flags: each pod whose CPU is at or above THROTTLE_PCT of the cpu
#      limit in any sample. This is a throttle candidate, not proof.
#      See the PromQL note below for a real trend.
#   5. CFS throttle ratio: for each ax worker pod, the share of CPU
#      periods the kernel throttled since the pod's last start.
#      The script reads this from each node's cadvisor, through the
#      read-only `kubectl get --raw
#      /api/v1/nodes/<node>/proxy/metrics/cadvisor` endpoint. The two
#      counters are container_cpu_cfs_throttled_periods_total and
#      container_cpu_cfs_periods_total. This ratio is real proof of a
#      throttle. A `kubectl top` sample only catches one moment.
#      Set the env var THROTTLE_RATIO_PCT to flag a pod at or above a
#      ratio. The default is 5.
#
# One `kubectl top pods` call is a read at one point in time. ax-ci8
# learned this the hard way for memory. CPU use is burstier than memory.
# This script samples several times. It does not rely on one call.
#
# For a longer trend than this script's window, query Prometheus.
# Use the Grafana datasource with uid "prometheus" and only ate_* metrics.
# Query through the grafana-nixfleet MCP tool or the Grafana UI.
# For example:
#
#   sum by (instance) (rate(ate_actor_stats_cpu_time_seconds_total{ate_workerpool_name="ax"}[1m]))
#
# This query shows the cores consumed per worker instance over time.
# It uses a counter rate, not a gauge sample. It can catch a sustained
# near-limit worker that a short script run can miss.
#
# The "instance" label is the atelet's own id.
# It is not the pod UID or the pod name. Do not join it to a pod name.
# See the ax soak-signals memory note, "never compare worker pod names".
#
# Every command here is read-only: get and top.
# Run the script again at any time to refresh the numbers.
# It does not change cluster state.
#
# Usage: deploy/stigenai/measure-workerpool-cpu.sh [context] [samples] [interval_seconds] [throttle_pct]
# Env var THROTTLE_RATIO_PCT sets the flag threshold for section 5, the
# cadvisor CFS throttle ratio. Default 5 (percent).
set -euo pipefail

CTX="${1:-gtr}"
NS="ax-system"
POOL="ax"
SAMPLES="${2:-6}"
SAMPLE_INTERVAL="${3:-20}"
THROTTLE_PCT="${4:-90}"
THROTTLE_RATIO_PCT="${THROTTLE_RATIO_PCT:-5}"

echo "## WorkerPool ${POOL} cpu spec (context ${CTX})"
kubectl --context "$CTX" get workerpool "$POOL" -n "$NS" -o json \
  | jq '{replicas: .spec.replicas, cpu: {request: .spec.template.resources.requests.cpu, limit: .spec.template.resources.limits.cpu}}'
CPU_LIMIT_MILLI=$(kubectl --context "$CTX" get workerpool "$POOL" -n "$NS" -o jsonpath='{.spec.template.resources.limits.cpu}')
# Normalize "4" -> 4000m, "4000m" -> 4000m.
case "$CPU_LIMIT_MILLI" in
  *m) CPU_LIMIT_MILLI="${CPU_LIMIT_MILLI%m}" ;;
  *) CPU_LIMIT_MILLI=$((CPU_LIMIT_MILLI * 1000)) ;;
esac
echo "cpu limit normalized: ${CPU_LIMIT_MILLI}m"

echo
echo "## Task density now (kubectl ate get workers: booked actors and memory)"
kubectl ate --context "$CTX" get workers 2>&1 || true

echo
echo "## CPU samples (${SAMPLES} samples, ${SAMPLE_INTERVAL}s apart; each column is one sample's millicores)"
declare -A POD_SAMPLES
POD_ORDER=()
for i in $(seq 1 "$SAMPLES"); do
  echo "-- sample ${i}/${SAMPLES} at $(date -u +%H:%M:%SZ) --"
  LINE=$(kubectl --context "$CTX" top pods -n "$NS" -l "ate.dev/worker-pool=${POOL}" --no-headers 2>&1) || true
  echo "$LINE"
  while read -r name cpu _mem; do
    [ -z "$name" ] && continue
    cpu_m="${cpu%m}"
    if [ -z "${POD_SAMPLES[$name]+x}" ]; then
      POD_ORDER+=("$name")
      POD_SAMPLES[$name]="$cpu_m"
    else
      POD_SAMPLES[$name]="${POD_SAMPLES[$name]} $cpu_m"
    fi
  done <<< "$LINE"
  if [ "$i" -lt "$SAMPLES" ]; then
    sleep "$SAMPLE_INTERVAL"
  fi
done

echo
echo "## Flags: pod at or above ${THROTTLE_PCT}% of the ${CPU_LIMIT_MILLI}m cpu limit in any sample"
THRESHOLD_MILLI=$(( CPU_LIMIT_MILLI * THROTTLE_PCT / 100 ))
echo "threshold: ${THRESHOLD_MILLI}m"
ANY_FLAG=0
for name in "${POD_ORDER[@]}"; do
  MAX=0
  for v in ${POD_SAMPLES[$name]}; do
    if [ "$v" -gt "$MAX" ]; then MAX="$v"; fi
  done
  if [ "$MAX" -ge "$THRESHOLD_MILLI" ]; then
    echo "${name}: max ${MAX}m >= ${THRESHOLD_MILLI}m threshold (throttle candidate)"
    ANY_FLAG=1
  else
    echo "${name}: max ${MAX}m"
  fi
done
if [ "$ANY_FLAG" -eq 0 ]; then
  echo "no pod reached the threshold in this window; re-run during known load or widen SAMPLES/SAMPLE_INTERVAL"
fi

echo
echo "## CFS throttle ratio per pod (cadvisor, since each pod's last start)"
echo "ratio = throttled periods / total periods; flag at or above ${THROTTLE_RATIO_PCT}%"

declare -A POD_NODE
declare -A POD_START
POD_ALL_ORDER=()
while read -r pname pnode pstart; do
  [ -z "$pname" ] && continue
  POD_ALL_ORDER+=("$pname")
  POD_NODE[$pname]="$pnode"
  POD_START[$pname]="$pstart"
done < <(kubectl --context "$CTX" -n "$NS" get pods -l "ate.dev/worker-pool=${POOL}" -o json \
  | jq -r '.items[] | "\(.metadata.name) \(.spec.nodeName) \(.status.startTime)"')

NODES=()
for pname in "${POD_ALL_ORDER[@]}"; do
  node="${POD_NODE[$pname]}"
  case " ${NODES[*]:-} " in
    *" ${node} "*) ;;
    *) NODES+=("$node") ;;
  esac
done

declare -A PERIODS
declare -A THROTTLED
for node in "${NODES[@]}"; do
  RAW=$(kubectl --context "$CTX" get --raw "/api/v1/nodes/${node}/proxy/metrics/cadvisor" 2>/dev/null) || {
    echo "warn: cadvisor fetch failed for node ${node}" >&2
    continue
  }
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    pod=$(echo "$line" | sed -E 's/.*pod="([^"]*)".*/\1/')
    [ -z "${POD_NODE[$pod]+x}" ] && continue
    val=$(echo "$line" | awk -F'} ' '{print $2}' | awk '{print $1}')
    PERIODS[$pod]="$val"
  done < <(echo "$RAW" | grep '^container_cpu_cfs_periods_total{' | grep "namespace=\"${NS}\"" | grep 'container=""')
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    pod=$(echo "$line" | sed -E 's/.*pod="([^"]*)".*/\1/')
    [ -z "${POD_NODE[$pod]+x}" ] && continue
    val=$(echo "$line" | awk -F'} ' '{print $2}' | awk '{print $1}')
    THROTTLED[$pod]="$val"
  done < <(echo "$RAW" | grep '^container_cpu_cfs_throttled_periods_total{' | grep "namespace=\"${NS}\"" | grep 'container=""')
done

printf '%-24s %-10s %10s %10s %8s %9s\n' "POD" "NODE" "PERIODS" "THROTTLED" "RATIO%" "AGE_MIN"
ANY_RATIO_FLAG=0
for pname in "${POD_ALL_ORDER[@]}"; do
  node="${POD_NODE[$pname]}"
  start="${POD_START[$pname]}"
  periods="${PERIODS[$pname]:-0}"
  throttled="${THROTTLED[$pname]:-0}"
  ratio=$(awk -v t="$throttled" -v p="$periods" 'BEGIN { if (p > 0) { printf "%.1f", (t / p) * 100 } else { printf "0.0" } }')
  start_epoch=$(date -u -d "$start" +%s 2>/dev/null) || start_epoch=$(date -j -u -f "%Y-%m-%dT%H:%M:%SZ" "$start" +%s 2>/dev/null) || start_epoch=0
  now_epoch=$(date -u +%s)
  if [ "$start_epoch" -gt 0 ]; then
    age_min=$(( (now_epoch - start_epoch) / 60 ))
  else
    age_min=-1
  fi
  flagged=$(awk -v r="$ratio" -v thresh="$THROTTLE_RATIO_PCT" 'BEGIN { print (r >= thresh) ? "1" : "0" }')
  mark=""
  if [ "$flagged" = "1" ]; then
    mark=" <- throttled"
    ANY_RATIO_FLAG=1
  fi
  printf '%-24s %-10s %10s %10s %7s%% %8s%s\n' "$pname" "$node" "$periods" "$throttled" "$ratio" "$age_min" "$mark"
done
if [ "$ANY_RATIO_FLAG" -eq 0 ]; then
  echo "no pod's CFS throttle ratio reached ${THROTTLE_RATIO_PCT}% since its last start"
fi
