#!/usr/bin/env bash
# Restart test for the ax-redis manifest (ax-vnr).
#
# 1. Static: deploy/redis.yaml has a PVC, strategy Recreate, AOF args and a
#    /data mount from that PVC.
# 2. Dynamic: start Redis with the args of the manifest, write task keys, a
#    stream entry, a consumer group and a pending entry. Then do a SIGKILL
#    (container crash) and a SHUTDOWN NOSAVE (pod restart), each followed by a
#    start on the same data dir. The data must be the same each time.
#
# Usage: redis-persistence-test.sh [manifest]
# Needs yq v4, and docker or redis-server on PATH.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
manifest=${1:-$here/../redis.yaml}

fail() { echo "FAIL: $*" >&2; exit 1; }

dep='select(.kind == "Deployment" and .metadata.name == "ax-redis")'
ctr="$dep | .spec.template.spec.containers[] | select(.name == \"redis\")"
pvc='select(.kind == "PersistentVolumeClaim" and .metadata.name == "ax-redis-data")'

q() { yq "$1" "$manifest"; }

# --- static checks ---
controller='select(.kind == "Deployment" and .metadata.name == "ax-controller")'
[ "$(yq "$controller | .spec.strategy.type" "$here/../ax-controller.yaml")" = Recreate ] ||
  fail "Deployment ax-controller must use strategy Recreate"
[ "$(yq "$controller | .spec.replicas" "$here/../ax-controller.yaml")" = 1 ] ||
  fail "Deployment ax-controller must use one replica"
[ "$(q "$pvc | .spec.accessModes[0]")" = ReadWriteOnce ] || fail "PVC ax-redis-data must be ReadWriteOnce"
[ "$(q "$pvc | .spec.resources.requests.storage")" = 1Gi ] || fail "PVC ax-redis-data must request 1Gi"
[ "$(q "$pvc | .spec.storageClassName")" = null ] || fail "PVC ax-redis-data must not set storageClassName"
[ "$(q "$dep | .spec.strategy.type")" = Recreate ] || fail "Deployment ax-redis must use strategy Recreate"
[ "$(q "$ctr | .command")" = null ] || fail "container redis must use args, not command"
args=$(q "$ctr | .args | join(\" \")")
case " $args " in *" --appendonly yes "*) ;; *) fail "args must have --appendonly yes" ;; esac
case " $args " in *" --appendfsync everysec "*) ;; *) fail "args must have --appendfsync everysec" ;; esac
case " $args " in *" --dir /data "*) ;; *) fail "args must have --dir /data" ;; esac
vol=$(q "$ctr | .volumeMounts[] | select(.mountPath == \"/data\") | .name")
[ -n "$vol" ] || fail "container redis must mount a volume at /data"
claim=$(q "$dep | .spec.template.spec.volumes[] | select(.name == \"$vol\") | .persistentVolumeClaim.claimName")
{ [ "$claim" != null ] && [ -n "$claim" ]; } || fail "volume $vol must be a persistentVolumeClaim"
[ "$(q "select(.kind == \"PersistentVolumeClaim\" and .metadata.name == \"$claim\") | .metadata.name")" = "$claim" ] ||
  fail "claim $claim is not a PersistentVolumeClaim in $manifest"
echo "static checks ok"

# --- dynamic checks ---
work=$(mktemp -d)
chmod 700 "$work"
pid=""
cid=""
dvol=""
read -ra rargs <<<"$args"

# The test removes only what it created. Docker assigns the container id and
# the volume name. Local Redis listens on a private Unix socket in $work, with
# no TCP port, so it cannot reach a Redis that already runs on the host.
cleanup() {
  [ -z "$pid" ] || kill -9 "$pid" 2>/dev/null || true
  [ -z "$cid" ] || docker rm -f "$cid" >/dev/null 2>&1 || true
  [ -z "$dvol" ] || docker volume rm "$dvol" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

if command -v docker >/dev/null; then
  image=$(q "$ctr | .image")
  dvol=$(docker volume create)
  rc() { docker exec "$cid" redis-cli "$@"; }
  start() {
    cid=$(docker run -d -v "$dvol:/data" "$image" "${rargs[@]}")
  }
  crash() { docker kill "$cid" >/dev/null; docker rm "$cid" >/dev/null; cid=""; }
  stop() { rc SHUTDOWN NOSAVE >/dev/null 2>&1 || true; docker wait "$cid" >/dev/null; docker rm "$cid" >/dev/null; cid=""; }
else
  command -v redis-server >/dev/null || fail "need docker or redis-server on PATH"
  rc() { redis-cli -s "$work/redis.sock" "$@"; }
  start() {
    local a=() skip=0 x
    for x in "${rargs[@]}"; do
      if [ "$skip" = 1 ]; then skip=0; continue; fi
      if [ "$x" = --dir ]; then skip=1; continue; fi
      a+=("$x")
    done
    redis-server "${a[@]}" --dir "$work" --port 0 --unixsocket "$work/redis.sock" --unixsocketperm 700 >"$work/server.log" 2>&1 &
    pid=$!
  }
  crash() { kill -9 "$pid"; wait "$pid" 2>/dev/null || true; pid=""; rm -f "$work/redis.sock"; }
  stop() { rc SHUTDOWN NOSAVE >/dev/null 2>&1 || true; wait "$pid" 2>/dev/null || true; pid=""; }
fi

wait_ready() {
  for _ in $(seq 1 60); do
    [ "$(rc ping 2>/dev/null || true)" = PONG ] && return 0
    sleep 0.5
  done
  fail "redis did not become ready"
}

snapshot() {
  rc DBSIZE
  rc GET ax:task:t:a
  rc GET ax:task:t:b
  rc GET ax:task:u:c
  rc ZRANGE ax:tasks:index 0 -1 WITHSCORES
  rc ZRANGE ax:tasks:atespace:t 0 -1 WITHSCORES
  rc ZRANGE ax:tasks:atespace:u 0 -1 WITHSCORES
  rc XLEN ax:stream:tasks
  rc XINFO GROUPS ax:stream:tasks
  rc XPENDING ax:stream:tasks ax-controllers | head -n1
}

start
wait_ready
# The fixture has the shape of SaveTask (internal/store/redis/store.go): a
# task record with metadata, a score in the global and atespace indexes, and
# a stream entry with action, atespace and name.
save_task() { # atespace name score
  rc SET "ax:task:$1:$2" "{\"apiVersion\":\"ax.io/v1alpha1\",\"kind\":\"Task\",\"metadata\":{\"name\":\"$2\",\"atespace\":\"$1\"},\"status\":{\"phase\":\"Running\"}}" >/dev/null
  rc ZADD ax:tasks:index "$3" "$1:$2" >/dev/null
  rc ZADD "ax:tasks:atespace:$1" "$3" "$2" >/dev/null
  rc XADD ax:stream:tasks '*' action reconcile atespace "$1" name "$2" >/dev/null
}
save_task t a 1001
save_task t b 1002
save_task u c 1003
rc XGROUP CREATE ax:stream:tasks ax-controllers 0 >/dev/null
rc XREADGROUP GROUP ax-controllers c1 COUNT 1 STREAMS ax:stream:tasks '>' >/dev/null
before=$(snapshot)
[ "$(rc XPENDING ax:stream:tasks ax-controllers | head -n1)" = 1 ] || fail "setup: want 1 pending entry"
sleep 2 # appendfsync everysec

crash
start
wait_ready
[ "$(snapshot)" = "$before" ] || fail "data differs after a SIGKILL and restart"
echo "SIGKILL restart ok"

stop
start
wait_ready
[ "$(snapshot)" = "$before" ] || fail "data differs after a SHUTDOWN NOSAVE and new container"
echo "pod restart ok"
echo "PASS"
