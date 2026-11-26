# ax-redis persistence

ax-redis holds the task records, the workspace records and the task stream. Before ax-vnr it had no volume. A restart of the container dropped all of them while the Substrate actors kept running.

All commands below use context `gtr` and namespace `ax-system`. A person must run the steps in "Migration" and "Rollback". A worker must not run them.

## What persists

`deploy/redis.yaml` now has:

- A PersistentVolumeClaim `ax-redis-data`: ReadWriteOnce, 1Gi, default StorageClass.
- Deployment `ax-redis` with strategy `Recreate`. The claim is ReadWriteOnce, so two pods must not share it.
- Redis args `--appendonly yes --appendfsync everysec --dir /data`.

A crash can lose about 1 second of writes. The AOF rewrite needs about 2 times the size of the data.

The volume holds task records. A task `spec.env` can hold a token. The volume uses the default StorageClass of the cluster. Redis has no auth, as before.

## Pre-check: StorageClass

The cluster must have a default StorageClass. If it has none, the claim stays Pending and Redis stays down.

```bash
kubectl --context gtr get storageclass
```

One class must show `(default)`.

## Migration

A plain apply of the new manifest starts Redis with 0 keys. Redis with `--appendonly yes` ignores `dump.rdb` when it has no AOF file. Seed the new Redis from the old data.

1. Save the current data and keep the file. It is the rollback copy.

   ```bash
   kubectl --context gtr -n ax-system exec deploy/ax-redis -- redis-cli SAVE
   kubectl --context gtr -n ax-system cp "$(kubectl --context gtr -n ax-system get pod -l app.kubernetes.io/name=ax-redis -o name | cut -d/ -f2)":/data/dump.rdb ./ax-redis-before.rdb
   ```

   Record the baseline: `DBSIZE` and `ax -a <atespace> get tasks` for each atespace.

   ```bash
   kubectl --context gtr -n ax-system exec deploy/ax-redis -- redis-cli DBSIZE
   ```

2. Stop the writers.

   ```bash
   kubectl --context gtr -n ax-system scale deploy/ax-server deploy/ax-controller --replicas=0
   ```

3. Apply the manifest. The Recreate strategy stops the old pod first.

   ```bash
   kubectl --context gtr apply -f deploy/redis.yaml
   kubectl --context gtr -n ax-system rollout status deploy/ax-redis
   ```

   The new pod has an empty `/data`. If the claim stays Pending, stop and go to "Rollback".

4. Seed. Copy the saved file to a temporary directory in the pod. Start a second Redis on port 6380 with it. Make the main Redis a replica of it. Then make the main Redis a master again.

   ```bash
   POD=$(kubectl --context gtr -n ax-system get pod -l app.kubernetes.io/name=ax-redis -o name | cut -d/ -f2)
   kubectl --context gtr -n ax-system exec "$POD" -- mkdir -p /tmp/seed
   kubectl --context gtr -n ax-system cp ./ax-redis-before.rdb "$POD":/tmp/seed/dump.rdb
   kubectl --context gtr -n ax-system exec "$POD" -- sh -c 'redis-server --port 6380 --dir /tmp/seed --appendonly no --daemonize yes'
   kubectl --context gtr -n ax-system exec "$POD" -- redis-cli REPLICAOF 127.0.0.1 6380
   kubectl --context gtr -n ax-system exec "$POD" -- redis-cli INFO replication
   ```

   Repeat the last command until it shows `master_link_status:up`. Then run:

   ```bash
   kubectl --context gtr -n ax-system exec "$POD" -- redis-cli REPLICAOF NO ONE
   kubectl --context gtr -n ax-system exec "$POD" -- redis-cli -p 6380 SHUTDOWN NOSAVE
   ```

5. Check. `DBSIZE` must equal the baseline. `INFO persistence` must show `aof_enabled:1` and `aof_last_write_status:ok`.

   ```bash
   kubectl --context gtr -n ax-system exec "$POD" -- redis-cli DBSIZE
   kubectl --context gtr -n ax-system exec "$POD" -- redis-cli INFO persistence
   kubectl --context gtr -n ax-system get pvc ax-redis-data
   ```

6. Scale the writers back.

   ```bash
   kubectl --context gtr -n ax-system scale deploy/ax-server deploy/ax-controller --replicas=1
   ```

## Restart check

CI runs `bash deploy/stigenai/redis-persistence-test.sh` on each pull request. It starts Redis with the args of `deploy/redis.yaml`. It writes 3 task keys, one stream entry, a consumer group and one pending entry. It then does a SIGKILL and a restart, and a `SHUTDOWN NOSAVE` and a new start on the same directory. After each step the data must be the same.

To check the live cluster, record `ax -a <atespace> get tasks` for each atespace. Then restart the container and the pod, and compare the lists after each.

```bash
kubectl --context gtr -n ax-system exec deploy/ax-redis -- redis-cli SHUTDOWN NOSAVE
kubectl --context gtr -n ax-system delete pod -l app.kubernetes.io/name=ax-redis
```

`ax ssh` to a Running task must connect after both.

## Orphan recovery

The delete sweep of ax-controller logs each orphan: an AX actor with no task record in any phase. The line has `msg="delete sweep: orphan actor"` and the fields `atespace`, `name`, `uid`, `state`, `template`, `worker_pod` and `age`. The summary line `delete sweep: stuck deletions` has the count in `orphan_actors`. The sweep only reports. It never rebuilds a record, because a rebuild can bring back a task that a person deleted.

The sweep skips the report when it cannot list the tasks.

```
{namespace="ax-system",container="controller"} |= "delete sweep: orphan actor"
```

To recover an orphan, apply the workspace manifest and then the task manifest again. Keep the original `creationTimestamp` of the task. The template name and the actor then stay the same. If the task is gone for good, a person deletes the actor (see `docs/stuck-deletions.md`).

## Rollback

1. Revert the pull request, and scale the writers to 0.
2. Apply the old `deploy/redis.yaml`. Redis starts with no data.
3. Seed it from `ax-redis-before.rdb` with the same steps as in "Migration" step 4.
4. Scale the writers back to 1.
5. Delete the claim only after the data is back.

   ```bash
   kubectl --context gtr -n ax-system delete pvc ax-redis-data
   ```

A rollback of ax-controller removes only the `orphan_actors` field.
