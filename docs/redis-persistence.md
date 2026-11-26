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

## Private work directory

Make one private directory outside the checkout. Run all local steps in it. Run `umask 077` in each new shell.

```bash
umask 077
WORK=$(mktemp -d "${TMPDIR:-/tmp}/ax-redis-migration.XXXXXX")
cd "$WORK"
stat -c %a "$WORK"
```

`mktemp -d` makes the directory with mode 700. The `stat` command must print `700`. The `kubectl exec` command does not use the local umask. Set the mode of each directory in the pod yourself.

## Task inventory

The CLI task list is not an inventory. The CLI sends no limit, and the server returns at most 50 tasks. Its AGE column also changes. Read the full task index of Redis instead. It holds one `<atespace>:<name>` member for each task.

Capture it only after Redis answers PING. Use `pipefail`, so a failed `kubectl` fails the capture. A capture must not be empty.

```bash
kubectl --context gtr -n ax-system exec deploy/ax-redis -- redis-cli PING
bash -o pipefail -c 'kubectl --context gtr -n ax-system exec deploy/ax-redis -- redis-cli ZRANGE ax:tasks:index 0 -1 | sort > tasks-before.txt'
test -s tasks-before.txt && wc -l tasks-before.txt
```

`PING` must print `PONG`. If a command fails, stop. Do not go on with an old or empty file.

The task inventory and the `ax get task` output name tasks and can show task specs. Keep them in the private work directory. Use a new file name for each record. To compare, run `diff tasks-before.txt tasks-after.txt`. The diff must be empty. `ax -a <atespace> get task <name>` needs the ax-server RPC. Run it for 3 tasks only while ax-server runs. Keep the output. After the step, when ax-server runs again, each of these commands must succeed with the same spec.

## Migration

A plain apply of the new manifest starts Redis with 0 keys. Redis with `--appendonly yes` ignores `dump.rdb` when it has no AOF file. Seed the new Redis from the old data.

1. Stop the writers. If a writer runs during the save, the saved file lacks its last writes.

   ```bash
   kubectl --context gtr -n ax-system scale deploy/ax-server deploy/ax-controller --replicas=0
   kubectl --context gtr -n ax-system wait --for=delete pod -l app.kubernetes.io/name=ax-server --timeout=120s
   kubectl --context gtr -n ax-system wait --for=delete pod -l app.kubernetes.io/name=ax-controller --timeout=120s
   ```

2. Record the baseline after the writers are gone: the task inventory (see "Task inventory") and `DBSIZE`. Then save the data and keep the file. It is the rollback copy.

   The file holds the full database. It can contain tokens. Run this step in the private work directory. Its `ls -l` must show mode `-rw-------`.

   ```bash
   kubectl --context gtr -n ax-system exec deploy/ax-redis -- redis-cli SAVE
   kubectl --context gtr -n ax-system cp "$(kubectl --context gtr -n ax-system get pod -l app.kubernetes.io/name=ax-redis -o name | cut -d/ -f2)":/data/dump.rdb ./ax-redis-before.rdb
   chmod 600 ax-redis-before.rdb
   ls -l ax-redis-before.rdb
   kubectl --context gtr -n ax-system exec deploy/ax-redis -- redis-cli DBSIZE
   ```

   Before step 1, while ax-server still runs, keep the output of `ax -a <atespace> get task <name>` for 3 tasks. Compare the inventory in step 6.

3. Apply the manifest. The Recreate strategy stops the old pod first.

   ```bash
   kubectl --context gtr apply -f deploy/redis.yaml
   kubectl --context gtr -n ax-system rollout status deploy/ax-redis
   ```

   The new pod has an empty `/data`. If the claim stays Pending, stop and go to "Rollback".

4. Seed. Copy the saved file to a temporary directory in the pod. Start a second Redis on port 6380 with it. Make the main Redis a replica of it. Then make the main Redis a master again.

   ```bash
   POD=$(kubectl --context gtr -n ax-system get pod -l app.kubernetes.io/name=ax-redis -o name | cut -d/ -f2)
   if kubectl --context gtr -n ax-system exec "$POD" -- sh -c 'umask 077 && mkdir -p /tmp/seed && chmod 700 /tmp/seed && test "$(stat -c %a /tmp/seed)" = 700'; then
     kubectl --context gtr -n ax-system cp ./ax-redis-before.rdb "$POD":/tmp/seed/dump.rdb &&
     kubectl --context gtr -n ax-system exec "$POD" -- sh -c 'redis-server --port 6380 --dir /tmp/seed --appendonly no --daemonize yes' &&
     kubectl --context gtr -n ax-system exec "$POD" -- redis-cli REPLICAOF 127.0.0.1 6380 &&
     kubectl --context gtr -n ax-system exec "$POD" -- redis-cli INFO replication
   else
     echo "seed dir is not mode 700: stop"
   fi
   ```

   The copy runs only if `/tmp/seed` has mode `700`. If the `else` branch prints the stop message, do not run the next command. Repeat `INFO replication` until it shows `master_link_status:up`. Then run:

   ```bash
   kubectl --context gtr -n ax-system exec "$POD" -- redis-cli REPLICAOF NO ONE
   kubectl --context gtr -n ax-system exec "$POD" -- redis-cli -p 6380 SHUTDOWN NOSAVE
   kubectl --context gtr -n ax-system exec "$POD" -- rm -rf /tmp/seed
   kubectl --context gtr -n ax-system exec "$POD" -- ls /tmp/seed
   ```

   The `rm` removes the second copy of the data from the pod. The `ls` must fail with "No such file".

5. Check. `DBSIZE` must equal the baseline. `INFO persistence` must show `aof_enabled:1` and `aof_last_write_status:ok`.

   ```bash
   kubectl --context gtr -n ax-system exec "$POD" -- redis-cli DBSIZE
   kubectl --context gtr -n ax-system exec "$POD" -- redis-cli INFO persistence
   kubectl --context gtr -n ax-system get pvc ax-redis-data
   ```

6. Scale the writers back.

   ```bash
   kubectl --context gtr -n ax-system scale deploy/ax-server deploy/ax-controller --replicas=1
   kubectl --context gtr -n ax-system rollout status deploy/ax-server deploy/ax-controller
   ```

   Wait until Redis answers PING, then record the task inventory again. It must equal the inventory of step 2. Run `ax -a <atespace> get task <name>` for the 3 tasks that you kept before step 1. Each command must succeed.

Remove the private work directory with each local file in it (`ax-redis-before.rdb`, the inventory files and the saved `ax get task` output) when the owner accepts the migration. Leave the directory, then run `rm -r "$WORK"` and `ls -d "$WORK"`. The `ls` must fail with "No such file".

## Restart check

CI runs `bash deploy/stigenai/redis-persistence-test.sh` on each pull request. It starts Redis with the args of `deploy/redis.yaml`. It writes 3 task records with their indexes, 3 stream entries, a consumer group and one pending entry. It then does a SIGKILL and a restart, and a `SHUTDOWN NOSAVE` and a new start on the same directory. After each step the data must be the same.

To check the live cluster, record the task inventory (it starts with a PING). Restart the container. Wait until Redis is ready. Record the inventory again and compare.

```bash
kubectl --context gtr -n ax-system exec deploy/ax-redis -- redis-cli SHUTDOWN NOSAVE
kubectl --context gtr -n ax-system rollout status deploy/ax-redis
kubectl --context gtr -n ax-system exec deploy/ax-redis -- redis-cli PING
```

Then restart the pod. Wait until Redis is ready. Compare the inventory again.

```bash
kubectl --context gtr -n ax-system delete pod -l app.kubernetes.io/name=ax-redis
kubectl --context gtr -n ax-system rollout status deploy/ax-redis
kubectl --context gtr -n ax-system exec deploy/ax-redis -- redis-cli PING
```

`ax ssh` to a Running task must connect after both.

## Orphan recovery

The delete sweep of ax-controller logs each orphan: an AX actor with no task record in any phase. The line has `msg="delete sweep: orphan actor"` and the fields `atespace`, `name`, `uid`, `state`, `template`, `worker_pod` and `age`. The summary line `delete sweep: stuck deletions` has the count in `orphan_actors`. The sweep only reports. It never rebuilds a record, because a rebuild can bring back a task that a person deleted.

The sweep skips the report when it cannot list the tasks.

```
{namespace="ax-system",container="controller"} |= "delete sweep: orphan actor"
```

To recover an orphan, apply the workspace manifest and then the task manifest again. Keep the original `creationTimestamp` of the task. The template name and the actor then stay the same. If the task is gone for good, a person deletes the actor (see `docs/stuck-deletions.md`).

## Owner steps

Only the owner runs these steps. They need the live cluster. CI and the offline checks cannot prove them. Run them after the pull request is merged.

| AC | Offline proof (done) | Owner step (post-merge) |
| --- | --- | --- |
| AC1 manifest | Static check in `deploy/stigenai/redis-persistence-test.sh` | Pre-check: `kubectl --context gtr get storageclass` |
| AC2 restart keeps data | `bash deploy/stigenai/redis-persistence-test.sh` against a local Redis with a data directory | "Migration", then "Restart check" |
| AC4, AC5 orphan report | `go test ./internal/controller -run TestDeleteSweepOrphanActors` | Apply ax-controller with `ko`, then read the `delete sweep: orphan actor` log lines |
| AC7 live migration | None | "Migration" steps 1 to 6 |
| AC8 soak | None | "Restart check" on the live pod, then watch the controller logs |
| ax-89l k0s-agents | None (same `deploy/redis.yaml`) | Run "Pre-check", "Migration" and "Restart check" with the k0s-agents context and namespace |

## Rollback

Use this procedure only before the writers resume.

### Rollback before the writers resume (before Migration step 6)

1. Scale the writers to 0 (they are 0 already).
2. Apply the old `deploy/redis.yaml`. Redis starts with no data.
3. Seed it from `ax-redis-before.rdb` with the same steps as in "Migration" step 4. No write happened after that file, so no data is lost.
4. Wait until Redis answers PING. Check `DBSIZE` and the task inventory against the baseline of Migration step 2. Scale the writers back to 1. Wait for the rollout.
5. Run `ax -a <atespace> get task <name>` for 3 tasks. Each command must succeed.
6. Remove `/tmp/seed` from the pod (Migration step 4). Delete the claim only after steps 4 and 5 pass: `kubectl --context gtr -n ax-system delete pvc ax-redis-data`.

### Do not apply the old manifest after the writers resume

After the writers resume, the old `deploy/redis.yaml` has no persistence. The next restart loses the data again. Do not apply it. Fix forward on the claim `ax-redis-data`. Do not delete the claim. A `git revert` of the pull request changes no live object.

A rollback of ax-controller removes only the `orphan_actors` field.
