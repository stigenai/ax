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

The migration operator keeps the rollback copy. The operator is the person who runs the migration. Keep the copy on persistent local storage outside the checkout. Do not use shared temporary storage (`/tmp`) or a path with automatic cleanup.

Start at the root of the checkout. Make one new private directory under your home. Run all local steps in it. Run `umask 077` before the first capture.

```bash
umask 077
REDIS_MANIFEST="$(pwd -P)/deploy/redis.yaml"
test -r "$REDIS_MANIFEST" || exit 1
WORK="$HOME/ax-redis-migration.$(date +%Y%m%d%H%M%S).$$"
mkdir -m 700 "$WORK" || exit 1
test -O "$WORK" && test "$(ls -ld "$WORK" | cut -c1-10)" = "drwx------" || exit 1
cd "$WORK" || exit 1
```

`REDIS_MANIFEST` keeps the absolute path of the manifest. If a command fails, stop. Do not start a capture.

The `test -O` and `ls -ld` checks make sure that you own the directory and that its mode is 700. They work on Linux and macOS.

Use this one shell for all steps. The steps use relative paths in `WORK`. Step 3 needs `REDIS_MANIFEST`. Cleanup needs `WORK`.

If you lose the shell before the first capture, start again from the checkout root. Make a new private directory.

If you lose the shell after any capture or after the start of step 1, stop. Do not repeat step 1. Do not make a new directory. Keep the original directory and all files that you captured so far.

Partial shutdown, seed, rollback and cleanup need these files. Ask the owner how to go on.

The `kubectl exec` command does not use the local umask. Set the mode of each directory in the pod yourself.

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

Before step 1, while ax-server still runs, keep the output of `ax -a <atespace> get task <name>` for 3 tasks. Compare the inventory in step 6.

1. Stop the writers. If a writer runs during the save, the saved file lacks its last writes.

   ```bash
   kubectl --context gtr -n ax-system scale deploy/ax-server deploy/ax-controller --replicas=0
   kubectl --context gtr -n ax-system wait --for=delete pod -l app.kubernetes.io/name=ax-server --timeout=120s
   kubectl --context gtr -n ax-system wait --for=delete pod -l app.kubernetes.io/name=ax-controller --timeout=120s
   ```

2. Record the baseline after the writers are gone: the task inventory (see "Task inventory") and `DBSIZE`. Then save the data and keep the file. It is the rollback copy.

   The file holds the full database. It can contain tokens. Run this step in the private work directory. For the RDB file, `ls -l` must show `-rw-------`.

   ```bash
   kubectl --context gtr -n ax-system exec deploy/ax-redis -- redis-cli SAVE
   kubectl --context gtr -n ax-system cp "$(kubectl --context gtr -n ax-system get pod -l app.kubernetes.io/name=ax-redis -o name | cut -d/ -f2)":/data/dump.rdb ./ax-redis-before.rdb
   chmod 600 ax-redis-before.rdb
   ls -l ax-redis-before.rdb
   kubectl --context gtr -n ax-system exec deploy/ax-redis -- redis-cli DBSIZE
   ```

3. Apply the manifest. The Recreate strategy stops the old pod first.

   ```bash
   kubectl --context gtr apply -f "$REDIS_MANIFEST"
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

   The replica sync of step 4 starts an AOF rewrite. Until the rewrite ends, the AOF on the volume does not hold the seeded data. Repeat `INFO persistence` until it shows `aof_rewrite_in_progress:0` and `aof_last_bgrewrite_status:ok`. Do this before step 6 scales the writers back.

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

7. After the migration checks pass, follow [Local backup cleanup](#local-backup-cleanup).

## Local backup cleanup

Keep the private work directory and its files. This includes `ax-redis-before.rdb`, the inventory files and the saved `ax get task` output. Before the writers resume, keep the copy available for rollback. After the writers resume, fix forward (see "Rollback").

If a check fails or the owner has not accepted the outcome, keep the files. After the migration or rollback checks pass and the owner accepts that outcome, the migration operator must do these steps.

1. In the private work directory, remove the dump, each inventory file and each saved task output file by name.
2. Leave the private work directory.
3. Remove the empty directory. If `rmdir` fails, check which files remain before you remove them.
4. Verify that the directory is absent. The `test` command must exit 0.

   ```bash
   cd "$WORK/.." &&
     rmdir -- "$WORK" &&
     test ! -e "$WORK"
   ```

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
| AC7 live migration and restart check | None | "Migration" steps 1 to 6, then "Restart check" on the live pod |
| AC8 soak | None | After the `ko` apply of ax-controller, watch the controller logs for the `orphan_actors` field of `delete sweep: stuck deletions` |
| ax-89l k0s-agents | None (same `deploy/redis.yaml`) | Run "Pre-check", "Migration" and "Restart check" with the k0s-agents context. The namespace stays `ax-system` |

## Rollback

Use this procedure only before the writers resume.

### Rollback before the writers resume (before Migration step 6)

1. Scale the writers to 0 (they are 0 already).
2. Apply the old `deploy/redis.yaml` with its absolute path. Redis starts with no data.
3. Seed it from `ax-redis-before.rdb` with the same steps as in "Migration" step 4. No write happened after that file, so no data is lost.
4. Wait until Redis answers PING. Check `DBSIZE` and the task inventory against the baseline of Migration step 2. Scale the writers back to 1. Wait for the rollout.
5. Run `ax -a <atespace> get task <name>` for 3 tasks. Each command must succeed.
6. Remove `/tmp/seed` from the pod (Migration step 4). Delete the claim only after steps 4 and 5 pass: `kubectl --context gtr -n ax-system delete pvc ax-redis-data`.
7. After the rollback checks pass, follow [Local backup cleanup](#local-backup-cleanup).

### Do not apply the old manifest after the writers resume

After the writers resume, the old `deploy/redis.yaml` has no persistence. The next restart loses the data again. Do not apply it. Fix forward on the claim `ax-redis-data`. Do not delete the claim. A `git revert` of the pull request changes no live object.

A rollback of ax-controller removes only the `orphan_actors` field.
