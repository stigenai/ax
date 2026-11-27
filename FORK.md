# The stigenai fork

This repository is a fork of [google/ax](https://github.com/google/ax).
We keep our changes as a **patch stack**. The stack is one upstream release tag, with a linear series of our commits on top.
`stigenai/substrate` uses the same model.

| Item | Value |
|---|---|
| Fork branch | `stigenai/deploy` |
| Base | An upstream release tag. `hack/fork/fork.sh base` prints it. |
| Upstream remote | `upstream` = `https://github.com/google/ax` |
| Fork release tags | `<base>-stigenai.<n>`, for example `v0.4.0-stigenai.1` |
| Tools | `hack/fork/fork.sh` |

> **Note:** The stack starts at the `v0.4.0` rebuild (bead `ax-mvu`).
> The commits on `v0.3.1` are merges. They have no trailers, so `fork.sh check` fails on them.

## Rules for fork commits

1. Each commit is one change with its tests. Put a Go test in the package that it tests. Put the test in the same commit as the change.
2. Each commit has two trailers. `hack/fork/fork.sh check` fails without them.

   ```text
   Fork-Topic: ax-cyt
   Fork-Upstream: candidate
   ```

   | `Fork-Upstream` | Meaning |
   |---|---|
   | `candidate` | General fix or feature. Send it upstream as a PR, then drop it here. |
   | `fork-only` | stigenai-specific: our fleet, our deploy, our product. |
   | `test-only` | A test that guards upstream behavior we depend on. |

   The value of `Fork-Topic` is the bead of the carry. Each carry below names its bead.
3. Keep the commits of one topic together in the stack.
4. Add new files when you can. Change an upstream file only when you must. A new file cannot cause a conflict on a rebase.
5. Do not merge upstream into `stigenai/deploy`. Rebase the stack onto a new tag.

To add the trailers to a commit:

```bash
git commit --amend --no-edit --trailer 'Fork-Topic: ax-cyt' --trailer 'Fork-Upstream: candidate'
```

## Show the stack

```bash
hack/fork/fork.sh base     # for example v0.4.0
hack/fork/fork.sh series   # hash, topic, upstream status, subject
hack/fork/fork.sh check
```

## Move to a new upstream release

1. Get the new tags. Run `git fetch upstream --tags`.
2. Make a branch, then replay the stack. The script turns on `rerere`. Git then uses each conflict resolution again.

   ```bash
   git switch -c stigenai/rebase-v0.4.1 stigenai/deploy
   hack/fork/fork.sh rebase v0.4.1
   ```

3. If a commit conflicts:
   - Resolve it, run `git add`, then run `git rebase --continue`.
   - If upstream already has the change, run `git rebase --skip`.
   - If the commit's tests pass on the upstream code, keep them in a `test-only` commit.
4. If a commit changes `pkg/apis/v1alpha1/ax.proto`, regenerate `ax.pb.go` after you resolve it. Do not resolve conflicts in `.pb.go` files by hand.
   If a commit changes `go.mod`, run `go mod tidy`. Regenerate the Bazel files with gazelle.
5. Compare the old and the new stack. The script prints the command: `git range-diff v0.4.0..<old-head> v0.4.1..HEAD`.
6. Run the checks: `make build && make test && hack/fork/fork.sh check`.
7. Push the branch and open a PR to `stigenai/deploy`. Put the `range-diff` output in the PR description. Do not merge the PR.
   After a human approves it, force-push and tag:

   ```bash
   git push --force-with-lease origin stigenai/rebase-v0.4.1:stigenai/deploy
   git tag v0.4.1-stigenai.1 && git push origin v0.4.1-stigenai.1
   ```

8. Run `hack/fork/fork.sh export ../ax-patches`.
   Then commit the result to the `stigenai/patches` branch.
   The history of that branch shows how each patch changed from one base to the next.

## Send a candidate upstream

1. Make a branch from `upstream/main`. Cherry-pick the topic commits on that branch.
2. Remove the trailers. Follow the upstream `CONTRIBUTING.md`.
3. A human sends the PR (constitution Principle IV).
4. Set the bead status to `upstream-proposed`, and add the PR URL.
   Then set the bead status to `upstream-merged`.
   The next rebase drops our commit.

## Carry register

Principle V in `.specify/memory/constitution.md` requires this register.
Each entry has a `carry` bead. The bead and this file MUST agree.
After each rebase, the tests of each entry MUST pass.

Entries are in stack order, the same order as the commits.

## Fork process files (constitution, FORK.md, fork.sh, upstream-sync workflow)

**Bead:** `ax-9e2` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- none

**New files:**

- `.github/workflows/upstream-sync.yml` (on `main`)
- `hack/fork/fork.sh`
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

**Reason:** The fork rules, the carry list and the weekly trial rebase exist only for the fork.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Beads and agent tool files

**Bead:** `ax-not` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `.gitignore`

**New files:**

- `CLAUDE.md` (imports the upstream `AGENTS.md`, then adds the beads block)
- `skills/beads/SKILL.md` (`.agents/skills` is the upstream link to `skills/`)
- `skills/beads/agents/openai.yaml`
- `.beads/` (4 files)
- `.claude/settings.json`
- `.codex/config.toml`
- `.codex/hooks.json`
- `.cursor/hooks.json`
- `.cursor/rules/beads.mdc`

**Tests:**

- none

**Reason:** The fork tracks work with bd; upstream does not. The fork does not edit the upstream `AGENTS.md`.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## Bazel build with BuildBuddy

**Bead:** `ax-k2q` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `.gitignore`

**New files:**

- `.bazelrc`
- `.bazelversion`
- `BUILD.bazel`
- `MODULE.bazel`
- `MODULE.bazel.lock`
- `.github/workflows/bazel.yml`
- `BUILD.bazel` in each Go package (made by `bazel run //:gazelle`)

**Tests:**

- `.github/workflows/bazel.yml`: the Go test packages equal the Bazel `go_test` targets, gazelle has no drift, and the Postgres store tests run with no skip.

**Notes:**

- `//cmd/ax:ax_test` is `manual`. The test runs `go build .`, and the Bazel sandbox has no go tool. `deploy-ci.yml` runs it with `go test`.
- `//internal/install/substrate:substrate_test` and `//internal/install/cluster:cluster_test` get `go.mod` and the Substrate manifests as `data`, because they read them by path.

**Reason:** Upstream builds with go and make, not Bazel.

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

**Notes:** The image builds `FROM` the upstream `ax-runner:v0.4.0-rc.2` image, pinned by digest. Before v0.4.0 it used `ax-task-runner:v0.3.1-stigenai.1`. Test it against a v0.4.0 `ax-server` before the cutover (`ax-mvu.4`).

**Reason:** stigenai task image in images/lap-pi-task; new files and a workflow only.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.

## ax stdio: local stdin, live_output, stream-reset resume

**Bead:** `ax-eah` · **Branch:** `stigenai/deploy` · **Upstream status:** `fork-only`

**Upstream files edited:**

- `cmd/ax/main.go`
- `go.mod`
- `go.sum`
- `internal/guest/client.go`

**New files:**

- `internal/guest/client_stdio_test.go`

**Tests:**

- `internal/guest/client_stdio_test.go`

**Notes:**

- Upstream #413 forwards stdin and Ctrl-C in `ax ssh`. This carry keeps that and adds the `ax stdio` command, a 30-day guest timeout, `live_output`, resume of the output stream after the 300 s router reset, rotation of idle stdin, and a stop of the remote process on SIGHUP.
- `go.mod` replaces `github.com/agent-substrate/env` with `github.com/stigenai/env` at `45314510bec4` (`live_output`, stigenai/env#1). Upstream env has no `live_output`.

**Reason:** Factory workers use `ax stdio` as an ssh ProxyCommand for long sessions.

**Removal condition:** Remove the carry when stigenai stops using it, or when upstream adds the same behavior.
