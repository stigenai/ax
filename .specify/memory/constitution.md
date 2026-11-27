# stigenai/ax Fork Constitution

This repository is a fork of [google/ax](https://github.com/google/ax).
The fork runs AX for stigenai. It does not change AX.
Each spec, plan and task in this fork MUST obey these principles.

## Core Principles

### I. Upstream Compatibility First

**Branches:**

- `stigenai/deploy` MUST be an upstream release tag plus a linear stack of fork commits. `FORK.md` gives the workflow.

- Each fork commit MUST hold one change and its tests. It MUST have the `Fork-Topic` and `Fork-Upstream` trailers. `hack/fork/fork.sh check` MUST pass.

- `main` is the same as `google/ax` `main`. It holds only the fork files that GitHub reads from the default branch, for example the `upstream-sync` workflow.

**Sync and wire formats:**

- A sync with upstream MUST be possible at all times without a large rewrite.

- If a change makes the next upstream sync harder, the plan MUST say why the change cannot use a smaller solution.

- Fork changes MUST NOT break upstream wire formats: API types, CRDs, protos, env vars and files that other AX components decode.

- New fields MUST be optional and additive.

- If an older upstream component uses strict decoding for the data, keep the fork field out of that data.

**Rationale:** Each line of fork carry is a merge conflict that we pay for at
each upstream sync.

### II. Smallest Fork Diff

Use the smallest change that works. The order of preference is:

| Order | Change |
|---|---|
| 1 | Configuration, flags or env vars that upstream already reads |
| 2 | New files in fork-only paths (`deploy/stigenai/`, `.specify/`, `.beads/`) |
| 3 | New files in upstream paths |
| 4 | Edits to upstream files; keep each hunk small and local |

- Do not reformat, rename, move or refactor upstream code in the fork.

- Fork behavior that changes upstream defaults MUST be off by default behind a flag or an env var.

- Turn on fork behavior only in `deploy/stigenai/`.

- Each fork commit MUST have one purpose, so that we can drop or upstream it alone.

**Rationale:** Small, isolated hunks rebase cleanly and are easy to donate.

### III. Upstream What Is Concise

**Candidates:**

- If a change is generic, small, self-contained and useful to other AX users, it is an upstream candidate.

- Examples: bug fixes, small flags, test fixes.

**Upstream branches:**

- For an upstream candidate, prepare a branch from `google/ax` `main` that holds only that change, its tests and no stigenai-specific information.

- The upstream branch MUST pass upstream `make build` and `make test`.

- Do not upstream changes that are stigenai-specific or large.

- Do not upstream changes that must have a design discussion with the maintainers.

- Keep them as fork carry, and record why.

**Rationale:** An accepted upstream change removes carry from the fork forever.

### IV. Human Gate for Upstream (NON-NEGOTIABLE)

**Agent actions:**

- Agents MUST NOT push to, open pull requests on, or comment on `google/ax`.

- Agents can prepare the upstream branch and a draft PR description.

- Then they MUST stop and flag a human with `bd human <id>`.

**Human actions:**

- A human reads the full diff, makes sure that the CLA covers it, and sends the PR.

- A human also answers all comments from the maintainers.

- Upstream PRs MUST NOT contain stigenai hostnames, IPs, secrets, internal bead IDs, or URLs of private repos.

**Rationale:** Upstream PRs are public and go out under a person's name and
CLA. An agent error there is public and permanent.

### V. Carry Ledger

- Each fork carry MUST have a bead with the label `carry`.

- The bead records the reason for the carry and one status: `fork-only`, `upstream-candidate`, `upstream-proposed` (with the PR URL) or `upstream-merged`.

- When upstream merges an equivalent change, drop the fork carry at the next sync and close the bead.

- Review the ledger at each upstream sync.

- Find carry that upstream now makes unnecessary, and drop it.

- `FORK.md` at the repository root MUST have one entry for each `carry` bead.
- Each entry MUST give the bead, the upstream files that the carry edits, the new files, the tests, the reason, the status and the removal condition.
- A PR that adds, changes or drops carry MUST update `FORK.md` and the bead in the same PR.
- A carry that edits upstream code MUST name at least one test in its entry. These tests MUST pass after each sync.
- The `Fork-Topic` trailer of each commit MUST name the bead of its carry, for example `Fork-Topic: ax-cyt`. The `Fork-Upstream` trailer is `candidate` (bead `upstream-candidate`), `fork-only` or `test-only`. A `test-only` commit holds a test that checks upstream behavior that we need.

**Rationale:** Without a ledger, carry stays in the fork after nobody needs it.

## Upstream Sync

**Sync steps:**

- Rebase the stack onto each new upstream release tag with `hack/fork/fork.sh rebase <tag>`. Do not merge upstream into `stigenai/deploy`.
- Do the rebase on a `stigenai/rebase-<tag>` branch. Open a PR to `stigenai/deploy` with the `git range-diff` output in its description.
- After a human approves the PR, force-push the rebased head to `stigenai/deploy` with `--force-with-lease`. Then tag it `<tag>-stigenai.<n>`. Do not merge the PR. Only this step can rewrite `stigenai/deploy`.
- After each rebase, commit `hack/fork/fork.sh export` to the `stigenai/patches` branch.
- The `upstream-sync` workflow on `main` does a trial rebase each week onto the most recent upstream tag. It also runs the tests. It fails when the stack is not on that tag, or when the rebase or a test fails.
- Fetch `google/ax` `main` with the release tags every two weeks, or more often. Merge `main` forward to `google/ax` `main`.

**Conflicts and checks:**

- The person who does the rebase resolves the conflicts.

- If upstream already has the change, drop the commit with `git rebase --skip`. If its tests pass on the upstream code, keep them in a `test-only` commit.

- If a carry conflicts on two syncs in sequence, examine it again. Then send it upstream, move it to new files, or drop it.

- After a rebase, `make build`, `make test` and `hack/fork/fork.sh check` MUST pass before a deploy.

## Development Workflow

- Use speckit for feature work (`/speckit-specify`, `/speckit-plan`, `/speckit-tasks`, `/speckit-implement`).

- Use bd (beads) for all task tracking.

Each plan MUST include a "Fork impact" section with these items:

| No. | Item |
|---|---|
| 1 | The upstream files that the change edits, and the number of lines |
| 2 | The upstream classification: `fork-only` or `upstream-candidate`, with a reason |
| 3 | Proof that fork behavior is off by default, or a reason why it cannot be |

- Each change MUST pass `make build` and `make test`.

- New behavior MUST have tests.

- Keep upstream code style.

- Run `gofmt` on touched Go files.

## Governance

- This constitution has priority over other practices in this fork.

- To change this constitution, open a PR on `stigenai/deploy` that changes this file.

- A human MUST approve the PR.

Versions use semantic versioning. Use the version parts in this table:

| Part | Meaning |
|---|---|
| MAJOR | Remove or redefine a principle |
| MINOR | Add a principle or section |
| PATCH | Wording only |

**Review checks:**

- Reviews of fork PRs MUST check Principles I, II and V.

- Reviews of upstream branches MUST also check Principles III and IV.

**Version**: 2.0.0 | **Ratified**: 2026-11-07 | **Last Amended**: 2026-11-26

**2.0.0:** `stigenai/deploy` changes from a merged branch to a patch stack on an upstream release tag. This is the same change as `stigenai/substrate` 2.0.0. The old merged history stays until the `v0.4.0` stack (bead `ax-mvu`) replaces it. Until then, `hack/fork/fork.sh check` fails on the old commits.
