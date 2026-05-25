# AGENTS.md — curio-fork

> **For future-Capri (and any other agent picking this repo up):**
> Read this before you `git checkout` anything. The branch topology is non-obvious.

## What this repo is

`Reiers/curio` (this fork) holds our patches on top of upstream `filecoin-project/curio`. We use it for two things:

1. **The binary that runs on `sp.reiers.io`** (mainnet f03678816). Built from `db-seam-refactor`.
2. **The Curio source that `Reiers/curio-core` consumes as a Go module** via `replace github.com/filecoin-project/curio => github.com/Reiers/curio @ db-seam-refactor` in curio-core's go.mod.

Both consumers track **`db-seam-refactor`**, not `main`.

## Remote layout

- `origin` → `https://github.com/Reiers/curio.git` (our fork, push enabled)
- `upstream` → `filecoin-project/curio` (push **DISABLED** via `git remote set-url --push upstream "DISABLED-no-pushing-to-filecoin-project-curio"`)
- Upstream fetch refspec includes `refs/heads/main` and `refs/heads/integ/task`. Adding more upstream branches: `git config --add remote.upstream.fetch "+refs/heads/BRANCH:refs/remotes/upstream/BRANCH"`.

## Branch layout

| Branch | Purpose | Base | Status |
|---|---|---|---|
| **`db-seam-refactor`** | **THE working branch.** All real work lands here. Carries: DBInterface / TxInterface seam (harmonyquery side), `*harmonydb.DB` → `harmonyquery.DBInterface` flip in `harmonytask`, SQLite-portability patches (`::bigint` → `CAST(... AS BIGINT)`, etc.), PDP v3.4.0 cleanup-deposit patches, prove-task hardening (MaxFailures=10, 25-min retry budget). | upstream/main, merged regularly | **active — work here** |
| `main` | Mirror of `upstream/main`. **Do not commit to this directly.** Fast-forwarded from `upstream/main` periodically. | upstream/main | mirror only |
| `upstream-pr-canaccept-warn` | The closed-upstream WARN diagnostic with the rate-limit-gated COUNT fix on top. Kept for our own ops use. | `db-seam-refactor` base | parked |
| `integ/task` | Old integration branch. Probably stale. Check before touching. | unknown | unknown |
| `pr-1239` | Adoption of upstream PR #1239 (deletion pipeline hardening). Already merged into `db-seam-refactor` as commit `050f9726`. | — | merged, can delete |

## Critical: do not work on `main`

Before today (2026-05-25), `main` was a Feb 2026 snapshot (`a7dae229`, ~3 months behind upstream). It looked like the working branch but wasn't. Cherry-picking upstream PRs onto `main` produced huge conflict sets because main was missing entire feature surfaces (all of `tasks/pdpv0/`, `lib/parkpiece/`, `pdp/handlers_*`).

Fixed today: main was fast-forwarded to upstream HEAD (`05d36498`). New rule:

- **Real work → `db-seam-refactor`**
- **`main` is mirror-only.** Fast-forward from upstream/main; don't commit to it.
- **Sync cadence:** merge `upstream/main` into `db-seam-refactor` weekly, or before adopting any upstream PR.

## Adopting an upstream PR (the canonical recipe)

```bash
# 1. Make sure upstream/main is fresh
git fetch upstream main
git checkout main
git merge --ff-only upstream/main
git push origin main

# 2. Make sure db-seam-refactor is caught up to upstream/main
git checkout db-seam-refactor
git merge upstream/main --no-ff -m "Merge upstream/main into db-seam-refactor (catch up)"
# resolve any conflicts — typically tiny, mostly SelectI vs Select calls
git push origin db-seam-refactor

# 3. Fetch the PR head
git fetch upstream pull/<NNNN>/head:upstream-pr-<NNNN>

# 4. Cherry-pick the PR commits onto a topic branch off db-seam-refactor
git checkout -b adopt-pr<NNNN>-<short-name> db-seam-refactor
git cherry-pick <commit1> <commit2> ...
# resolve conflicts: typically Postgres-only SQL needs SQLite mirror in curio-core,
# but the Postgres SQL stays here as-is in curio-fork
```

## Critical: never push to upstream

`git remote set-url --push upstream "DISABLED-no-pushing-to-filecoin-project-curio"` is already set. Defense in depth against the May-23 near-miss where a branch was accidentally pushed to filecoin-project/curio for ~30 seconds. See workspace `TOOLS.md` for the full story.

If you ever need to undo the disable (don't): `git remote set-url --push upstream https://github.com/filecoin-project/curio.git`. Don't.

## Live downstream consumers

- **sp.reiers.io** (mainnet f03678816) — runs `curio` binary built from `db-seam-refactor`. Restart procedure in workspace `TOOLS.md` under "Curio on sp.reiers.io".
- **cc-smoke** (Hetzner calibration, `/usr/local/bin/curio-core`) — runs `curio-core`, which `replace`s github.com/filecoin-project/curio with this fork's `db-seam-refactor`. Updating the fork = restart cc-smoke after rebuilding curio-core.

## Why `db-seam-refactor` and not a branch with a cleaner name

The branch started as the DB-seam refactor in May 2026 and grew into the actual production branch. Renaming it now would break `replace` directives in curio-core's go.mod and require coordinated commits. Not worth it. Treat `db-seam-refactor` as the de facto default branch in this fork.

---

_Last updated: 2026-05-25, after fast-forwarding `main` from Feb→May and merging the 3 missing upstream commits into `db-seam-refactor`._
