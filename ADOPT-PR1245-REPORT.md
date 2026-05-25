# Adopt Upstream PR #1245 — Report

**Branch:** `adopt-pr1245-pullpiece-refactor`  
**Upstream PR:** `filecoin-project/curio/pull/1245` (PullPiece refactor)  
**Date:** 2026-05-25  
**Tracking issue:** `Reiers/curio-core#24` (P1 blocker)

---

## Commits Adopted

| Our SHA (fork) | Upstream SHA | Message |
|---|---|---|
| `4865219d` | `c69751f1` | pullPiece refactor |
| `2505d525` | `5142f800` | account for stale rows in pdp_piece_pull_items for created_at |
| `1896184b` | `3f3d6da8` | fix SQL order |
| `96d0415d` | `e4ea9a16` | Correct order for YB. DDL then DML |

Branch tip: `96d0415d` (= `e4ea9a16` equivalent in fork).

---

## Conflict Resolution Summary

### Commit 1: `c69751f1` pullPiece refactor — 6 files conflicted

#### `pdp/piece_cid.go` (trivial)
Upstream inline-replaced the manual FR32 math in `PadPieceSize` with
`padreader.PaddedSize(uint64(rawSize)).Padded()`. Our fork keeps `pdp/piece_cid.go`
as a forwarding shim to `pdp/piececid/piece_cid.go`, so:

- `pdp/piece_cid.go`: kept our shim unchanged (no imports changed)
- `pdp/piececid/piece_cid.go`: updated `PadPieceSize` to use `padreader.PaddedSize`
  (dropped `math/bits`, added `go-padreader`), preserving the existing
  `if rawSize == 0 { return 0 }` guard

**Judgment call:** The `rawSize == 0` guard was in our implementation already. It
is not present in the upstream's inline version. We kept it in `piececid.PadPieceSize`
because it protects callers that may pass empty pieces.

#### `lib/parkpiece/upsert.go` (structural)
Upstream refactored `UpsertSkip` into a thin wrapper around new
`UpsertSkipWithInserted`, and similarly `upsertFallback` → `upsertFallbackWithInserted`.
The PR also changed the partial-index path from `DO UPDATE SET piece_cid = ...`
(RETURNING-on-conflict) to `DO NOTHING` + a follow-up SELECT.

Resolution:
- Kept `harmonyquery.TxInterface` types throughout (not `*harmonydb.Tx`)
- Applied upstream's structural refactor (thin wrappers + WithInserted functions)
- `DO NOTHING` + follow-up SELECT adopted as-is (Postgres/YB path)
- Preserved SQLite compat in `upsertFallbackWithInserted`: checked
  `pgx.ErrNoRows`, `sql.ErrNoRows`, and the string `"sql: no rows in result set"`
- In `UpsertSkipWithInserted` valid-index path, also preserved
  `sql.ErrNoRows` alongside `pgx.ErrNoRows` for the DO NOTHING empty-RETURNING case

#### `pdp/handlers.go` (cleanup transaction)
Upstream refactored the 5-day cleanup of old pull records into a two-step
transaction: first delete unused `parked_piece_refs` created by PullPiece, then
delete the `pdp_piece_pulls`. Head had a single-statement DELETE.

Resolution:
- Took upstream's transaction logic in full
- Applied seam: `BeginTransaction` → `BeginTransactionI`, `tx.Exec` → `tx.ExecI`,
  callback type `harmonyquery.TxInterface` instead of `*harmonydb.Tx`

#### `pdp/handlers_add.go` (SQL portability)
Conflict at the subPiece CID batch query. Upstream reverted to Postgres-native
`WHERE ppr.piece_cid = ANY($2)`. HEAD had an IN-list expansion for SQLite compat.

Resolution:
- Kept HEAD's portable IN-list expansion (compatible with both Postgres and SQLite)
- Adopted the upstream's `ORDER BY ppr.created_at ASC, ppr.id ASC` from the
  upstream side (deterministic ordering for consistent picks)

#### `pdp/handlers_pull.go` (code removal)
HEAD had a large old implementation block after line 444: `determinePieceStatus`,
the old `dbPullStore`, and related helpers (`GetPieceStatuses`, `GetPullItemStatuses`,
`GetPullPieces`, `MarkPieceFailed`, `CheckTaskExhaustedRetries`).

Upstream removed all of these (the new `dbPullStore` moved to `pull_types.go` with
a completely different design — admission control, backpressure, cleaner status API).

Resolution: took upstream's empty replacement (deleted all old code). The new
`dbPullStore` in `pull_types.go` fully replaces the old one.

#### `pdp/pull_types.go` (auto-merged with seam issues)
The file auto-merged (no conflict markers) but the upstream code uses
`*harmonydb.DB` and `*harmonydb.Tx`. Applied seam transformations in a post-merge
pass:
- `dbPullStore.db *harmonydb.DB` → `harmonyquery.DBInterface`
- `NewDBPullStore(db *harmonydb.DB)` → `harmonyquery.DBInterface`
- `BeginTransaction` → `BeginTransactionI`
- Callback `func(tx *harmonyquery.Tx) (commit bool, err error)` → `func(tx harmonyquery.TxInterface) (bool, error)`
- All `tx.QueryRow/Exec` → `tx.QueryRowI/ExecI`
- All `s.db.Select` → `s.db.SelectI`
- `harmonydb.OptionRetry()` → `harmonyquery.OptionRetry()`
- `enforceBackpressure(tx *harmonydb.Tx, ...)` → `harmonyquery.TxInterface`

The callback return-signature change (`(commit bool, err error)` → `(bool, error)`)
required fixing two `err =` usages inside the callback body to `err :=` to avoid
undefined-variable compiler errors.

#### `tasks/pdpv0/task_pull_piece.go` (full rewrite)
This file went from ~478 lines to ~1070 lines. Auto-merge created a 1264-line file
with 8 conflict blocks. Rather than resolve block by block (which risked missing
seam sites in auto-merged sections), we took the upstream file in its entirety
and applied seam transformations mechanically:

1. Added `"github.com/curiostorage/harmonyquery"` import; removed `harmonydb`
2. `*harmonydb.DB` → `harmonyquery.DBInterface`
3. `db.BeginTransaction` → `db.BeginTransactionI`
4. `db.Select` → `db.SelectI`
5. `db.Exec` → `db.ExecI`
6. `db.QueryRow` → `db.QueryRowI`
7. `tx.Select` → `tx.SelectI`
8. `tx.Exec` → `tx.ExecI`
9. `tx.QueryRow` → `tx.QueryRowI`
10. `*harmonydb.Tx` (all occurrences) → `harmonyquery.TxInterface`
11. `harmonydb.OptionRetry()` → `harmonyquery.OptionRetry()`

This also correctly handled the TF.Val callback:
```go
// upstream:   func(id harmonytask.TaskID, tx *harmonydb.Tx) (bool, error)
// fork:       func(id harmonytask.TaskID, tx harmonyquery.TxInterface) (bool, error)
```

The import for `pdp` is kept (upstream uses `pdp.PullAllowInsecure()` from the
new `pull_types.go` func; our shim `pdp.PadPieceSize` also exports via that package).
Import of `pdp/piececid` (HEAD) was dropped in favour of `pdp` per the upstream.

### Commits 2–4 (SQL-only, no conflicts)
All applied clean. These only touch
`harmony/harmonydb/sql/20260521-pdp-v0-pull-refactor.sql`.

---

## Build + Vet Status

| Package | CGO_ENABLED=0 go vet | CGO_ENABLED=0 go build |
|---|---|---|
| `./lib/parkpiece/...` | ✅ Clean | ✅ Clean |
| `./pdp/piececid/...` | ✅ Clean | ✅ Clean |
| `./pdp/...` | ✅ Clean (no vet errors) | ✅ Clean |
| `./tasks/pdpv0/...` | ✅ Clean | ✅ Clean |
| `./harmony/...` | ✅ Clean | ✅ Clean |

Notes:
- `CGO_ENABLED=1 go build ./pdp/...` and `./tasks/pdpv0/...` fail with
  `extern/filecoin-ffi: no such file` — expected, ffi submodule not initialized.
- `CGO_ENABLED=0` skips the cgo-gated ffi paths (`lib/ffiselect/ffidirect/ffi-direct.go`
  and `lib/paths/local_cgo.go` both have `//go:build cgo`). The Go type checker
  still verifies all seam types in the non-cgo paths.

---

## Branch + Commit SHAs

```
Branch: adopt-pr1245-pullpiece-refactor (pushed to origin)
  96d0415d  ← Correct order for YB. DDL then DML        (= e4ea9a16 upstream)
  1896184b  ← fix SQL order                              (= 3f3d6da8 upstream)
  2505d525  ← account for stale rows in pdp_piece_pull_items for created_at (= 5142f800 upstream)
  4865219d  ← pullPiece refactor                         (= c69751f1 upstream)
  ...
  f116c987  ← Add AGENTS.md (db-seam-refactor base)
```

---

## Follow-on TODO for curio-core

1. **SQLite migration mirror** — `harmony/harmonydb/sql/20260521-pdp-v0-pull-refactor.sql`
   is Postgres/Yugabyte-only (uses `NOW() - INTERVAL '...'`, `DISTINCT ON`, `unnest`,
   `TEXT[]`/`BIGINT[]` array types, etc.). curio-core needs an equivalent SQLite
   migration for the new schema (new columns on `pdp_piece_pull_items`:
   `complete BOOLEAN`, `pull_parked_piece_id BIGINT`, `parked_piece_ref BIGINT`;
   new column on `pdp_piece_pulls`: `client_address TEXT`).

2. **go.mod `replace` update** — After this branch merges to `db-seam-refactor`,
   curio-core's `go.mod` replace directive
   (`github.com/filecoin-project/curio => github.com/Reiers/curio @ db-seam-refactor`)
   will pull in the new `go-padreader` usage. Verify curio-core already has
   `go-padreader` in its own module graph (likely yes, via `go-state-types`).

3. **`PullStore.CreatePullWithPieces` signature change** — Upstream changed the
   return type from `(int64, error)` to `(int64, bool, error)` where the bool is
   the backpressure flag. Any curio-core code calling this interface must be updated.
   The handler `HandlePull` now returns HTTP 503 when backpressure=true (new
   synapse-sdk#799 integration point).

4. **`NewPDPPullPieceTask` signature** — Old: `(ctx, db, storage, max)`.
   New: `(ctx, db, sc)`. curio-core's wiring code needs updating to pass `sc`
   (`*ffi2.SealCalls`) instead of the StashStore.

5. **`pdp.PullAllowInsecure()` export** — New exported function in `pull_types.go`.
   curio-core's pdpv0 handler or test harness may need to call this for CURIO_PULL_ALLOW_INSECURE
   support in testing. Verify test scaffolding still works.

---

## Judgment Calls

- **`handlers_add.go` IN-list vs ANY()**: Kept our portable IN-list. The upstream's
  `= ANY($2)` Postgres-array form is semantically identical on YB. If this is ever
  a hot path, we could switch to ANY() with an explicit cast and note the SQLite
  incompatibility.

- **`upsertFallbackWithInserted` SQLite compat**: Upstream dropped the `sql.ErrNoRows`
  and string-match checks (Postgres-only). We preserved them. This is the correct
  decision for a codebase that uses the same helpers via curio-core/SQLite.

- **`pdp/piececid.PadPieceSize` `rawSize == 0` guard**: Upstream's inline version
  omits the guard (padreader likely handles it). We kept the guard in our piececid
  implementation as defensive programming — it makes the zero-size contract explicit.
