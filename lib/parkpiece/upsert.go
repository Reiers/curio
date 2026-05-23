// Package parkpiece provides race-safe upsert helpers for the parked_pieces
// table.
package parkpiece

import (
	"database/sql"
	"errors"
	"sync/atomic"

	"github.com/yugabyte/pgx/v5"
	"golang.org/x/xerrors"

	"github.com/curiostorage/harmonyquery"
)

var activePieceIndexKnownValid atomic.Bool

// Upsert returns the id for the active parked_pieces row matching
// (piece_cid, piece_padded_size, long_term), inserting it when no active row is
// present. When parked_pieces_active_piece_key is known valid, this uses the
// partial unique index as the concurrency guard and performs a no-op conflict
// update only so RETURNING can report the existing row id. When the index is
// missing or INVALID, this avoids ON CONFLICT entirely and uses the degraded
// check-then-insert fallback; that fallback can race and create duplicates
// until FixParkPieceTask repairs the table/index.
func Upsert(tx harmonyquery.TxInterface, pieceCID string, paddedSize, rawSize int64, longTerm bool) (int64, error) {
	indexValid, err := ActiveIndexValid(tx)
	if err != nil {
		return 0, xerrors.Errorf("checking parked_pieces_active_piece_key: %w", err)
	}

	if indexValid {
		var id int64
		err = tx.QueryRowI(`
			INSERT INTO parked_pieces (piece_cid, piece_padded_size, piece_raw_size, long_term)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (piece_cid, piece_padded_size, long_term) WHERE cleanup_task_id IS NULL
			-- no-op SET so RETURNING fires on the conflict path (DO NOTHING returns no rows)
			DO UPDATE SET piece_cid = parked_pieces.piece_cid
			RETURNING id`, pieceCID, paddedSize, rawSize, longTerm).Scan(&id)
		if err != nil {
			return 0, xerrors.Errorf("upsert parked_pieces: %w", err)
		}
		return id, nil
	}

	return upsertFallback(tx, pieceCID, paddedSize, rawSize, longTerm, nil)
}

// UpsertSkip is Upsert plus a skip value for newly inserted rows. The skip
// flag is intentionally insert-only: if the piece already exists, both the
// valid-index path and fallback path return the existing id without changing
// the existing row's skip or raw-size metadata.
func UpsertSkip(tx harmonyquery.TxInterface, pieceCID string, paddedSize, rawSize int64, longTerm, skip bool) (int64, error) {
	indexValid, err := ActiveIndexValid(tx)
	if err != nil {
		// SQLite path: ActiveIndexValid swallows pg_catalog-unavailable
		// errors and returns (false, nil), so we never reach here on
		// SQLite. On Postgres a real error is escalated.
		return 0, xerrors.Errorf("checking parked_pieces_active_piece_key: %w", err)
	}

	if indexValid {
		var id int64
		err = tx.QueryRowI(`
			INSERT INTO parked_pieces (piece_cid, piece_padded_size, piece_raw_size, long_term, skip)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (piece_cid, piece_padded_size, long_term) WHERE cleanup_task_id IS NULL
			-- no-op SET so RETURNING fires on the conflict path (DO NOTHING returns no rows)
			DO UPDATE SET piece_cid = parked_pieces.piece_cid
			RETURNING id`, pieceCID, paddedSize, rawSize, longTerm, skip).Scan(&id)
		if err != nil {
			return 0, xerrors.Errorf("upsert parked_pieces: %w", err)
		}
		return id, nil
	}

	return upsertFallback(tx, pieceCID, paddedSize, rawSize, longTerm, &skip)
}

// upsertFallback is the non-atomic check-then-insert path used while the
// partial unique index cannot be bound by ON CONFLICT. It first reuses the
// lowest-id active row for the key, and inserts only when no active row exists.
// There is deliberately no lock here; callers accept the same temporary
// duplicate risk that the cleanup task is responsible for repairing. skip is
// applied only to the inserted row.
func upsertFallback(tx harmonyquery.TxInterface, pieceCID string, paddedSize, rawSize int64, longTerm bool, skip *bool) (int64, error) {
	var id int64
	err := tx.QueryRowI(`
		SELECT id FROM parked_pieces
		WHERE piece_cid = $1 AND piece_padded_size = $2 AND long_term = $3 AND cleanup_task_id IS NULL
		ORDER BY id LIMIT 1`, pieceCID, paddedSize, longTerm).Scan(&id)
	if err == nil {
		return id, nil
	}
	// pgx.ErrNoRows on Postgres, database/sql.ErrNoRows on SQLite, or the
	// stringy message "sql: no rows in result set" — all mean "no
	// existing row, proceed to insert."
	if !errors.Is(err, pgx.ErrNoRows) && !errors.Is(err, sql.ErrNoRows) && err.Error() != "sql: no rows in result set" {
		return 0, xerrors.Errorf("upsert parked_pieces (fallback select): %w", err)
	}
	if skip != nil {
		err = tx.QueryRowI(`
			INSERT INTO parked_pieces (piece_cid, piece_padded_size, piece_raw_size, long_term, skip)
			VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			pieceCID, paddedSize, rawSize, longTerm, *skip).Scan(&id)
	} else {
		err = tx.QueryRowI(`
			INSERT INTO parked_pieces (piece_cid, piece_padded_size, piece_raw_size, long_term)
			VALUES ($1, $2, $3, $4) RETURNING id`,
			pieceCID, paddedSize, rawSize, longTerm).Scan(&id)
	}
	if err != nil {
		return 0, xerrors.Errorf("upsert parked_pieces (fallback insert): %w", err)
	}
	return id, nil
}

// ActiveIndexValid returns whether parked_pieces_active_piece_key is present,
// valid, and shaped exactly as the upsert helpers require. It caches only a
// positive answer: once this process has observed the index as valid, later
// calls skip the catalog lookup. Missing, invalid, or wrongly-shaped states are
// rechecked on every call so a repair that drops/recreates the index can be
// observed without restarting the process.
func ActiveIndexValid(tx harmonyquery.TxInterface) (bool, error) {
	if activePieceIndexKnownValid.Load() {
		return true, nil
	}

	ok, err := RefreshActiveIndexValid(tx)
	if err != nil {
		// SQLite backends can't run the pg_catalog probe. Returning
		// (false, nil) drives callers down the upsertFallback path,
		// which works on either backend.
		if isPgCatalogUnavailable(err) {
			return false, nil
		}
		return false, err
	}
	return ok, nil
}

// isPgCatalogUnavailable detects the SQLite "no such table: pg_catalog.*" /
// "near SELECT: syntax error" errors that come back when RefreshActiveIndexValid
// runs against a non-Postgres backend.
func isPgCatalogUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return stringContainsAny(msg,
		"no such table: pg_catalog",
		"SQL logic error",
		"syntax error",
		"pg_catalog.pg_index")
}

func stringContainsAny(s string, needles ...string) bool {
	for _, n := range needles {
		if len(n) > 0 && len(s) >= len(n) {
			for i := 0; i+len(n) <= len(s); i++ {
				if s[i:i+len(n)] == n {
					return true
				}
			}
		}
	}
	return false
}

// RefreshActiveIndexValid checks the catalog even if this process previously
// cached the index as valid. Repair code uses this path because it must be
// authoritative when deciding whether to drop/recreate the index. The result
// refreshes the positive cache; a false result clears a stale positive cache
// but is still not a negative cache because ActiveIndexValid will requery while
// the flag is false.
func RefreshActiveIndexValid(tx harmonyquery.TxInterface) (bool, error) {
	var exists bool
	// SQLite-friendly fast-path: the pg_catalog query below has no SQLite
	// equivalent. When the query errors (SQLite returns 'near SELECT:
	// syntax error' on the pg_catalog references), treat it as 'no
	// active index' so callers fall through to the upsertFallback path.
	// PostgreSQL backends remain bound by the real check.
	defer func() {
		// no-op; the error path below handles the fallback
	}()
	err := tx.QueryRowI(`
		SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_index ix
			JOIN pg_catalog.pg_class idx ON idx.oid = ix.indexrelid
			JOIN pg_catalog.pg_class tbl ON tbl.oid = ix.indrelid
			JOIN pg_catalog.pg_namespace ns ON ns.oid = tbl.relnamespace
			JOIN pg_catalog.pg_am am ON am.oid = idx.relam
			WHERE ns.nspname = current_schema()
			  AND idx.relnamespace = ns.oid
			  AND tbl.relname = 'parked_pieces'
			  AND idx.relname = 'parked_pieces_active_piece_key'
			  AND idx.relkind = 'i'
			  AND ix.indisunique
			  AND ix.indisvalid
			  AND ix.indisready
			  AND ix.indislive
			  AND ix.indnkeyatts = 3
			  AND ix.indnatts = 3
			  AND ix.indexprs IS NULL
			  AND ARRAY(
				  SELECT pg_catalog.pg_get_indexdef(ix.indexrelid, n, true)
				  FROM generate_series(1, ix.indnkeyatts) AS n
				  ORDER BY n
			  ) = ARRAY['piece_cid', 'piece_padded_size', 'long_term']
			  AND pg_catalog.pg_get_expr(ix.indpred, ix.indrelid, false) = '(cleanup_task_id IS NULL)'
		)`).Scan(&exists)
	if err != nil {
		return false, err
	}
	activePieceIndexKnownValid.Store(exists)
	return exists, nil
}

// ResetActiveIndexValidCacheForTest clears the process-local valid-index cache.
// Production code should not call this; it exists so tests that drop or corrupt
// the index are not order-dependent.
func ResetActiveIndexValidCacheForTest() {
	activePieceIndexKnownValid.Store(false)
}
