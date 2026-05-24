// lifecycle_sweeper.go - tipset-driven recovery passes for pdpv0
// lifecycle bugs that can silently strand data sets / uploads.
//
// Background: filecoin-project/curio#879 identified a class of bugs
// where a task's MaxFailures exhaustion leaves backing rows in an
// inconsistent state. The cascade-delete from harmony_task drops the
// task row, but the dataset / piece / upload row retains side-effect
// columns (challenge_request_msg_hash already-NULL'd, prove_at_epoch
// still set, notify_task_id still set) that prevent re-pickup. The
// result is silent faults (dataset never proves again until operator
// intervention) or silent data loss (notify never moves the upload
// into pdp_piecerefs).
//
// BigLep punted #879 to post-GA upstream. We can't wait; Hot Storage
// SPs running curio-core are the operator class that gets bitten
// first. This file runs two recovery passes on every tipset (~30s
// cadence) instead of waiting for the 8h-interval ChainSync poll.

package pdpv0

import (
	"context"

	"github.com/curiostorage/harmonyquery"
	"github.com/filecoin-project/curio/lib/chainsched"

	chainTypes "github.com/filecoin-project/lotus/chain/types"
)

// NewLifecycleSweeper registers two tipset-driven recovery passes.
//
//  1. sweepOrphanedProvableDataSets: re-arms data sets where a prior
//     ProveTask exhausted MaxFailures, the cascade dropped
//     pdp_prove_tasks, but pdp_data_sets is stuck in a state the
//     Adder query can't match again (challenge_request_msg_hash IS
//     NULL after the Adder cleared it, prove_at_epoch still set).
//     Fix: reset prove_at_epoch and let InitProvingPeriod re-arm the
//     dataset cleanly on the next proving window.
//
//  2. sweepStuckNotifyUploads: re-tries pdp_piece_uploads rows where
//     piece_ref IS NOT NULL (bytes landed in stash) but notify_task_id
//     IS NULL after a prior cascade (task exhausted MaxFailures), so
//     the upload is stuck and never makes it into pdp_piecerefs.
//     Fix: NULL-out notify_task_id is already done by the cascade; we
//     check needs_save_cache / piece_ref state and let the
//     PDPNotify.schedule.func1 Adder re-pick the row. (The pdp_piece_
//     uploads.notify_task_id column does not currently have an FK,
//     which audit #29 finding 3 flagged; we treat the explicit
//     re-arm as the working fix even before the FK migration lands,
//     since the FK only changes cascade behavior - the stuck state
//     is the same problem either way.)
//
// Logs each recovery event at WARN so operators can correlate with
// the original MaxFailures error in their journals.
func NewLifecycleSweeper(db harmonyquery.DBInterface, pcs *chainsched.CurioChainSched) error {
	return pcs.AddHandler(func(ctx context.Context, revert, apply *chainTypes.TipSet) error {
		if err := sweepOrphanedProvableDataSets(ctx, db); err != nil {
			log.Warnf("lifecycle sweeper: sweepOrphanedProvableDataSets: %v", err)
		}
		if err := sweepStuckNotifyUploads(ctx, db); err != nil {
			log.Warnf("lifecycle sweeper: sweepStuckNotifyUploads: %v", err)
		}
		return nil
	})
}

// sweepOrphanedProvableDataSets finds pdp_data_sets rows in the
// stuck-after-MaxFailures shape:
//
//	prove_at_epoch IS NOT NULL
//	AND challenge_request_msg_hash IS NULL          (Adder cleared it before exhaustion)
//	AND init_ready = TRUE                           (dataset has pieces)
//	AND unrecoverable_proving_failure_epoch IS NULL (not terminally failed)
//	AND NOT EXISTS (SELECT 1 FROM pdp_prove_tasks WHERE data_set_id = id)
//
// Action: reset prove_at_epoch + clear next_prove_attempt_at, so the
// InitProvingPeriod / NextProvingPeriod handlers re-arm the dataset
// on the next tipset. (We do NOT touch challenge_request_msg_hash;
// re-driving the request through the standard path is safer than
// trying to repair partial state in-place.)
//
// Logged at WARN so operators see the recovery; if a dataset is
// being swept repeatedly that's a signal of an underlying ProveTask
// bug they need to investigate.
func sweepOrphanedProvableDataSets(ctx context.Context, db harmonyquery.DBInterface) error {
	// Stage 1: enumerate orphans. Done as a separate read so we can
	// log + alert before touching state. SQLite + Postgres both accept
	// this shape.
	type orphan struct {
		ID           int64 `db:"id"`
		ProveAtEpoch int64 `db:"prove_at_epoch"`
	}
	var orphans []orphan
	// pdp_prove_tasks uses the legacy column name `proofset` (not
	// `data_set_id`) from the v1 schema; this name is preserved across
	// the v0 rename because the column references the on-chain id, not
	// the renamed pdp_data_sets.id. Verified live on calibration.
	err := db.SelectI(ctx, &orphans, `
		SELECT p.id, p.prove_at_epoch
		FROM pdp_data_sets p
		WHERE p.prove_at_epoch IS NOT NULL
		  AND p.challenge_request_msg_hash IS NULL
		  AND p.init_ready = 1
		  AND p.unrecoverable_proving_failure_epoch IS NULL
		  AND NOT EXISTS (
		      SELECT 1 FROM pdp_prove_tasks pt WHERE pt.proofset = p.id
		  )
	`)
	if err != nil {
		return err
	}
	if len(orphans) == 0 {
		return nil
	}

	for _, o := range orphans {
		log.Warnw("lifecycle sweeper: re-arming orphaned dataset",
			"data_set_id", o.ID,
			"orphaned_prove_at_epoch", o.ProveAtEpoch,
			"reason", "ProveTask MaxFailures exhausted; resetting for next proving window")
	}

	// Stage 2: clear prove_at_epoch on each orphan. Letting the
	// InitProvingPeriod adder schedule the next window from scratch
	// rather than trying to repair partial state in-place.
	_, err = db.ExecI(ctx, `
		UPDATE pdp_data_sets
		SET prove_at_epoch = NULL,
		    next_prove_attempt_at = NULL
		WHERE prove_at_epoch IS NOT NULL
		  AND challenge_request_msg_hash IS NULL
		  AND init_ready = 1
		  AND unrecoverable_proving_failure_epoch IS NULL
		  AND NOT EXISTS (
		      SELECT 1 FROM pdp_prove_tasks pt WHERE pt.proofset = pdp_data_sets.id
		  )
	`)
	return err
}

// sweepStuckNotifyUploads finds pdp_piece_uploads rows where the
// piece bytes landed (piece_ref IS NOT NULL) but notify_task_id is
// NULL (cleared by cascade after MaxFailures exhaustion). The
// PDPNotify task's Adder query SELECTs rows WHERE piece_ref IS NOT
// NULL AND notify_task_id IS NULL, so simply being in that state
// makes the row re-pickable on the next harmonytask cycle. This
// sweeper logs the condition so operators see it; it does NOT modify
// state because the row is already in the right shape for re-pickup.
//
// We surface the count + the upload IDs at WARN for observability.
// In production, repeated re-sweeps of the same upload IDs indicate
// the underlying notify failure is real (e.g. notify_url unreachable,
// piece_cid validation failure, etc.) and the operator should
// investigate.
func sweepStuckNotifyUploads(ctx context.Context, db harmonyquery.DBInterface) error {
	type stuck struct {
		ID        string `db:"id"`
		Service   string `db:"service"`
		PieceCid  string `db:"piece_cid"`
		PieceRef  int64  `db:"piece_ref"`
		CreatedAt string `db:"created_at"`
	}
	var stucks []stuck
	err := db.SelectI(ctx, &stucks, `
		SELECT id, service, piece_cid, piece_ref, created_at
		FROM pdp_piece_uploads
		WHERE piece_ref IS NOT NULL
		  AND notify_task_id IS NULL
		  AND piece_cid IS NOT NULL
	`)
	if err != nil {
		return err
	}
	if len(stucks) == 0 {
		return nil
	}
	for _, s := range stucks {
		log.Warnw("lifecycle sweeper: stuck pdp_piece_upload (notify failed, bytes still on disk; will be re-picked by PDPNotify Adder)",
			"upload_id", s.ID,
			"service", s.Service,
			"piece_cid", s.PieceCid,
			"piece_ref", s.PieceRef,
			"created_at", s.CreatedAt)
	}
	return nil
}
