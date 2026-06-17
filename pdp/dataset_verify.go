package pdp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/yugabyte/pgx/v5"

	"github.com/curiostorage/harmonyquery"
	"github.com/filecoin-project/curio/harmony/harmonydb"
)

var (
	// ErrDataSetNotFound indicates the data set does not exist or does not belong to the service.
	ErrDataSetNotFound = errors.New("data set not found")
	// ErrDataSetTerminated indicates the data set was terminated due to unrecoverable proving failure.
	ErrDataSetTerminated = errors.New("data set has been terminated due to unrecoverable proving failure")
)

// verifyDataSetForService checks that dataSetId exists in pdp_data_sets, belongs to service,
// and has not been terminated due to unrecoverable proving failure.
func verifyDataSetForService(ctx context.Context, db harmonyquery.DBInterface, service string, dataSetId uint64) error {
	var dataSetService string
	var unrecoverable *int64
	err := db.QueryRowI(ctx, `
		SELECT service, unrecoverable_proving_failure_epoch
		FROM pdp_data_sets
		WHERE id = $1
	`, dataSetId).Scan(&dataSetService, &unrecoverable)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrDataSetNotFound
		}
		return fmt.Errorf("failed to retrieve data set: %w", err)
	}

	if dataSetService != service {
		return ErrDataSetNotFound
	}

	if unrecoverable != nil {
		return ErrDataSetTerminated
	}

	return nil
}

// discardOrphanPiecrefsForSubPieces removes unreferenced pdp_piecerefs for the
// given subPiece CIDs. Called when addPieces is rejected for a missing or
// terminated data set, so notify-created piecerefs do not linger.
func discardOrphanPiecrefsForSubPieces(ctx context.Context, db harmonyquery.DBInterface, service string, subPieceCidV1List []string) error {
	if len(subPieceCidV1List) == 0 {
		return nil
	}

	// SQLite portability: rewrite `piece_cid = ANY($2)` (Postgres TEXT[]) to
	// `piece_cid IN ($2, $3, ...)` with placeholder expansion. Mirrors the
	// pattern used in pdp/handlers.go and pdp/handlers_pull.go.
	placeholders := make([]string, len(subPieceCidV1List))
	args := make([]any, 0, len(subPieceCidV1List)+1)
	args = append(args, service)
	for i, c := range subPieceCidV1List {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, c)
	}
	doomedQuery := harmonyquery.RawString(`
			WITH doomed AS (
				SELECT pr.id, pr.piece_ref
				FROM pdp_piecerefs pr
				WHERE pr.service = $1
				  AND pr.piece_cid IN (` + strings.Join(placeholders, ", ") + `)
				  AND pr.data_set_refcount = 0
				  AND NOT EXISTS (
					SELECT 1 FROM pdp_data_set_piece_adds a
					WHERE a.pdp_pieceref = pr.id
					  AND a.pieces_added = FALSE
					  AND (a.add_message_ok IS NULL OR a.add_message_ok = TRUE)
				  )
			),
			deleted AS (
				DELETE FROM pdp_piecerefs pr
				USING doomed d
				WHERE pr.id = d.id
				RETURNING d.piece_ref AS piece_ref
			)
			DELETE FROM parked_piece_refs ppr
			USING deleted d
			WHERE ppr.ref_id = d.piece_ref
			  AND NOT EXISTS (SELECT 1 FROM pdp_piecerefs pr WHERE pr.piece_ref = ppr.ref_id)
		`)

	_, err := db.BeginTransactionI(ctx, func(tx harmonyquery.TxInterface) (bool, error) {
		n, err := tx.ExecI(doomedQuery, args...)
		if err != nil {
			return false, fmt.Errorf("discard orphan piecerefs: %w", err)
		}
		if n > 0 {
			log.Infow("discarded orphan PDP piecerefs after bad data set addPieces",
				"service", service,
				"subPieceCount", len(subPieceCidV1List),
				"parkedRefsRemoved", n)
		}
		return true, nil
	}, harmonydb.OptionRetry())
	return err
}
