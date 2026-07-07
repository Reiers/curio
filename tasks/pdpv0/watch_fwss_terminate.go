package pdpv0

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"

	"golang.org/x/xerrors"

	"github.com/curiostorage/harmonyquery"
	"github.com/filecoin-project/curio/lib/ethchain"
	"github.com/filecoin-project/curio/lib/paths/alertinginterface"
	"github.com/filecoin-project/curio/pdp/contract"
	"github.com/filecoin-project/curio/pdp/contract/FWSS"

	chainTypes "github.com/filecoin-project/lotus/chain/types"
)

const alertNameTerminateFWSSService = "TerminateFWSSService"

type pendingServiceTermination struct {
	DataSetId int64  `db:"id"`
	TxHash    string `db:"terminate_tx_hash"`
}

type serviceTerminationMessageWait struct {
	TxHash  string       `db:"signed_tx_hash"`
	Status  string       `db:"tx_status"`
	Success sql.NullBool `db:"tx_success"`
}

// NewTerminateServiceWatcher registers a Terminate-phase watcher that reconciles
// pdp_delete_data_set termination tx state against on-chain receipts.
// network selects the on-chain FWSS contract address; pass the empty
// string to fall back to contract.NetworkFromBuildType().
func NewTerminateServiceWatcher(w *Watcher, network contract.Network) {
	if network == "" {
		network = contract.NetworkFromBuildType()
	}
	if err := w.AddWatcher(func(ctx context.Context, db harmonyquery.DBInterface, ethClient ethchain.EthClient, al alertinginterface.AlertingInterface, revert, apply *chainTypes.TipSet) {
		at := al.AddAlertType(alertNameTerminateFWSSService, alertType)
		err := processTerminations(ctx, network, db, ethClient)
		if err != nil {
			log.Warnf("Failed to process pending service termination transactions: %s", err)
			al.Raise(at, map[string]interface{}{
				"error": err.Error(),
			})
		}
	}, WatcherOrderTerminate); err != nil {
		panic(err)
	}
}

func processTerminations(ctx context.Context, network contract.Network, db harmonyquery.DBInterface, ethClient ethchain.EthClient) error {
	var pending []pendingServiceTermination
	err := db.SelectI(ctx, &pending, `
		SELECT id, terminate_tx_hash
		FROM pdp_delete_data_set
		WHERE service_termination_epoch IS NULL
		  AND after_terminate_service = TRUE
		  AND terminate_tx_hash IS NOT NULL
	`)
	if err != nil {
		return xerrors.Errorf("failed to select pending data set terminations: %w", err)
	}

	if len(pending) == 0 {
		return nil
	}

	byHash := make(map[string]pendingServiceTermination, len(pending))
	hashes := make([]string, 0, len(pending))
	for _, detail := range pending {
		hashes = append(hashes, detail.TxHash)
		byHash[detail.TxHash] = detail
	}

	// SQLite portability: rewrite `WHERE signed_tx_hash = ANY($1)` (Postgres
	// TEXT[]) to `WHERE signed_tx_hash IN ($1, $2, ...)` with placeholder
	// expansion. Mirrors the pattern used in pdp/handlers.go and
	// pdp/handlers_pull.go for the same reason.
	placeholders := make([]string, len(hashes))
	args := make([]any, len(hashes))
	for i, h := range hashes {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = h
	}
	waitsQuery := harmonyquery.RawString(
		"SELECT signed_tx_hash, tx_status, tx_success FROM message_waits_eth WHERE signed_tx_hash IN (" +
			strings.Join(placeholders, ",") + ")",
	)

	var waits []serviceTerminationMessageWait
	err = db.SelectI(ctx, &waits, waitsQuery, args...)
	if err != nil {
		return xerrors.Errorf("failed to select service termination message waits: %w", err)
	}

	seen := map[string]struct{}{}
	var successes []pendingServiceTermination
	var failures []pendingServiceTermination
	for _, wait := range waits {
		seen[wait.TxHash] = struct{}{}
		detail, ok := byHash[wait.TxHash]
		if !ok {
			continue
		}

		if wait.Status == "confirmed" && wait.Success.Valid && wait.Success.Bool {
			successes = append(successes, detail)
			continue
		}

		if wait.Status == "failed" || (wait.Status == "confirmed" && wait.Success.Valid && !wait.Success.Bool) {
			failures = append(failures, detail)
		}
	}

	for _, detail := range pending {
		if _, ok := seen[detail.TxHash]; ok {
			continue
		}
		log.Warnw("filecoin warm storage service termination tx missing message_waits_eth row", "txHash", detail.TxHash, "dataSetId", detail.DataSetId)
	}

	successErr := processSuccessfulTerminations(ctx, network, db, ethClient, successes)
	failureErr := processFailedTerminations(ctx, db, failures)
	if successErr != nil {
		return successErr
	}
	return failureErr
}

func processSuccessfulTerminations(ctx context.Context, network contract.Network, db harmonyquery.DBInterface, ethClient ethchain.EthClient, successes []pendingServiceTermination) error {
	if len(successes) == 0 {
		return nil
	}

	sAddr := contract.ContractAddressesFor(network).AllowedPublicRecordKeepers.FWSService
	viewAddr, err := contract.ResolveViewAddress(ctx, sAddr, ethClient)
	if err != nil {
		return xerrors.Errorf("failed to get FWSS view address: %w", err)
	}
	fwssv, err := FWSS.NewFilecoinWarmStorageServiceStateView(viewAddr, ethClient)
	if err != nil {
		return xerrors.Errorf("failed to instantiate FWSS service state view: %w", err)
	}

	for _, detail := range successes {
		ds, err := fwssv.GetDataSet(contract.EthCallOpts(ctx), big.NewInt(detail.DataSetId))
		if err != nil {
			return xerrors.Errorf("failed to get data set %d: %w", detail.DataSetId, err)
		}

		if ds.PdpEndEpoch.Int64() == 0 {
			// Huston! we have a serious problem
			return xerrors.Errorf("data set %d has no termination epoch", detail.DataSetId)
		}

		n, err := db.ExecI(ctx, `UPDATE pdp_delete_data_set SET service_termination_epoch = $1 WHERE id = $2`, ds.PdpEndEpoch.Int64(), detail.DataSetId)
		if err != nil {
			return xerrors.Errorf("failed to update pdp_delete_data_set: %w", err)
		}

		if n != 1 {
			return xerrors.Errorf("expected to update 1 row, updated %d", n)
		}

		log.Infow("Successfully confirmed data set termination", "dataSetId", detail.DataSetId, "epoch", ds.PdpEndEpoch.Int64(), "txHash", detail.TxHash)
	}
	return nil
}

func processFailedTerminations(ctx context.Context, db harmonyquery.DBInterface, failures []pendingServiceTermination) error {
	for _, detail := range failures {
		_, err := db.ExecI(ctx, `
			UPDATE pdp_delete_data_set
			SET terminate_tx_hash = NULL,
			    after_terminate_service = FALSE,
			    terminate_service_task_id = NULL
			WHERE id = $1
			  AND terminate_tx_hash = $2
			  AND after_terminate_service = TRUE
			  AND service_termination_epoch IS NULL
		`, detail.DataSetId, detail.TxHash)
		if err != nil {
			return xerrors.Errorf("failed to reset failed service termination for data set %d: %w", detail.DataSetId, err)
		}

		log.Warnw("reset failed provider service termination for retry", "dataSetId", detail.DataSetId, "txHash", detail.TxHash)
	}

	return nil
}
