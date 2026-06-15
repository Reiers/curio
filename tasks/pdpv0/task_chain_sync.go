package pdpv0

import (
	"context"
	"database/sql"
	"math/big"
	"strings"
	"time"

	"golang.org/x/xerrors"

	"github.com/curiostorage/harmonyquery"
	"github.com/filecoin-project/curio/harmony/harmonydb"
	"github.com/filecoin-project/curio/harmony/harmonytask"
	"github.com/filecoin-project/curio/harmony/resources"
	"github.com/filecoin-project/curio/harmony/taskhelp"
	"github.com/filecoin-project/curio/lib/ethchain"
	payment "github.com/filecoin-project/curio/lib/filecoinpayment"
	"github.com/filecoin-project/curio/pdp/contract"
	"github.com/filecoin-project/curio/pdp/contract/FWSS"
	"github.com/filecoin-project/curio/tasks/message"
	"github.com/filecoin-project/curio/tasks/tasknames"
)

type TaskChainSync struct {
	db        harmonyquery.DBInterface
	ethClient ethchain.EthClient
	sender    *message.SenderETH
	network   contract.Network
}

// NewTaskChainSync constructs the singleton ChainSync task. network
// selects which on-chain contract addresses (PDPVerifier, FWSS) the
// task's Do() body resolves. Pass the empty string to fall back to
// contract.NetworkFromBuildType().
func NewTaskChainSync(db harmonyquery.DBInterface, ethClient ethchain.EthClient, sender *message.SenderETH, network contract.Network) *TaskChainSync {
	return &TaskChainSync{
		db:        db,
		ethClient: ethClient,
		sender:    sender,
		network:   network,
	}
}

// resolvedNetwork returns the task's configured network, falling back
// to the build-tag-selected default if none was provided. Used at
// every contract.ContractAddressesFor / ContractAddresses call site
// within this task.
func (t *TaskChainSync) resolvedNetwork() contract.Network {
	if t.network == "" {
		return contract.NetworkFromBuildType()
	}
	return t.network
}

func (t *TaskChainSync) Do(ctx context.Context, taskID harmonytask.TaskID, stillOwned func() bool) (done bool, err error) {
	if !stillOwned() {
		return false, nil
	}
	if err := t.syncStaleDeletionTaskIDs(ctx); err != nil {
		return false, xerrors.Errorf("syncing stale PDP deletion task ids: %w", err)
	}

	if !stillOwned() {
		return false, nil
	}
	if err := t.syncMissingDeletionMessageWaits(ctx); err != nil {
		return false, xerrors.Errorf("syncing missing PDP deletion message waits: %w", err)
	}

	if !stillOwned() {
		return false, nil
	}
	if err := t.syncProvenDataSetFailureState(ctx); err != nil {
		return false, xerrors.Errorf("syncing proven PDP data set failure state: %w", err)
	}

	if !stillOwned() {
		return false, nil
	}
	if err := t.reArmDriftedProvingSchedule(ctx); err != nil {
		return false, xerrors.Errorf("re-arming drifted PDP proving schedule: %w", err)
	}

	if !stillOwned() {
		return false, nil
	}
	err = t.syncFinalizedDataSetDeletionRails(ctx)
	if err != nil {
		return false, xerrors.Errorf("syncing finalized PDP deletion rails: %w", err)
	}

	return true, nil
}

func (t *TaskChainSync) CanAccept(ids []harmonytask.TaskID, engine *harmonytask.TaskEngine) ([]harmonytask.TaskID, error) {
	return ids, nil
}

func (t *TaskChainSync) TypeDetails() harmonytask.TaskTypeDetails {
	return harmonytask.TaskTypeDetails{
		Max:  taskhelp.Max(1),
		Name: tasknames.PDPv0_ChainSync,
		Cost: resources.Resources{
			Cpu: 1,
			Gpu: 0,
			Ram: 64 << 20,
		},
		MaxFailures: 3,
		IAmBored:    harmonytask.SingletonTaskAdder(time.Hour*8, t),
	}
}

func (t *TaskChainSync) Adder(taskFunc harmonytask.AddTaskFunc) {}

var _ = harmonytask.Reg(&TaskChainSync{})
var _ harmonytask.TaskInterface = &TaskChainSync{}

// syncStaleDeletionTaskIDs clears deletion task ids that point at Harmony tasks which no longer exist.
// It does not advance any deletion state; it only unpins rows whose terminate/delete task exhausted retries so the normal
// schedulers can pick them up again.
func (t *TaskChainSync) syncStaleDeletionTaskIDs(ctx context.Context) error {
	comm, err := t.db.BeginTransactionI(ctx, func(tx harmonyquery.TxInterface) (bool, error) {
		// `UPDATE <table> <alias> SET ...` is Postgres-specific shorthand.
		// SQLite (modernc.org) requires the explicit `AS` keyword between
		// table and alias, otherwise it rejects with 'near "<alias>": syntax
		// error'. Adding `AS` is SQL-standard and accepted by Postgres too.
		terminated, err := tx.ExecI(`UPDATE pdp_delete_data_set AS pdds
			SET terminate_service_task_id = NULL
			WHERE pdds.terminate_service_task_id IS NOT NULL
			  AND pdds.after_terminate_service = FALSE
			  AND NOT EXISTS (
				SELECT 1
				FROM harmony_task ht
				WHERE ht.id = pdds.terminate_service_task_id
			  )`)
		if err != nil {
			return false, xerrors.Errorf("failed to clear stale terminate service task ids: %w", err)
		}

		deleted, err := tx.ExecI(`UPDATE pdp_delete_data_set AS pdds
			SET delete_data_set_task_id = NULL
			WHERE pdds.delete_data_set_task_id IS NOT NULL
			  AND pdds.after_delete_data_set = FALSE
			  AND NOT EXISTS (
				SELECT 1
				FROM harmony_task ht
				WHERE ht.id = pdds.delete_data_set_task_id
			  )`)
		if err != nil {
			return false, xerrors.Errorf("failed to clear stale delete data set task ids: %w", err)
		}

		if terminated > 0 || deleted > 0 {
			log.Infow("cleared stale PDP deletion task ids",
				"terminateServiceTasks", terminated,
				"deleteDataSetTasks", deleted)
		}

		return true, nil
	}, harmonydb.OptionRetry())
	if err != nil {
		return xerrors.Errorf("failed to commit stale task id cleanup: %w", err)
	}
	if !comm {
		return xerrors.Errorf("failed to commit stale task id cleanup")
	}

	return nil
}

type missingTerminationMessageWait struct {
	ID     int64  `db:"id"`
	TxHash string `db:"terminate_tx_hash"`
}

type missingDeleteMessageWait struct {
	ID     int64  `db:"id"`
	TxHash string `db:"delete_tx_hash"`
}

// syncMissingDeletionMessageWaits repairs deletion pipeline rows whose tx hash was recorded
// without a matching message_waits_eth row. The tx may no longer be available from the
// connected node, so this reconciles from contract state instead of tx history.
func (t *TaskChainSync) syncMissingDeletionMessageWaits(ctx context.Context) error {
	if err := t.syncMissingTerminationMessageWaits(ctx); err != nil {
		return err
	}
	return t.syncMissingDeleteMessageWaits(ctx)
}

func (t *TaskChainSync) syncMissingTerminationMessageWaits(ctx context.Context) error {
	var missing []missingTerminationMessageWait
	if err := t.db.SelectI(ctx, &missing, `
		SELECT id, terminate_tx_hash
		FROM pdp_delete_data_set pdds
		WHERE pdds.service_termination_epoch IS NULL
		  AND pdds.after_terminate_service = TRUE
		  AND pdds.terminate_tx_hash IS NOT NULL
		  AND NOT EXISTS (
			SELECT 1
			FROM message_waits_eth mwe
			WHERE mwe.signed_tx_hash = pdds.terminate_tx_hash
		  )
		ORDER BY id
	`); err != nil {
		return xerrors.Errorf("failed to select terminations missing message wait rows: %w", err)
	}

	if len(missing) == 0 {
		return nil
	}

	sAddr := contract.ContractAddressesFor(t.resolvedNetwork()).AllowedPublicRecordKeepers.FWSService
	viewAddr, err := contract.ResolveViewAddress(ctx, sAddr, t.ethClient)
	if err != nil {
		return xerrors.Errorf("failed to get FWSS view address: %w", err)
	}
	fwssv, err := FWSS.NewFilecoinWarmStorageServiceStateView(viewAddr, t.ethClient)
	if err != nil {
		return xerrors.Errorf("failed to instantiate FWSS service state view: %w", err)
	}

	for _, detail := range missing {
		ds, err := fwssv.GetDataSet(contract.EthCallOpts(ctx), big.NewInt(detail.ID))
		if err != nil {
			return xerrors.Errorf("failed to get data set %d from FWSS view: %w", detail.ID, err)
		}

		if ds.PdpEndEpoch.Int64() != 0 {
			n, err := t.db.ExecI(ctx, `
				UPDATE pdp_delete_data_set
				SET service_termination_epoch = $1,
				    terminate_service_task_id = NULL
				WHERE id = $2
				  AND terminate_tx_hash = $3
				  AND after_terminate_service = TRUE
				  AND service_termination_epoch IS NULL
			`, ds.PdpEndEpoch.Int64(), detail.ID, detail.TxHash)
			if err != nil {
				return xerrors.Errorf("failed to reconcile terminated data set %d: %w", detail.ID, err)
			}
			if n > 1 {
				return xerrors.Errorf("expected to update 0 or 1 rows for data set %d, updated %d", detail.ID, n)
			}
			if n == 1 {
				log.Infow("reconciled missing service termination message wait from chain state", "dataSetId", detail.ID, "txHash", detail.TxHash, "epoch", ds.PdpEndEpoch.Int64())
			}
			continue
		}

		n, err := t.db.ExecI(ctx, `
			UPDATE pdp_delete_data_set
			SET terminate_tx_hash = NULL,
			    after_terminate_service = FALSE,
			    terminate_service_task_id = NULL
			WHERE id = $1
			  AND terminate_tx_hash = $2
			  AND after_terminate_service = TRUE
			  AND service_termination_epoch IS NULL
		`, detail.ID, detail.TxHash)
		if err != nil {
			return xerrors.Errorf("failed to reset service termination missing message wait for data set %d: %w", detail.ID, err)
		}
		if n > 1 {
			return xerrors.Errorf("expected to update 0 or 1 rows for data set %d, updated %d", detail.ID, n)
		}
		if n == 1 {
			log.Warnw("reset service termination missing message wait for retry", "dataSetId", detail.ID, "txHash", detail.TxHash)
		}
	}

	return nil
}

func (t *TaskChainSync) syncMissingDeleteMessageWaits(ctx context.Context) error {
	var missing []missingDeleteMessageWait
	if err := t.db.SelectI(ctx, &missing, `
		SELECT id, delete_tx_hash
		FROM pdp_delete_data_set pdds
		WHERE pdds.service_termination_epoch IS NOT NULL
		  AND pdds.terminated = FALSE
		  AND pdds.after_delete_data_set = TRUE
		  AND pdds.delete_tx_hash IS NOT NULL
		  AND NOT EXISTS (
			SELECT 1
			FROM message_waits_eth mwe
			WHERE mwe.signed_tx_hash = pdds.delete_tx_hash
		  )
		ORDER BY id
	`); err != nil {
		return xerrors.Errorf("failed to select data set deletes missing message wait rows: %w", err)
	}

	if len(missing) == 0 {
		return nil
	}

	verifier, err := contract.NewPDPVerifier(contract.ContractAddressesFor(t.resolvedNetwork()).PDPVerifier, t.ethClient)
	if err != nil {
		return xerrors.Errorf("failed to instantiate PDPVerifier contract: %w", err)
	}

	for _, detail := range missing {
		live, err := verifier.DataSetLive(contract.EthCallOpts(ctx), big.NewInt(detail.ID))
		if err != nil {
			return xerrors.Errorf("failed to check if data set %d is live: %w", detail.ID, err)
		}

		if !live {
			if err := cleanupDeletedDataSet(ctx, t.db, detail.ID, detail.TxHash); err != nil {
				return xerrors.Errorf("failed to reconcile deleted data set %d: %w", detail.ID, err)
			}
			log.Infow("reconciled missing data set delete message wait from chain state", "dataSetId", detail.ID, "txHash", detail.TxHash)
			continue
		}

		n, err := t.db.ExecI(ctx, `
			UPDATE pdp_delete_data_set
			SET delete_tx_hash = NULL,
			    after_delete_data_set = FALSE,
			    delete_data_set_task_id = NULL
			WHERE id = $1
			  AND delete_tx_hash = $2
			  AND after_delete_data_set = TRUE
			  AND service_termination_epoch IS NOT NULL
			  AND terminated = FALSE
		`, detail.ID, detail.TxHash)
		if err != nil {
			return xerrors.Errorf("failed to reset data set delete missing message wait for data set %d: %w", detail.ID, err)
		}
		if n > 1 {
			return xerrors.Errorf("expected to update 0 or 1 rows for data set %d, updated %d", detail.ID, n)
		}
		if n == 1 {
			log.Warnw("reset data set delete missing message wait for retry", "dataSetId", detail.ID, "txHash", detail.TxHash)
		}
	}

	return nil
}

// syncProvenDataSetFailureState reconciles local proving backoff after PDPVerifier
// reports that the data set has advanced past the local failure state.
func (t *TaskChainSync) syncProvenDataSetFailureState(ctx context.Context) error {
	var dataSets []struct {
		ID                    int64         `db:"id"`
		ProveAtEpoch          sql.NullInt64 `db:"prove_at_epoch"`
		ConsecutiveFailures   int           `db:"consecutive_prove_failures"`
		NextProveAttemptEpoch sql.NullInt64 `db:"next_prove_attempt_at"`
		PrevChallengeEpoch    sql.NullInt64 `db:"prev_challenge_request_epoch"`
	}
	if err := t.db.SelectI(ctx, &dataSets, `SELECT id, prove_at_epoch, consecutive_prove_failures, next_prove_attempt_at, prev_challenge_request_epoch
		FROM pdp_data_sets
		WHERE unrecoverable_proving_failure_epoch IS NULL
		  AND (consecutive_prove_failures > 0 OR next_prove_attempt_at IS NOT NULL)
		ORDER BY id`); err != nil {
		return xerrors.Errorf("failed to select data sets with proving failure state: %w", err)
	}

	if len(dataSets) == 0 {
		log.Debugw("no PDP data set proving failure state to reconcile")
		return nil
	}

	pdpVerifier, err := contract.NewPDPVerifier(contract.ContractAddressesFor(t.resolvedNetwork()).PDPVerifier, t.ethClient)
	if err != nil {
		return xerrors.Errorf("failed to instantiate PDPVerifier contract: %w", err)
	}

	for _, dataSet := range dataSets {
		dataSetID := big.NewInt(dataSet.ID)

		live, err := pdpVerifier.DataSetLive(contract.EthCallOpts(ctx), dataSetID)
		if err != nil {
			return xerrors.Errorf("failed to check if data set %d is live: %w", dataSet.ID, err)
		}
		if !live {
			continue
		}

		lastProvenEpoch, err := pdpVerifier.GetDataSetLastProvenEpoch(contract.EthCallOpts(ctx), dataSetID)
		if err != nil {
			return xerrors.Errorf("failed to get last proven epoch for data set %d: %w", dataSet.ID, err)
		}
		if lastProvenEpoch == nil || lastProvenEpoch.Sign() <= 0 {
			continue
		}

		if !proofClearsLocalFailure(lastProvenEpoch,
			dataSet.ProveAtEpoch, dataSet.NextProveAttemptEpoch, dataSet.PrevChallengeEpoch,
			dataSet.ConsecutiveFailures) {
			continue
		}

		updated, err := t.db.ExecI(ctx, `UPDATE pdp_data_sets
			SET consecutive_prove_failures = 0,
				next_prove_attempt_at = NULL
			WHERE id = $1
			  AND unrecoverable_proving_failure_epoch IS NULL
			  AND (consecutive_prove_failures > 0 OR next_prove_attempt_at IS NOT NULL)`, dataSet.ID)
		if err != nil {
			return xerrors.Errorf("failed to reset proving failure state for data set %d: %w", dataSet.ID, err)
		}
		if updated != 0 && updated != 1 {
			return xerrors.Errorf("expected to update 0 or 1 rows for data set %d, updated %d", dataSet.ID, updated)
		}
		if updated == 1 {
			log.Infow("reset PDP data set proving failure state after confirmed PDPVerifier progress",
				"dataSetId", dataSet.ID,
				"lastProvenEpoch", lastProvenEpoch)
		}
	}

	return nil
}

// proofClearsLocalFailure decides whether an on-chain lastProvenEpoch proves
// that a dataset's local proving-failure state is stale and should be cleared.
//
// Three anchors, in priority order:
//  1. prove_at_epoch: the window we last armed locally. A proof at/after it
//     means we proved the window we were worried about.
//  2. next_prove_attempt_at: when prove_at_epoch is NULL but a backoff is
//     pending, reconstruct the epoch of the last failure (next attempt minus
//     the backoff for the current failure count) and require a proof strictly
//     after it.
//  3. prev_challenge_request_epoch (curio-core#65 drift recovery): when both
//     of the above are NULL but consecutive_prove_failures > 0, the dataset
//     drifted into a state no branch could anchor (an old failure path NULL'd
//     prove_at_epoch while the on-chain loop kept advancing). Anchor on the
//     last challenge we requested; a proof at/after it means the local count
//     is stale on an otherwise-healthy dataset.
//
// Returns false when no anchor is available (caller leaves the row untouched).
func proofClearsLocalFailure(lastProvenEpoch *big.Int, proveAtEpoch, nextProveAttemptEpoch, prevChallengeEpoch sql.NullInt64, consecutiveFailures int) bool {
	if lastProvenEpoch == nil {
		return false
	}
	switch {
	case proveAtEpoch.Valid:
		return lastProvenEpoch.Cmp(big.NewInt(proveAtEpoch.Int64)) >= 0
	case consecutiveFailures > 0 && nextProveAttemptEpoch.Valid:
		lastFailureEpoch := nextProveAttemptEpoch.Int64 - int64(CalculateBackoffBlocks(consecutiveFailures))
		return lastProvenEpoch.Cmp(big.NewInt(lastFailureEpoch)) > 0
	case consecutiveFailures > 0 && prevChallengeEpoch.Valid:
		return lastProvenEpoch.Cmp(big.NewInt(prevChallengeEpoch.Int64)) >= 0
	default:
		return false
	}
}

// reArmDriftedProvingSchedule recovers datasets that are proving healthily
// on-chain but have locally drifted to prove_at_epoch=NULL (curio-core#79).
//
// Background: once a failure path NULLs prove_at_epoch (max-failures branch,
// MarkDatasetProvingUnrecoverable's earlier siblings, resetDatasetToInitPP),
// the only re-arm path is InitProvingPeriodTask. But InitPP computes a window
// from config.InitChallengeWindowStart, valid ONLY for first-time init;
// calling initProvingPeriod on a dataset whose on-chain period is already
// initialized reverts. NextProvingPeriodTask, meanwhile, only selects rows
// with prove_at_epoch IS NOT NULL. So a healthy-on-chain dataset stuck at
// NULL can never re-sync to its live schedule.
//
// #65's syncProvenDataSetFailureState does NOT cover this: it only fires when
// consecutive_prove_failures > 0 OR next_prove_attempt_at IS NOT NULL. The #79
// case has zero failures and no pending backoff (the drift came from a path
// that NULL'd prove_at_epoch without leaving failure breadcrumbs).
//
// Fix: read the contract's live proving schedule (the same path NextPP uses)
// and re-arm prove_at_epoch to NextPDPChallengeWindowStart. This re-arms the
// LOCAL ROW ONLY (no on-chain tx) so NextPP picks the dataset back up on its
// real schedule, with no illegal re-init.
func (t *TaskChainSync) reArmDriftedProvingSchedule(ctx context.Context) error {
	// Candidate drift set: live-shaped rows (init_ready) that are not armed
	// locally, not terminated, and not currently being driven by a NextPP
	// challenge task. Zero-failure by construction here — the failure-count
	// reconciler (#65) owns the rows with failure breadcrumbs.
	var dataSets []struct {
		ID int64 `db:"id"`
	}
	if err := t.db.SelectI(ctx, &dataSets, `SELECT id
		FROM pdp_data_sets
		WHERE prove_at_epoch IS NULL
		  AND init_ready = TRUE
		  AND challenge_request_task_id IS NULL
		  AND unrecoverable_proving_failure_epoch IS NULL
		  AND next_prove_attempt_at IS NULL
		  AND consecutive_prove_failures = 0
		ORDER BY id`); err != nil {
		return xerrors.Errorf("failed to select drifted PDP data sets: %w", err)
	}

	if len(dataSets) == 0 {
		return nil
	}

	pdpVerifier, err := contract.NewPDPVerifier(contract.ContractAddressesFor(t.resolvedNetwork()).PDPVerifier, t.ethClient)
	if err != nil {
		return xerrors.Errorf("failed to instantiate PDPVerifier contract: %w", err)
	}

	for _, dataSet := range dataSets {
		dataSetID := big.NewInt(dataSet.ID)

		// Only re-arm datasets that are actually live + proving on-chain.
		// A dead/never-initialized dataset must NOT be re-armed here — that
		// belongs to InitPP's first-time path.
		live, err := pdpVerifier.DataSetLive(contract.EthCallOpts(ctx), dataSetID)
		if err != nil {
			return xerrors.Errorf("failed to check if data set %d is live: %w", dataSet.ID, err)
		}
		if !live {
			continue
		}

		lastProvenEpoch, err := pdpVerifier.GetDataSetLastProvenEpoch(contract.EthCallOpts(ctx), dataSetID)
		if err != nil {
			return xerrors.Errorf("failed to get last proven epoch for data set %d: %w", dataSet.ID, err)
		}
		if !driftedDataSetIsProving(live, lastProvenEpoch) {
			// Not live, or never proven on-chain yet — a first-init case;
			// leave it for InitPP rather than re-arming a window the chain
			// doesn't have.
			continue
		}

		// Resolve the live proving schedule exactly as NextPP does.
		listenerAddr, err := pdpVerifier.GetDataSetListener(contract.EthCallOpts(ctx), dataSetID)
		if err != nil {
			return xerrors.Errorf("failed to get listener for data set %d: %w", dataSet.ID, err)
		}
		provingSchedule, err := contract.GetProvingScheduleFromListener(ctx, listenerAddr, t.ethClient)
		if err != nil {
			return xerrors.Errorf("failed to get proving schedule for data set %d: %w", dataSet.ID, err)
		}

		nextProveAt, err := provingSchedule.NextPDPChallengeWindowStart(contract.EthCallOpts(ctx), dataSetID)
		if err != nil {
			// If the contract reports the period isn't initialized, this is
			// genuinely an init case after all — leave it to InitPP, don't
			// fail the whole reconciler pass on one drifted row.
			if strings.Contains(err.Error(), "0x999010d5") { // Error.ProvingPeriodNotInitialized
				log.Warnw("drifted data set reports proving period not initialized; leaving for InitPP",
					"dataSetId", dataSet.ID)
				continue
			}
			return xerrors.Errorf("failed to get next challenge window for data set %d: %w", dataSet.ID, err)
		}
		if !nextProveEpochIsArmable(nextProveAt) {
			continue
		}

		// Re-arm the LOCAL row only. Guard the UPDATE with the same NULL/
		// init_ready/not-terminated predicate so a concurrent NextPP/InitPP
		// that armed the row first wins (affected=0, no clobber).
		updated, err := t.db.ExecI(ctx, `UPDATE pdp_data_sets
			SET prove_at_epoch = $1,
				init_ready = FALSE
			WHERE id = $2
			  AND prove_at_epoch IS NULL
			  AND init_ready = TRUE
			  AND challenge_request_task_id IS NULL
			  AND unrecoverable_proving_failure_epoch IS NULL`, nextProveAt.Int64(), dataSet.ID)
		if err != nil {
			return xerrors.Errorf("failed to re-arm prove_at_epoch for data set %d: %w", dataSet.ID, err)
		}
		if updated == 1 {
			log.Infow("re-armed drift-recovered PDP data set to live proving schedule",
				"dataSetId", dataSet.ID,
				"lastProvenEpoch", lastProvenEpoch,
				"nextProveAt", nextProveAt)
		}
	}

	return nil
}

// driftedDataSetIsProving reports whether an on-chain observation (DataSetLive
// + GetDataSetLastProvenEpoch) shows a dataset healthy enough to re-arm.
// A non-live dataset, or one that has never produced a proof (lastProven<=0),
// is a first-init case that belongs to InitPP, not the drift reconciler.
func driftedDataSetIsProving(live bool, lastProvenEpoch *big.Int) bool {
	if !live {
		return false
	}
	return lastProvenEpoch != nil && lastProvenEpoch.Sign() > 0
}

// nextProveEpochIsarmable reports whether a NextPDPChallengeWindowStart result
// is a usable re-arm target (non-nil, strictly positive).
func nextProveEpochIsArmable(nextProveAt *big.Int) bool {
	return nextProveAt != nil && nextProveAt.Sign() > 0
}

// syncFinalizedDataSetDeletionRails moves terminated PDP data sets to the local deletion-allowed state once the payment rail is final.
// A RailInactiveOrSettled revert or EndEpoch == SettledUpTo means settlement is complete; this pass
// only toggles deletion_allowed and leaves the actual delete to the existing delete task.
func (t *TaskChainSync) syncFinalizedDataSetDeletionRails(ctx context.Context) error {
	current, err := t.ethClient.BlockNumber(ctx)
	if err != nil {
		return xerrors.Errorf("failed to get current block number: %w", err)
	}

	var pending []struct {
		ID int64 `db:"id"`
	}
	if err := t.db.SelectI(ctx, &pending, `SELECT id
		FROM pdp_delete_data_set
		WHERE after_terminate_service = TRUE
		  AND deletion_allowed = FALSE
		  AND service_termination_epoch IS NOT NULL
		  AND service_termination_epoch <= $1
		ORDER BY service_termination_epoch, id`, current); err != nil {
		return xerrors.Errorf("failed to select pending data sets: %w", err)
	}

	if len(pending) == 0 {
		log.Debugw("no PDP deletion rails waiting for finalization")
		return nil
	}

	sAddr := contract.ContractAddressesFor(t.resolvedNetwork()).AllowedPublicRecordKeepers.FWSService
	viewAddr, err := contract.ResolveViewAddress(ctx, sAddr, t.ethClient)
	if err != nil {
		return xerrors.Errorf("failed to get FWSS view address: %w", err)
	}

	fwssv, err := FWSS.NewFilecoinWarmStorageServiceStateView(viewAddr, t.ethClient)
	if err != nil {
		return xerrors.Errorf("failed to instantiate FWSS service state view: %w", err)
	}

	paymentAddr, err := payment.PaymentContractAddressFor(t.resolvedNetwork())
	if err != nil {
		return xerrors.Errorf("failed to get payment contract address: %w", err)
	}

	payments, err := payment.NewPayments(paymentAddr, t.ethClient)
	if err != nil {
		return xerrors.Errorf("failed to instantiate Payments contract: %w", err)
	}

	for _, dataSet := range pending {
		ds, err := fwssv.GetDataSet(contract.EthCallOpts(ctx), big.NewInt(dataSet.ID))
		if err != nil {
			return xerrors.Errorf("failed to get data set %d from FWSS view: %w", dataSet.ID, err)
		}

		rail, err := payments.GetRail(contract.EthCallOpts(ctx), ds.PdpRailId)
		if err != nil {
			if payment.IsRailInactiveOrSettledError(err) {
				if err := t.ensureDataSetDeletion(ctx, dataSet.ID); err != nil {
					return err
				}
				log.Infow("allowed PDP data set deletion after finalized rail lookup reverted",
					"dataSetId", dataSet.ID,
					"pdpRailId", ds.PdpRailId)
				continue
			}
			return xerrors.Errorf("failed to get payment rail %s for data set %d: %w", ds.PdpRailId, dataSet.ID, err)
		}

		if rail.EndEpoch != nil && rail.SettledUpTo != nil && rail.EndEpoch.Sign() > 0 && rail.EndEpoch.Cmp(rail.SettledUpTo) == 0 {
			if err := t.ensureDataSetDeletion(ctx, dataSet.ID); err != nil {
				return err
			}
			log.Infow("allowed PDP data set deletion after rail finalized",
				"dataSetId", dataSet.ID,
				"pdpRailId", ds.PdpRailId,
				"endEpoch", rail.EndEpoch,
				"settledUpTo", rail.SettledUpTo)
		}
	}

	return nil
}

// ensureDataSetDeletion marks a terminated data set as eligible for the normal delete task once chain sync has confirmed rail finality.
// The WHERE clause preserves idempotency and prevents this helper from moving rows that have not reached the post-terminate state.
func (t *TaskChainSync) ensureDataSetDeletion(ctx context.Context, dataSetID int64) error {
	n, err := t.db.ExecI(ctx, `UPDATE pdp_delete_data_set
		SET deletion_allowed = TRUE
		WHERE id = $1
		  AND after_terminate_service = TRUE
		  AND deletion_allowed = FALSE
		  AND service_termination_epoch IS NOT NULL`, dataSetID)
	if err != nil {
		return xerrors.Errorf("failed to allow data set deletion for %d: %w", dataSetID, err)
	}
	if n != 0 && n != 1 {
		return xerrors.Errorf("expected to update 0 or 1 rows for data set %d, updated %d", dataSetID, n)
	}

	return nil
}
