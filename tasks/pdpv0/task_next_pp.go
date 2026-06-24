package pdpv0

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/samber/lo"
	"github.com/yugabyte/pgx/v5"
	"golang.org/x/xerrors"

	"github.com/curiostorage/harmonyquery"
	"github.com/filecoin-project/curio/harmony/harmonydb"
	"github.com/filecoin-project/curio/harmony/harmonytask"
	"github.com/filecoin-project/curio/harmony/resources"
	"github.com/filecoin-project/curio/harmony/taskhelp"
	"github.com/filecoin-project/curio/lib/chainsched"
	"github.com/filecoin-project/curio/lib/ethchain"
	"github.com/filecoin-project/curio/lib/promise"
	"github.com/filecoin-project/curio/pdp/contract"
	"github.com/filecoin-project/curio/tasks/message"
	"github.com/filecoin-project/curio/tasks/tasknames"

	chainTypes "github.com/filecoin-project/lotus/chain/types"
)

type NextProvingPeriodTask struct {
	db        harmonyquery.DBInterface
	ethClient ethchain.EthClient
	sender    *message.SenderETH

	fil NextProvingPeriodTaskChainApi

	addFunc promise.Promise[harmonytask.AddTaskFunc]

	network contract.Network
}

func (n *NextProvingPeriodTask) resolvedNetwork() contract.Network {
	if n.network == "" {
		return contract.NetworkFromBuildType()
	}
	return n.network
}

type NextProvingPeriodTaskChainApi interface {
	ChainHead(context.Context) (*chainTypes.TipSet, error)
}

func NewNextProvingPeriodTask(db harmonyquery.DBInterface, ethClient ethchain.EthClient, fil NextProvingPeriodTaskChainApi, chainSched *chainsched.CurioChainSched, sender *message.SenderETH, network contract.Network) *NextProvingPeriodTask {
	n := &NextProvingPeriodTask{
		db:        db,
		ethClient: ethClient,
		sender:    sender,
		fil:       fil,
		network:   network,
	}

	_ = chainSched.AddHandler(func(ctx context.Context, revert, apply *chainTypes.TipSet) error {
		if apply == nil {
			return nil
		}

		// Now query the db for data sets needing nextProvingPeriod
		var toCallNext []struct {
			DataSetId int64 `db:"id"`
		}

		currentHeight := apply.Height()
		err := db.SelectI(ctx, &toCallNext, `
                SELECT id
                FROM pdp_data_sets
                WHERE challenge_request_task_id IS NULL
                  AND (prove_at_epoch + challenge_window) <= $1
                  AND unrecoverable_proving_failure_epoch IS NULL
                  AND (next_prove_attempt_at IS NULL OR next_prove_attempt_at <= $1)
            `, currentHeight)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return xerrors.Errorf("failed to select data sets needing nextProvingPeriod: %w", err)
		}

		for _, ps := range toCallNext {
			n.addFunc.Val(ctx)(func(id harmonytask.TaskID, tx harmonyquery.TxInterface) (shouldCommit bool, seriousError error) {
				// Update pdp_data_sets to set challenge_request_task_id = id
				affected, err := tx.ExecI(`
                        UPDATE pdp_data_sets
                        SET challenge_request_task_id = $1
                        WHERE id = $2 AND challenge_request_task_id IS NULL
                    `, id, ps.DataSetId)
				if err != nil {
					return false, xerrors.Errorf("failed to update pdp_data_sets: %w", err)
				}
				if affected == 0 {
					// Someone else might have already scheduled the task
					return false, nil
				}

				return true, nil
			})
		}

		return nil
	})

	return n
}

// resetDatasetToInitPP resets a dataset so that InitProvingPeriodTask picks it up.
// This is only appropriate for datasets whose on-chain proving period was never
// initialized (e.g. ProvingPeriodNotInitialized error from the contract). InitPP
// computes a fresh challenge window from config.InitChallengeWindowStart, which is
// only valid for first-time initialization.
func resetDatasetToInitPP(ctx context.Context, db harmonyquery.DBInterface, dataSetId int64) error {
	log.Infow("resetting dataset to init proving period state", "dataSetId", dataSetId)
	_, err := db.ExecI(ctx, `
             UPDATE pdp_data_sets
             SET challenge_request_msg_hash = NULL,
                     prove_at_epoch = NULL,
                     init_ready = TRUE,
                     prev_challenge_request_epoch = NULL
             WHERE id = $1
     `, dataSetId)
	if err != nil {
		return xerrors.Errorf("failed to reset dataset to init state: %w", err)
	}
	return nil
}

// invalidChallengeEpochSelector is the 4-byte selector of
// InvalidChallengeEpoch(uint256 setId, uint256 min, uint256 max, uint256 provided).
var invalidChallengeEpochSelector = [4]byte{0x25, 0xa0, 0xc7, 0xf7}

// clampNextProveAt simulates nextProvingPeriod(dataSetId, candidate) via eth_call.
// If the contract accepts it, candidate is returned unchanged. If the contract
// reverts with InvalidChallengeEpoch, the reported [min,max] window is parsed and
// candidate is clamped into range (preferring the midpoint when fully out of range
// to maximize the margin against head drift before the tx lands). Any other revert
// or decode failure returns an error so the caller can fall back to the raw value.
func (n *NextProvingPeriodTask) clampNextProveAt(ctx context.Context, abiData *abi.ABI, pdpVerifierAddress common.Address, dataSetId int64, candidate *big.Int) (*big.Int, error) {
	pdpVerifier, err := contract.NewPDPVerifier(pdpVerifierAddress, n.ethClient)
	if err != nil {
		return nil, xerrors.Errorf("instantiate PDPVerifier for clamp sim: %w", err)
	}
	fromAddr, _, err := pdpVerifier.GetDataSetStorageProvider(contract.EthCallOpts(ctx), big.NewInt(dataSetId))
	if err != nil {
		return nil, xerrors.Errorf("get storage provider for clamp sim: %w", err)
	}

	call := func(epoch *big.Int) error {
		data, perr := abiData.Pack("nextProvingPeriod", big.NewInt(dataSetId), epoch, []byte{})
		if perr != nil {
			return xerrors.Errorf("pack sim: %w", perr)
		}
		_, cerr := n.ethClient.CallContract(ctx, ethereum.CallMsg{
			From: fromAddr,
			To:   &pdpVerifierAddress,
			Data: data,
		}, nil)
		return cerr
	}

	err = call(candidate)
	if err == nil {
		return candidate, nil // contract accepts the view value as-is
	}

	min, max, ok := parseInvalidChallengeEpoch(err)
	if !ok {
		return nil, xerrors.Errorf("nextProvingPeriod sim reverted (non-window): %w", err)
	}
	if min == nil || max == nil || min.Cmp(max) > 0 {
		return nil, xerrors.Errorf("nextProvingPeriod reported nonsensical window [%v,%v]", min, max)
	}

	// Clamp: prefer midpoint of [min,max] for the widest margin against head drift.
	mid := new(big.Int).Add(min, max)
	mid.Rsh(mid, 1)
	if verr := call(mid); verr != nil {
		return nil, xerrors.Errorf("clamped midpoint %v still rejected: %w", mid, verr)
	}
	return mid, nil
}

// parseInvalidChallengeEpoch extracts (min,max) from an InvalidChallengeEpoch
// revert carried in err. Returns ok=false if the error is not that custom error.
func parseInvalidChallengeEpoch(err error) (min, max *big.Int, ok bool) {
	if err == nil {
		return nil, nil, false
	}
	raw := extractRevertHex(err.Error())
	if len(raw) < 4+32*4 {
		return nil, nil, false
	}
	if raw[0] != invalidChallengeEpochSelector[0] || raw[1] != invalidChallengeEpochSelector[1] ||
		raw[2] != invalidChallengeEpochSelector[2] || raw[3] != invalidChallengeEpochSelector[3] {
		return nil, nil, false
	}
	// layout: selector | setId(32) | min(32) | max(32) | provided(32)
	min = new(big.Int).SetBytes(raw[4+32 : 4+64])
	max = new(big.Int).SetBytes(raw[4+64 : 4+96])
	return min, max, true
}

// extractRevertHex pulls the trailing 0x... revert payload out of an error string.
func extractRevertHex(s string) []byte {
	idx := strings.LastIndex(s, "0x")
	if idx < 0 {
		return nil
	}
	hexPart := s[idx+2:]
	// trim any trailing non-hex characters
	end := 0
	for end < len(hexPart) {
		c := hexPart[end]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
			end++
			continue
		}
		break
	}
	hexPart = hexPart[:end]
	if len(hexPart)%2 == 1 {
		hexPart = hexPart[:len(hexPart)-1]
	}
	b, derr := hex.DecodeString(hexPart)
	if derr != nil {
		return nil
	}
	return b
}

func (n *NextProvingPeriodTask) Do(ctx context.Context, taskID harmonytask.TaskID, stillOwned func() bool) (done bool, err error) {
	// Select the data set where challenge_request_task_id = taskID
	var dataSetId int64

	err = n.db.QueryRowI(ctx, `
        SELECT id
        FROM pdp_data_sets
        WHERE challenge_request_task_id = $1 AND prove_at_epoch IS NOT NULL
    `, taskID).Scan(&dataSetId)
	if errors.Is(err, pgx.ErrNoRows) {
		// No matching data set, task is done (something weird happened, and e.g another task was spawned in place of this one)
		return true, nil
	}
	if err != nil {
		return false, xerrors.Errorf("failed to query pdp_data_sets: %w", err)
	}

	defer func() {
		if err != nil {
			log.Errorw("Next challange window scheduling failed", "dataSetId", dataSetId, "error", err)
			err = fmt.Errorf("failed to set up next proving period for dataset %d: %w", dataSetId, err)
		}
	}()

	// Get the listener address for this data set from the PDPVerifier contract
	pdpVerifier, err := contract.NewPDPVerifier(contract.ContractAddressesFor(n.resolvedNetwork()).PDPVerifier, n.ethClient)
	if err != nil {
		return false, xerrors.Errorf("failed to instantiate PDPVerifier contract: %w", err)
	}

	listenerAddr, err := pdpVerifier.GetDataSetListener(contract.EthCallOpts(ctx), big.NewInt(dataSetId))
	if err != nil {
		return false, xerrors.Errorf("failed to get listener address for data set %d: %w", dataSetId, err)
	}

	// Get the proving schedule from the listener (handles view contract indirection)
	provingSchedule, err := contract.GetProvingScheduleFromListener(ctx, listenerAddr, n.ethClient)
	if err != nil {
		return false, xerrors.Errorf("failed to get proving schedule from listener: %w", err)
	}

	// In case of contract migration update db schema with latest proving schedule
	err = n.refreshProvingPeriod(ctx, dataSetId, provingSchedule)
	if err != nil {
		return false, xerrors.Errorf("failed to refresh proving period: %w", err)
	}

	next_prove_at, err := provingSchedule.NextPDPChallengeWindowStart(contract.EthCallOpts(ctx), big.NewInt(dataSetId))
	if err != nil {
		// not my favourite way to handle this but pragmatic
		// for some reason we are in a proving loop running but it is not initialized
		if strings.Contains(err.Error(), "0x999010d5") { // Error.ProvingPeriodNotInitialized
			if err := resetDatasetToInitPP(ctx, n.db, dataSetId); err != nil {
				return false, xerrors.Errorf("failed to reset to init: %w", err)
			}
			return true, nil // true as this task is done
		}
		return false, xerrors.Errorf("failed to get next challenge window start: %w", err)
	}

	// Instantiate the PDPVerifier contract
	pdpContracts := contract.ContractAddressesFor(n.resolvedNetwork())
	pdpVerifierAddress := pdpContracts.PDPVerifier

	// Prepare the transaction data
	abiData, err := contract.PDPVerifierMetaData.GetAbi()
	if err != nil {
		return false, xerrors.Errorf("failed to get PDPVerifier ABI: %w", err)
	}

	// curio-core: the NextPDPChallengeWindowStart view can drift out of the
	// window nextProvingPeriod will actually accept when the dataset's on-chain
	// proving state is briefly inconsistent (e.g. lastProvenEpoch momentarily
	// ahead of nextChallengeEpoch after a missed/recovered window). Submitting
	// the drifted value reverts with InvalidChallengeEpoch(setId,min,max,provided)
	// and wedges the dataset in a retry loop that never re-arms proving.
	// Validate the value against the contract's accept-window first; if it is
	// out of range, clamp into [min,max] using the bounds the contract reports.
	if clamped, cerr := n.clampNextProveAt(ctx, abiData, pdpVerifierAddress, dataSetId, next_prove_at); cerr != nil {
		log.Warnw("nextProvingPeriod window validation failed; sending unclamped value",
			"dataSetId", dataSetId, "next_prove_at", next_prove_at, "err", cerr)
	} else if clamped != nil && clamped.Cmp(next_prove_at) != 0 {
		log.Warnw("clamped next_prove_at into contract accept-window",
			"dataSetId", dataSetId, "view_value", next_prove_at, "clamped", clamped)
		next_prove_at = clamped
	}

	data, err := abiData.Pack("nextProvingPeriod", big.NewInt(dataSetId), next_prove_at, []byte{})
	if err != nil {
		return false, xerrors.Errorf("failed to pack data: %w", err)
	}

	// Prepare the transaction
	txEth := types.NewTransaction(
		0,                  // nonce (will be set by sender)
		pdpVerifierAddress, // to
		big.NewInt(0),      // value
		0,                  // gasLimit (to be estimated)
		nil,                // gasPrice (to be set by sender)
		data,               // data
	)

	if !stillOwned() {
		// Task was abandoned, don't send the transaction
		return false, nil
	}

	fromAddress, _, err := pdpVerifier.GetDataSetStorageProvider(contract.EthCallOpts(ctx), big.NewInt(dataSetId))
	if err != nil {
		return false, xerrors.Errorf("failed to get default sender address: %w", err)
	}

	// Get the current tipset
	ts, err := n.fil.ChainHead(ctx)
	if err != nil {
		return false, xerrors.Errorf("failed to get chain head: %w", err)
	}

	// Send the transaction
	reason := "pdp-proving-period"
	txHash, sendErr := n.sender.Send(ctx, fromAddress, txEth, reason)
	if sendErr != nil {
		currentHeight := int64(ts.Height())
		comm, err := n.db.BeginTransactionI(ctx, func(tx harmonyquery.TxInterface) (commit bool, err error) {
			handleErr := HandleProvingSendError(tx, dataSetId, currentHeight, sendErr)
			if handleErr != nil {
				return false, xerrors.Errorf("failed to handle proving send error: %w", handleErr)
			}
			return true, nil
		}, harmonydb.OptionRetry())
		if err != nil {
			return false, xerrors.Errorf("failed to send transaction: %w", err)
		}
		if !comm {
			return false, xerrors.Errorf("failed to commit transaction")
		}
		return true, nil
	}

	// Update the database in a transaction
	_, err = n.db.BeginTransactionI(ctx, func(tx harmonyquery.TxInterface) (bool, error) {
		// Update pdp_data_sets
		affected, err := tx.ExecI(`
            UPDATE pdp_data_sets
            SET challenge_request_msg_hash = $1,
                prev_challenge_request_epoch = $2,
				prove_at_epoch = $3
            WHERE id = $4
        `, txHash.Hex(), ts.Height(), next_prove_at.Uint64(), dataSetId)
		if err != nil {
			return false, xerrors.Errorf("failed to update pdp_data_sets: %w", err)
		}
		if affected == 0 {
			return false, xerrors.Errorf("pdp_data_sets update affected 0 rows")
		}

		// Insert into message_waits_eth
		_, err = tx.ExecI(`
            INSERT INTO message_waits_eth (signed_tx_hash, tx_status)
            VALUES ($1, 'pending') ON CONFLICT DO NOTHING
        `, txHash.Hex())
		if err != nil {
			return false, xerrors.Errorf("failed to insert into message_waits_eth: %w", err)
		}

		return true, nil
	})
	if err != nil {
		return false, xerrors.Errorf("failed to perform database transaction: %w", err)
	}

	// For all `schedulePieceDeletions` messages relevant to this dataset, mark these pieces as removed
	err = n.processPendingPieceDeletes(ctx, dataSetId)
	if err != nil {
		log.Warnf("Failed to process pending piece delete: %s", err)
	}

	// Task completed successfully
	log.Infow("Next challenge window scheduled", "epoch", next_prove_at, "dataSetId", dataSetId)

	return true, nil
}

func (n *NextProvingPeriodTask) processPendingPieceDeletes(ctx context.Context, dataSetId int64) error {

	var pendingDeletes []struct {
		PieceID   int64        `db:"piece_id"`
		TxHash    string       `db:"rm_message_hash"`
		TxSuccess sql.NullBool `db:"tx_success"`
	}

	err := n.db.SelectI(ctx, &pendingDeletes, `SELECT
    												psp.piece_id,
    												psp.rm_message_hash,
													mwe.tx_success
												FROM pdp_data_set_pieces psp
												LEFT JOIN message_waits_eth mwe ON mwe.signed_tx_hash = psp.rm_message_hash
												WHERE psp.rm_message_hash IS NOT NULL
												  AND psp.data_set = $1
												  AND psp.removed = FALSE
												  AND mwe.tx_status = 'confirmed'`, dataSetId)
	if err != nil {
		return xerrors.Errorf("failed to select pending piece deletes: %w", err)
	}

	if len(pendingDeletes) == 0 {
		return nil
	}

	pdpAddress := contract.ContractAddressesFor(n.resolvedNetwork()).PDPVerifier

	verifier, err := contract.NewPDPVerifier(pdpAddress, n.ethClient)
	if err != nil {
		return xerrors.Errorf("failed to instantiate PDPVerifier contract: %w", err)
	}

	removals, err := verifier.GetScheduledRemovals(contract.EthCallOpts(ctx), big.NewInt(dataSetId))
	if err != nil {
		return xerrors.Errorf("failed to get scheduled removals: %w", err)
	}

	for _, piece := range pendingDeletes {
		if !piece.TxSuccess.Valid {
			log.Errorf("invalid message_waits_eth state for piece (%d:%d) tx %s neither successful or unsuccessful", dataSetId, piece.PieceID, piece.TxHash)
			_, err := n.db.ExecI(ctx, `UPDATE pdp_data_set_pieces SET rm_message_hash = NULL WHERE data_set = $1 AND piece_id = $2 AND rm_message_hash = $3`, dataSetId, piece.PieceID, piece.TxHash)
			if err != nil {
				return xerrors.Errorf("failed to clear stuck rm_message_hash %s: %w", piece.TxHash, err)
			}
			continue
		}

		if !piece.TxSuccess.Bool {
			log.Errorf("failed to process pending piece delete as transaction %s failed", piece.TxHash)
			_, err := n.db.ExecI(ctx, `UPDATE pdp_data_set_pieces SET rm_message_hash = NULL WHERE data_set = $1 AND piece_id = $2 AND rm_message_hash = $3`, dataSetId, piece.PieceID, piece.TxHash)
			if err != nil {
				return xerrors.Errorf("failed to clear stuck rm_message_hash %s: %w", piece.TxHash, err)
			}
			continue
		}

		pieceID := big.NewInt(piece.PieceID)
		contains := lo.ContainsBy(removals, func(r *big.Int) bool {
			return r.Cmp(pieceID) == 0
		})
		if !contains {
			// Check for the case where next proving period has run and piece deletions fully processed
			live, err := verifier.PieceLive(contract.EthCallOpts(ctx), big.NewInt(dataSetId), pieceID)
			if err != nil {
				return xerrors.Errorf("failed to check if piece is live: %w", err)
			}
			if live {
				log.Warnw("piece is live but not in scheduled removals despite successful delete tx; (possible chain reorg) clearing stale delete tracking",
					"dataSetId", dataSetId, "pieceID", piece.PieceID, "txHash", piece.TxHash)
				_, err := n.db.ExecI(ctx, `UPDATE pdp_data_set_pieces SET rm_message_hash = NULL
                              WHERE data_set = $1 AND piece_id = $2 AND rm_message_hash = $3`,
					dataSetId, piece.PieceID, piece.TxHash)
				if err != nil {
					return xerrors.Errorf("failed to clear stale rm_message_hash: %w", err)
				}
				continue
			}
			log.Infow("piece already removed on-chain, marking as removed in DB", "dataSetId", dataSetId, "pieceID", piece.PieceID, "txHash", piece.TxHash)
		} else {
			log.Infow("noticed scheduled deletion, marking as removed", "dataSetId", dataSetId, "pieceID", piece.PieceID, "txHash", piece.TxHash)
		}

		m, err := n.db.ExecI(ctx, `UPDATE pdp_data_set_pieces
								SET removed = TRUE
								WHERE data_set = $1
								  AND piece_id = $2
								  AND rm_message_hash = $3
								  AND removed = FALSE`, dataSetId, piece.PieceID, piece.TxHash)
		if err != nil {
			return xerrors.Errorf("failed to update pdp_data_set_pieces: %w", err)
		}

		if m != 1 {
			return xerrors.Errorf("expected to update 1 row but updated %d", m)
		}
	}

	return nil
}

// Note: this function needs revisiting if we are ever *shrinking* proving period or challenge window values
func (n *NextProvingPeriodTask) refreshProvingPeriod(ctx context.Context, dataSetId int64, provingSchedule *contract.IPDPProvingSchedule) error {
	config, err := provingSchedule.GetPDPConfig(contract.EthCallOpts(ctx))
	if err != nil {
		return xerrors.Errorf("failed to GetPDPConfig: %w", err)
	}

	_, err = n.db.ExecI(ctx, `UPDATE pdp_data_sets
								SET proving_period = $1,
									challenge_window = $2
								WHERE id = $3
								  AND (proving_period IS DISTINCT FROM $1 OR challenge_window IS DISTINCT FROM $2)`, config.MaxProvingPeriod, config.ChallengeWindow.Uint64(), dataSetId)
	return err
}

func (n *NextProvingPeriodTask) CanAccept(ids []harmonytask.TaskID, engine *harmonytask.TaskEngine) ([]harmonytask.TaskID, error) {
	return ids, nil
}

func (n *NextProvingPeriodTask) TypeDetails() harmonytask.TaskTypeDetails {
	return harmonytask.TaskTypeDetails{
		Name:          tasknames.PDPv0_ProvPeriod,
		TimeSensitive: true,
		MayFollow:     []string{tasknames.PDPv0_Prove},
		Cost: resources.Resources{
			Cpu: 0,
			Gpu: 0,
			Ram: 1 << 20,
		},
		MaxFailures: 3, // Set retry limit to 3 attempts
		RetryWait:   taskhelp.RetryWaitExp(5*time.Second, 2),
	}
}

func (n *NextProvingPeriodTask) Adder(taskFunc harmonytask.AddTaskFunc) {
	n.addFunc.Set(taskFunc)
}

var _ = harmonytask.Reg(&NextProvingPeriodTask{})
