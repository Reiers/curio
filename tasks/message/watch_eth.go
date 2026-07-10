package message

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/curiostorage/harmonyquery"
	"github.com/filecoin-project/curio/harmony/harmonytask"
	"github.com/filecoin-project/curio/harmony/resources"
	"github.com/filecoin-project/curio/lib/chainsched"
	"github.com/filecoin-project/curio/lib/ethchain"

	types2 "github.com/filecoin-project/lotus/chain/types"
)

const (
	// defaultEthCallTimeout is the timeout for sets of Ethereum client calls per transaction
	// (i.e. receipt and transaction data)
	defaultEthCallTimeout = 30 * time.Second
	MinEthConfidence      = 1
)

// EthClient is an interface for the Ethereum client operations we need
type EthClient interface {
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error)
	TransactionByHash(ctx context.Context, txHash common.Hash) (tx *types.Transaction, isPending bool, err error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	// FilterLogs is used as a fallback to backfill logs on receipts whose
	// eth_getTransactionReceipt response arrived with an empty Logs slice.
	// The curio-core embedded Lantern's locally-served receipt path
	// (lantern#45 Stage 5) omits logs; downstream pdpv0 watchers
	// (DataSetCreated / DataSetDeleted / add-piece) need them to extract
	// event params. Backfill via eth_getLogs scoped to the receipt's block
	// number keeps the on-disk tx_receipt JSON self-sufficient.
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
}

// TaskEngine is an interface for the parts of harmonytask.TaskEngine we need
type TaskEngine interface {
	ResourcesAvailable() resources.Resources
}

type MessageWatcherEth struct {
	txMgr EthTransactionManager
	ht    TaskEngine
	api   EthClient

	stopping, stopped chan struct{}

	updateCh        chan struct{}
	bestBlockNumber atomic.Pointer[big.Int]

	// ethCallTimeout is the timeout for Ethereum client calls
	ethCallTimeout time.Duration
}

func NewMessageWatcherEth(db harmonyquery.DBInterface, ht *harmonytask.TaskEngine, pcs *chainsched.CurioChainSched, api ethchain.EthClient) (*MessageWatcherEth, error) {
	mw := &MessageWatcherEth{
		txMgr:          NewHarmonyEthTxManager(db),
		ht:             ht,
		api:            api,
		stopping:       make(chan struct{}),
		stopped:        make(chan struct{}),
		updateCh:       make(chan struct{}, 1),
		ethCallTimeout: defaultEthCallTimeout,
	}
	go mw.run()
	if err := pcs.AddWatcher(mw.processHeadChange); err != nil {
		return nil, err
	}
	return mw, nil
}

func (mw *MessageWatcherEth) run() {
	defer close(mw.stopped)
	log.Infow("MessageWatcherEth started")

	for {
		select {
		case <-mw.stopping:
			log.Infow("MessageWatcherEth stopping")
			// TODO: cleanup assignments
			return
		case <-mw.updateCh:
			log.Debugw("MessageWatcherEth update triggered")
			mw.update()
		}
	}
}

func (mw *MessageWatcherEth) update() {
	ctx := context.Background()

	bestBlockNumber := mw.bestBlockNumber.Load()
	log.Debugw("MessageWatcherEth update starting", "bestBlockNumber", bestBlockNumber)

	confirmedBlockNumber := new(big.Int).Sub(bestBlockNumber, big.NewInt(MinEthConfidence))
	if confirmedBlockNumber.Sign() < 0 {
		// Not enough blocks yet
		log.Debugw("Not enough blocks for confirmations", "bestBlockNumber", bestBlockNumber, "minConfidence", MinEthConfidence)
		return
	}

	machineID := mw.ht.ResourcesAvailable().MachineID

	// Assign pending transactions with null owner to ourselves
	{
		n, err := mw.txMgr.AssignPendingToMachine(ctx, int64(machineID))
		if err != nil {
			log.Errorf("failed to assign pending transactions: %+v", err)
			return
		}
		if n > 0 {
			log.Infow("Assigned orphaned pending transactions", "count", n, "machineID", machineID)
		}
	}

	// Get transactions assigned to us
	txHashes, err := mw.txMgr.GetPendingForMachine(ctx, int64(machineID))
	if err != nil {
		log.Errorf("failed to get assigned transactions: %+v", err)
		return
	}

	log.Infow("Processing pending transactions", "count", len(txHashes), "machineID", machineID)

	// Track statistics
	var processed, stillPending, waitingConfirmations, confirmed, errorCount int

	// Check if any of the transactions we have assigned are now confirmed
	for _, txHashStr := range txHashes {
		processed++
		processStart := time.Now()

		txHash := common.HexToHash(txHashStr)
		log.Debugw("Checking transaction", "txHash", txHash.Hex())

		ethCtx, ethCancel := context.WithTimeout(ctx, mw.ethCallTimeout)

		receipt, err := mw.api.TransactionReceipt(ethCtx, txHash)
		if err != nil {
			ethCancel()
			if errors.Is(err, ethereum.NotFound) {
				// Transaction is still pending
				stillPending++
				log.Debugw("Transaction still pending", "txHash", txHash.Hex())
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) {
				errorCount++
				log.Debugw("Eth calls timed out - continuing with next tx",
					"txHash", txHash.Hex())
				continue
			}
			errorCount++
			log.Errorw("Failed to get transaction receipt - continuing with next tx",
				"txHash", txHash.Hex(),
				"error", err,
				"errorType", fmt.Sprintf("%T", err))
			continue
		}

		// Transaction receipt found
		log.Debugw("Transaction receipt found",
			"txHash", txHash.Hex(),
			"blockNumber", receipt.BlockNumber,
			"status", receipt.Status)

		// Check if the transaction has enough confirmations
		confirmations := new(big.Int).Sub(bestBlockNumber, receipt.BlockNumber)
		if confirmations.Cmp(big.NewInt(MinEthConfidence)) < 0 {
			ethCancel()
			// Not enough confirmations yet
			waitingConfirmations++
			log.Debugw("Transaction waiting for confirmations",
				"txHash", txHash.Hex(),
				"confirmations", confirmations,
				"required", MinEthConfidence,
				"blockNumber", receipt.BlockNumber)
			continue
		}

		// Get the transaction data using same timeout context
		txData, _, err := mw.api.TransactionByHash(ethCtx, txHash)
		ethCancel()

		if err != nil {
			if errors.Is(err, ethereum.NotFound) {
				errorCount++
				log.Errorw("Transaction data not found - continuing", "txHash", txHash.Hex())
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) {
				errorCount++
				log.Debugw("Eth calls timed out - continuing with next tx",
					"txHash", txHash.Hex())
				continue
			}
			errorCount++
			log.Errorw("Failed to get transaction by hash - continuing",
				"txHash", txHash.Hex(),
				"error", err)
			continue
		}

		txDataJSON, err := json.Marshal(txData)
		if err != nil {
			errorCount++
			log.Errorw("Failed to marshal transaction data - continuing",
				"txHash", txHash.Hex(),
				"error", err)
			continue
		}

		// Backfill Logs on receipts arriving with an empty slice. Motivation:
		// curio-core's embedded Lantern (lantern#45 Stage 5) serves a
		// locally-resolved eth_getTransactionReceipt for txs it broadcast,
		// and that shape hardcodes `logs: []` (Lantern V1 doesn't decode
		// events from Filecoin's message receipts). Downstream pdpv0
		// watchers parse DataSetCreated / DataSetDeleted / add-piece events
		// off receipt.Logs and silently stall ("event not found in receipt")
		// if this slice is empty. We fetch the missing logs via eth_getLogs
		// scoped to the receipt's block, filter to logs emitted by this tx,
		// and stitch them onto the in-memory receipt before persisting so
		// the stored JSON is self-sufficient. Guarded by Status==Successful:
		// reverted txs don't emit logs, and we don't want to spin extra RPCs
		// on failed transactions. Non-Lantern deployments already return
		// populated Logs and the backfill is a no-op (len==0 && bloom==0 =>
		// FilterLogs returns [] and we skip the reassignment).
		if receipt.Status == types.ReceiptStatusSuccessful && len(receipt.Logs) == 0 && receipt.BlockNumber != nil && receipt.BlockNumber.Sign() > 0 {
			if backfilled, ferr := mw.backfillReceiptLogs(ctx, receipt); ferr != nil {
				log.Warnw("receipt logs backfill failed - persisting empty-logs receipt",
					"txHash", txHash.Hex(),
					"blockNumber", receipt.BlockNumber,
					"error", ferr)
			} else if len(backfilled) > 0 {
				log.Infow("backfilled receipt logs from eth_getLogs",
					"txHash", txHash.Hex(),
					"blockNumber", receipt.BlockNumber,
					"count", len(backfilled))
				receipt.Logs = backfilled
			}
		}

		receiptJSON, err := json.Marshal(receipt)
		if err != nil {
			errorCount++
			log.Errorw("Failed to marshal receipt data - continuing",
				"txHash", txHash.Hex(),
				"error", err)
			continue
		}

		txSuccess := receipt.Status == 1

		log.Infow("Updating transaction to confirmed",
			"txHash", txHash.Hex(),
			"success", txSuccess,
			"blockNumber", receipt.BlockNumber,
			"confirmations", confirmations)

		// Update the database
		err = mw.txMgr.UpdateToConfirmed(ctx,
			txHashStr,
			receipt.BlockNumber.Int64(),
			receipt.TxHash.Hex(),
			txDataJSON,
			receiptJSON,
			txSuccess,
		)
		if err != nil {
			errorCount++
			log.Errorw("Failed to update message wait - continuing",
				"txHash", txHash.Hex(),
				"error", err)
			continue
		}

		confirmed++
		log.Infow("Successfully confirmed transaction", "txHash", txHash.Hex())

		processDuration := time.Since(processStart)
		if processDuration > 5*time.Second {
			log.Warnw("Transaction processing took longer than expected",
				"txHash", txHash.Hex(),
				"duration", processDuration)
		}
	}

	log.Debugw("MessageWatcherEth update completed",
		"processed", processed,
		"confirmed", confirmed,
		"stillPending", stillPending,
		"waitingConfirmations", waitingConfirmations,
		"errors", errorCount,
		"total", len(txHashes))
}

// backfillReceiptLogs fetches logs for the tx via eth_getLogs and returns
// only those emitted by this transaction, ordered by log index. Uses
// FromBlock/ToBlock (numeric) rather than BlockHash because Lantern's
// locally-served receipts synthesise a non-consensus blockHash that the
// upstream bridge's eth_getLogs won't match. eth_getLogs itself still
// forwards to the VMBridge in Lantern, so it returns real event data.
func (mw *MessageWatcherEth) backfillReceiptLogs(ctx context.Context, receipt *types.Receipt) ([]*types.Log, error) {
	if receipt == nil || receipt.BlockNumber == nil {
		return nil, nil
	}
	blockNum := new(big.Int).Set(receipt.BlockNumber)
	logs, err := mw.api.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: blockNum,
		ToBlock:   blockNum,
	})
	if err != nil {
		return nil, fmt.Errorf("eth_getLogs block=%s: %w", blockNum, err)
	}
	txHash := receipt.TxHash
	var matched []*types.Log
	for i := range logs {
		if logs[i].TxHash != txHash {
			continue
		}
		log := logs[i]
		matched = append(matched, &log)
	}
	if len(matched) > 1 {
		sort.Slice(matched, func(i, j int) bool { return matched[i].Index < matched[j].Index })
	}
	return matched, nil
}

func (mw *MessageWatcherEth) Stop(ctx context.Context) error {
	close(mw.stopping)
	select {
	case <-mw.stopped:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (mw *MessageWatcherEth) processHeadChange(ctx context.Context, revert, apply *types2.TipSet) error {
	if apply != nil {
		height := apply.Height()
		mw.bestBlockNumber.Store(big.NewInt(int64(height)))
		log.Debugw("Head change received", "newHeight", height)
		select {
		case mw.updateCh <- struct{}{}:
			log.Debugw("Update triggered by head change", "height", height)
		default:
			log.Debugw("Update channel full, skipping", "height", height)
		}
	}
	return nil
}
