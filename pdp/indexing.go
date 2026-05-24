package pdp

import (
	"context"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/curiostorage/harmonyquery"
	"github.com/filecoin-project/curio/lib/ethchain"
	"github.com/filecoin-project/curio/pdp/contract"
)

// CheckIfIndexingNeeded checks if a data set has the withIPFSIndexing metadata flag.
// Returns true if indexing is needed, false otherwise.
// This is a read-only check that can be done outside a transaction for existing datasets.
//
// network selects the on-chain PDPVerifier address. Pass the empty
// string to fall back to the build-tag-selected default (backward-
// compatible for upstream callers; curio-core passes its configured
// runtime network here).
func CheckIfIndexingNeeded(
	ctx context.Context,
	network contract.Network,
	ethClient ethchain.EthClient,
	dataSetId uint64,
) (bool, error) {
	if network == "" {
		network = contract.NetworkFromBuildType()
	}
	// Get the PDPVerifier contract instance
	pdpVerifier, err := contract.NewPDPVerifier(contract.ContractAddressesFor(network).PDPVerifier, ethClient)
	if err != nil {
		log.Errorw("Failed to instantiate PDPVerifier contract", "error", err, "dataSetId", dataSetId)
		return false, err
	}

	// Get the listener (record keeper) address for this data set
	listenerAddr, err := pdpVerifier.GetDataSetListener(contract.EthCallOpts(ctx), new(big.Int).SetUint64(dataSetId))
	if err != nil {
		log.Errorw("Failed to get listener address for data set", "error", err, "dataSetId", dataSetId)
		return false, err
	}

	// Check if the withIPFSIndexing metadata exists
	mustIndex, _, err := contract.GetDataSetMetadataAtKey(ctx, listenerAddr, ethClient, new(big.Int).SetUint64(dataSetId), "withIPFSIndexing")
	if err != nil {
		// Hard to differentiate between unsupported listener type OR internal error
		// So we log and skip indexing attempt
		log.Warnw("Failed to get data set metadata, skipping indexing", "error", err, "dataSetId", dataSetId)
		return false, nil
	}

	return mustIndex, nil
}

// CheckIfIndexingNeededFromExtraData checks if extraData contains withIPFSIndexing metadata.
// This is used for the CreateDataSet+AddPieces combined operation where the dataset doesn't exist yet.
// The extraData format for combined operations is: (bytes createPayload, bytes addPayload)
// We attempt to decode the createPayload format is is decoded according to the
// FilecoinWarmStorageService format:
//
//	(address payer, uint256 clientDataSetId, string[] keys, string[] values, bytes signature)
func CheckIfIndexingNeededFromExtraData(extraData []byte) (bool, error) {
	if len(extraData) == 0 {
		return false, nil
	}

	// Define the ABI types for nested decoding
	bytes32Type, _ := abi.NewType("bytes", "", nil)
	outerArgs := abi.Arguments{
		{Type: bytes32Type}, // createPayload
		{Type: bytes32Type}, // addPayload
	}

	// Decode outer layer: (bytes createPayload, bytes addPayload)
	decoded, err := outerArgs.Unpack(extraData)
	if err != nil {
		log.Debugw("Failed to decode extraData as combined operation, not an error", "error", err)
		return false, nil
	}

	if len(decoded) < 1 {
		return false, nil
	}

	createPayload, ok := decoded[0].([]byte)
	if !ok {
		log.Debugw("createPayload is not bytes")
		return false, nil
	}

	// Define the ABI types for createPayload decoding
	addressType, _ := abi.NewType("address", "", nil)
	uint256Type, _ := abi.NewType("uint256", "", nil)
	stringArrayType, _ := abi.NewType("string[]", "", nil)
	createArgs := abi.Arguments{
		{Type: addressType},     // payer
		{Type: uint256Type},     // clientDataSetId
		{Type: stringArrayType}, // keys
		{Type: stringArrayType}, // values
		{Type: bytes32Type},     // signature
	}

	// Decode createPayload: (address payer, uint256 clientDataSetId, string[] keys, string[] values, bytes signature)
	createDecoded, err := createArgs.Unpack(createPayload)
	if err != nil {
		log.Debugw("Failed to decode createPayload", "error", err)
		return false, nil
	}

	if len(createDecoded) < 3 {
		return false, nil
	}

	keys, ok := createDecoded[2].([]string)
	if !ok {
		log.Debugw("keys is not []string")
		return false, nil
	}

	// Look for withIPFSIndexing in the keys array
	if slices.Contains(keys, "withIPFSIndexing") {
		log.Debugw("Found withIPFSIndexing in extraData metadata keys")
		return true, nil
	}

	return false, nil
}

// EnableIndexingForPiecesInTx marks the specified piecerefs as needing indexing within a transaction.
//
// SQLite portability: rewritten from `WHERE id = ANY($2)` (Postgres BIGINT[])
// to `WHERE id IN ($2, $3, ...)` with placeholder expansion. SQLite has no
// array type and database/sql can't bind []int64; both backends accept the
// IN-list shape. Same pattern as the d978fd3 claim-query and the 3b77fde
// pdp/handlers_add.go subPieces validation.
func EnableIndexingForPiecesInTx(
	tx harmonyquery.TxInterface,
	serviceLabel string,
	subPieceRefIDs []int64,
) error {
	log.Debugw("Marking subpieces as needing indexing (in transaction)",
		"serviceLabel", serviceLabel,
		"subPieceCount", len(subPieceRefIDs))

	if len(subPieceRefIDs) == 0 {
		return nil
	}

	phList := make([]string, len(subPieceRefIDs))
	args := make([]any, 0, 1+len(subPieceRefIDs))
	args = append(args, serviceLabel)
	for i, id := range subPieceRefIDs {
		phList[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, id)
	}
	_, err := tx.ExecI(harmonyquery.RawString(`
		UPDATE pdp_piecerefs
		SET needs_indexing = TRUE
		WHERE service = $1
			AND id IN (`+strings.Join(phList, ", ")+`)
			AND needs_indexing = FALSE
	`), args...)
	return err
}
