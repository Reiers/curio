package pdpv0

import (
	"context"

	"github.com/curiostorage/harmonyquery"
	"github.com/filecoin-project/curio/lib/chainsched"
	"github.com/filecoin-project/curio/lib/ethchain"
	"github.com/filecoin-project/curio/pdp/contract"

	chainTypes "github.com/filecoin-project/lotus/chain/types"
)

// NewDataSetWatch runs processing steps for data set creation and piece addition.
// These two are run in sequence to allow for combined create-and-add flow to first
// create the data set, then add the pieces to it.
//
// network selects the on-chain contract addresses; pass the empty
// string to fall back to contract.NetworkFromBuildType().
func NewDataSetWatch(db harmonyquery.DBInterface, ethClient ethchain.EthClient, pcs *chainsched.CurioChainSched, network contract.Network) {
	if network == "" {
		network = contract.NetworkFromBuildType()
	}
	if err := pcs.AddHandler(func(ctx context.Context, revert, apply *chainTypes.TipSet) error {
		err := processPendingDataSetCreates(ctx, network, db, ethClient)
		if err != nil {
			log.Warnf("Failed to process pending data set creates: %v", err)
		}

		err = processPendingDataSetPieceAdds(ctx, network, db, ethClient)
		if err != nil {
			log.Warnf("Failed to process pending data set piece adds: %v", err)
		}
		return nil
	}); err != nil {
		panic(err)
	}
}
