package pdpv0

import (
	"context"

	"github.com/curiostorage/harmonyquery"
	"github.com/filecoin-project/curio/lib/ethchain"
	"github.com/filecoin-project/curio/lib/paths/alertinginterface"
	"github.com/filecoin-project/curio/pdp/contract"

	chainTypes "github.com/filecoin-project/lotus/chain/types"
)

const (
	alertType              = "PDPV0"
	alertNameCreateDataSet = "CreateDataSet"
	alertNameAddPiece      = "AddPiece"
)

// NewDataSetWatch runs processing steps for data set creation and piece addition.
// These two are run in sequence to allow for combined create-and-add flow to first
// create the data set, then add the pieces to it.
//
// network selects the on-chain contract addresses; pass the empty
// string to fall back to contract.NetworkFromBuildType().
func NewDataSetWatch(w *Watcher, network contract.Network) {
	if network == "" {
		network = contract.NetworkFromBuildType()
	}
	if err := w.AddWatcher(func(ctx context.Context, db harmonyquery.DBInterface, ethClient ethchain.EthClient, al alertinginterface.AlertingInterface, revert, apply *chainTypes.TipSet) {
		cat := al.AddAlertType(alertNameCreateDataSet, alertType)
		err := processPendingDataSetCreates(ctx, network, db, ethClient)
		if err != nil {
			log.Errorf("Failed to process pending data set creates: %v", err)
			al.Raise(cat, map[string]interface{}{
				"error": err.Error(),
			})
		}

		adat := al.AddAlertType(alertNameAddPiece, alertType)
		err = processPendingDataSetPieceAdds(ctx, network, db, ethClient)
		if err != nil {
			log.Errorf("Failed to process pending data set piece adds: %v", err)
			al.Raise(adat, map[string]interface{}{
				"error": err.Error(),
			})
		}
	}, WatcherOrderCreateAndAdd); err != nil {
		panic(err)
	}
}
