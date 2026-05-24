package filecoinpayment

import (
	"os"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/curio/build"
	"github.com/filecoin-project/curio/pdp/contract"
)

const PaymentContractMainnet = "0x23b1e018F08BB982348b15a86ee926eEBf7F4DAa"
const PaymentContractCalibnet = "0x09a0fDc2723fAd1A7b8e3e00eE5DF73841df55a0"

// PaymentContractAddressFor returns the FilecoinPayV1 contract address
// for the given network. Network-aware variant of PaymentContractAddress.
// Curio Core uses this with its runtime --network flag.
func PaymentContractAddressFor(network contract.Network) (common.Address, error) {
	switch network {
	case contract.NetworkCalibration:
		return common.HexToAddress(PaymentContractCalibnet), nil
	case contract.NetworkMainnet:
		return common.HexToAddress(PaymentContractMainnet), nil
	case contract.NetworkDevnet:
		if addr := os.Getenv("CURIO_DEVNET_PAYMENTS_ADDRESS"); addr != "" {
			return common.HexToAddress(addr), nil
		}
		return common.Address{}, xerrors.Errorf("payment contract address not configured for devnet - set CURIO_DEVNET_PAYMENTS_ADDRESS env var")
	default:
		return common.Address{}, xerrors.Errorf("payment contract address not set for network %s", network)
	}
}

// PaymentContractAddress returns the FilecoinPayV1 address for the
// build-tag-selected network. Preserved for legacy upstream callers
// that don't know their network at runtime.
func PaymentContractAddress() (common.Address, error) {
	switch build.BuildType {
	case build.BuildCalibnet:
		return PaymentContractAddressFor(contract.NetworkCalibration)
	case build.BuildMainnet:
		return PaymentContractAddressFor(contract.NetworkMainnet)
	case build.Build2k, build.BuildDebug:
		return PaymentContractAddressFor(contract.NetworkDevnet)
	default:
		return common.Address{}, xerrors.Errorf("payment contract address not set for this network %s", build.BuildTypeString()[1:])
	}
}
