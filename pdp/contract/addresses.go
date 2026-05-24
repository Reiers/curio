package contract

import (
	"math/big"
	"os"
	"slices"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/snadrus/must"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/curio/build"

	"github.com/filecoin-project/lotus/chain/types"
)

type PDPContracts struct {
	PDPVerifier                common.Address
	AllowedPublicRecordKeepers RecordKeeperAddresses
}

type RecordKeeperAddresses struct {
	FWSService common.Address
	Simple     common.Address
}

// Network identifies the Filecoin network for contract-address lookup.
// String-typed to match the runtime configuration shape consumers like
// curio-core use (mirrors github.com/Reiers/lantern/build.Network).
//
// The legacy build-tag-gated APIs (ContractAddresses, ServiceRegistryAddress,
// USDFCAddress with no arguments) still work and map BuildType -> Network
// internally. Runtime-configurable consumers should use the *For variants.
type Network string

const (
	NetworkMainnet     Network = "mainnet"
	NetworkCalibration Network = "calibration"
	NetworkDevnet      Network = "devnet" // 2k + debug; addresses sourced from env
)

// lazyValue holds a value that is loaded exactly once on first access.
type lazyValue[T any] struct {
	once  sync.Once
	value T
	err   error
}

// get loads the value on first call, then returns the cached result.
func (l *lazyValue[T]) get(loader func() (T, error)) (T, error) {
	l.once.Do(func() {
		l.value, l.err = loader()
	})
	return l.value, l.err
}

var (
	pdpContracts    lazyValue[PDPContracts]
	serviceRegistry lazyValue[common.Address]
	usdfc           lazyValue[common.Address]
)

func (a RecordKeeperAddresses) List() []common.Address {
	return []common.Address{a.FWSService, a.Simple}
}

// NetworkFromBuildType maps the package-init-set BuildType (controlled
// by Go build tags) to the runtime-shape Network string. Used by the
// legacy no-argument APIs (ContractAddresses, ServiceRegistryAddress,
// USDFCAddress) for backward compatibility with upstream consumers
// that build with -tags mainnet/calibnet/2k/debug.
//
// Returns an empty string when BuildType is the zero value (uninitialised),
// which happens in unit-test contexts that don't link any params_*.go file.
func NetworkFromBuildType() Network {
	switch build.BuildType {
	case build.BuildMainnet:
		return NetworkMainnet
	case build.BuildCalibnet:
		return NetworkCalibration
	case build.Build2k, build.BuildDebug:
		return NetworkDevnet
	default:
		return ""
	}
}

// ContractAddressesFor returns the PDPVerifier + FWSService addresses
// for the given network. Preferred over ContractAddresses() at call
// sites that can determine the network at runtime (e.g. from a daemon
// config or a CLI flag).
func ContractAddressesFor(network Network) PDPContracts {
	switch network {
	case NetworkCalibration:
		return PDPContracts{
			PDPVerifier: common.HexToAddress("0x85e366Cf9DD2c0aE37E963d9556F5f4718d6417C"), // PDPVerifier Proxy v3.1.0
			AllowedPublicRecordKeepers: RecordKeeperAddresses{
				FWSService: common.HexToAddress("0x02925630df557F957f70E112bA06e50965417CA0"), // FWSS Proxy
			},
		}
	case NetworkMainnet:
		return PDPContracts{
			PDPVerifier: common.HexToAddress("0xBADd0B92C1c71d02E7d520f64c0876538fa2557F"), // PDPVerifier Proxy v3.1.0
			AllowedPublicRecordKeepers: RecordKeeperAddresses{
				FWSService: common.HexToAddress("0x8408502033C418E1bbC97cE9ac48E5528F371A9f"), // FWSS Proxy
			},
		}
	case NetworkDevnet:
		result, err := pdpContracts.get(func() (PDPContracts, error) {
			pdpVerifier := os.Getenv("CURIO_DEVNET_PDP_VERIFIER_ADDRESS")
			if pdpVerifier == "" {
				return PDPContracts{}, xerrors.Errorf("PDP verifier address not configured for devnet - set CURIO_DEVNET_PDP_VERIFIER_ADDRESS env var")
			}
			fwsService := os.Getenv("CURIO_DEVNET_FWSS_ADDRESS")
			if fwsService == "" {
				return PDPContracts{}, xerrors.Errorf("FWSS address not configured for devnet - set CURIO_DEVNET_FWSS_ADDRESS env var")
			}

			contracts := PDPContracts{
				PDPVerifier: common.HexToAddress(pdpVerifier),
				AllowedPublicRecordKeepers: RecordKeeperAddresses{
					FWSService: common.HexToAddress(fwsService),
				},
			}

			// Simple record keeper is optional
			if simple := os.Getenv("CURIO_DEVNET_RECORD_KEEPER_SIMPLE_ADDRESS"); simple != "" {
				contracts.AllowedPublicRecordKeepers.Simple = common.HexToAddress(simple)
			}

			return contracts, nil
		})
		if err != nil {
			panic(err)
		}
		return result
	default:
		panic("PDP contract unknown for network: " + string(network))
	}
}

// ContractAddresses returns the PDPVerifier + FWSService for the
// build-tag-selected network. Preserved for upstream callers that
// haven't migrated to ContractAddressesFor.
func ContractAddresses() PDPContracts {
	return ContractAddressesFor(NetworkFromBuildType())
}

const NumChallenges = 5

func SybilFee() *big.Int {
	return must.One(types.ParseFIL("0.1")).Int
}

// IsPublicService checks if a service label indicates a public service
func IsPublicService(serviceLabel string) bool {
	return serviceLabel == "public"
}

// IsRecordKeeperAllowed checks if a recordkeeper address is in the
// build-tag-selected network's whitelist. Backward-compatible wrapper;
// callers with runtime network info should use IsRecordKeeperAllowedFor.
func IsRecordKeeperAllowed(recordKeeper common.Address) bool {
	return slices.Contains(ContractAddresses().AllowedPublicRecordKeepers.List(), recordKeeper)
}

// IsRecordKeeperAllowedFor is the network-aware variant. Use at call
// sites that know the runtime network (HTTP handlers reading from a
// per-request context, schedulers parameterised on daemon config).
func IsRecordKeeperAllowedFor(network Network, recordKeeper common.Address) bool {
	return slices.Contains(ContractAddressesFor(network).AllowedPublicRecordKeepers.List(), recordKeeper)
}

const ServiceRegistryMainnet = "0xf55dDbf63F1b55c3F1D4FA7e339a68AB7b64A5eB"  // ServiceProviderRegistry Proxy
const ServiceRegistryCalibnet = "0x839e5c9988e4e9977d40708d0094103c0839Ac9D" // ServiceProviderRegistry Proxy

// ServiceRegistryAddressFor returns the ServiceProviderRegistry proxy
// address for the given network. Network-aware variant of
// ServiceRegistryAddress; preferred at call sites with runtime network.
func ServiceRegistryAddressFor(network Network) (common.Address, error) {
	switch network {
	case NetworkCalibration:
		return common.HexToAddress(ServiceRegistryCalibnet), nil
	case NetworkMainnet:
		return common.HexToAddress(ServiceRegistryMainnet), nil
	case NetworkDevnet:
		return serviceRegistry.get(func() (common.Address, error) {
			if addr := os.Getenv("CURIO_DEVNET_SERVICE_REGISTRY_ADDRESS"); addr != "" {
				return common.HexToAddress(addr), nil
			}
			return common.Address{}, xerrors.Errorf("service registry address not configured for devnet - set CURIO_DEVNET_SERVICE_REGISTRY_ADDRESS env var")
		})
	default:
		return common.Address{}, xerrors.Errorf("service registry address not set for network %s", network)
	}
}

// ServiceRegistryAddress returns the registry for the build-tag-selected
// network. Preserved for legacy upstream callers.
func ServiceRegistryAddress() (common.Address, error) {
	return ServiceRegistryAddressFor(NetworkFromBuildType())
}

const USDFCAddressMainnet = "0x80B98d3aa09ffff255c3ba4A241111Ff1262F045"
const USDFCAddressCalibnet = "0xb3042734b608a1B16e9e86B374A3f3e389B4cDf0"

// USDFCAddressFor returns the USDFC token address for the given network.
// Network-aware variant of USDFCAddress.
func USDFCAddressFor(network Network) (common.Address, error) {
	switch network {
	case NetworkCalibration:
		return common.HexToAddress(USDFCAddressCalibnet), nil
	case NetworkMainnet:
		return common.HexToAddress(USDFCAddressMainnet), nil
	case NetworkDevnet:
		return usdfc.get(func() (common.Address, error) {
			if addr := os.Getenv("CURIO_DEVNET_USDFC_ADDRESS"); addr != "" {
				return common.HexToAddress(addr), nil
			}
			return common.Address{}, xerrors.Errorf("USDFC address not configured for devnet - set CURIO_DEVNET_USDFC_ADDRESS env var")
		})
	default:
		return common.Address{}, xerrors.Errorf("USDFC address not set for network %s", network)
	}
}

// USDFCAddress returns the USDFC token address for the build-tag-selected
// network. Preserved for legacy upstream callers.
func USDFCAddress() (common.Address, error) {
	return USDFCAddressFor(NetworkFromBuildType())
}
