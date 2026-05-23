// piece_cid.go — backward-compat forwarding shim.
//
// The actual implementations of PadPieceSize, ParsePieceCid, etc. moved
// to the sub-package curio/pdp/piececid so that pure-Go consumers
// (curio-core's pdpv0 task code, builders, anything that only needs the
// piece-CID utility functions) can import them without dragging the
// heavyweight curio/pdp package — which transitively pulls
// lotus/storage/sealer via curio/deps.
//
// Existing call sites in curio's heavy paths (HTTP handlers etc.) keep
// working through these type aliases + thin wrapper functions. New
// callers should import curio/pdp/piececid directly.

package pdp

import (
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/curio/pdp/piececid"
)

// PieceCidInfo re-exported from the piececid sub-package.
type PieceCidInfo = piececid.PieceCidInfo

// ParsePieceCid forwards to piececid.ParsePieceCid.
func ParsePieceCid(cidStr string) (*PieceCidInfo, error) {
	return piececid.ParsePieceCid(cidStr)
}

// ParsePieceCidV2 forwards to piececid.ParsePieceCidV2.
func ParsePieceCidV2(cidStr string) (*PieceCidInfo, error) {
	return piececid.ParsePieceCidV2(cidStr)
}

// PieceCidV2FromV1 forwards to piececid.PieceCidV2FromV1.
func PieceCidV2FromV1(v1 cid.Cid, rawSize uint64) (*PieceCidInfo, error) {
	return piececid.PieceCidV2FromV1(v1, rawSize)
}

// PieceCidV2FromV1Str forwards to piececid.PieceCidV2FromV1Str.
func PieceCidV2FromV1Str(v1Str string, rawSize uint64) (*PieceCidInfo, error) {
	return piececid.PieceCidV2FromV1Str(v1Str, rawSize)
}

// PadPieceSize forwards to piececid.PadPieceSize.
func PadPieceSize(rawSize int64) int64 {
	return piececid.PadPieceSize(rawSize)
}
