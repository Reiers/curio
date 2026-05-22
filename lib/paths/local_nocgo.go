//go:build !cgo
// +build !cgo

// Stub implementations of the four CGo-dependent methods on *Local so
// that lib/paths satisfies the Store interface under CGO_ENABLED=0.
// These methods are not exercised by PDP-only consumers (Curio Core);
// any caller that invokes them under !cgo gets a clear error pointing
// at the missing build configuration.

package paths

import (
	"context"
	"errors"

	"github.com/filecoin-project/curio/lib/storiface"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
)

var errNotBuiltWithCGo = errors.New("paths: this method requires building with CGO_ENABLED=1 (filecoin-ffi linkage)")

func (st *Local) GenerateSingleVanillaProof(ctx context.Context, minerID abi.ActorID, si storiface.PostSectorChallenge, ppt abi.RegisteredPoStProof) ([]byte, error) {
	_ = ctx
	_ = minerID
	_ = si
	_ = ppt
	return nil, errNotBuiltWithCGo
}

func (st *Local) GeneratePoRepVanillaProof(ctx context.Context, sr storiface.SectorRef, sealed, unsealed cid.Cid, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness) ([]byte, error) {
	_ = ctx
	_ = sr
	_ = sealed
	_ = unsealed
	_ = ticket
	_ = seed
	return nil, errNotBuiltWithCGo
}

func (st *Local) ReadSnapVanillaProof(ctx context.Context, sr storiface.SectorRef) ([]byte, error) {
	_ = ctx
	_ = sr
	return nil, errNotBuiltWithCGo
}
