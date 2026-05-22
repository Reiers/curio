//go:build !cgo
// +build !cgo

// Non-CGo companion to sdr_funcs.go. Under CGO_ENABLED=0 the real
// sdr_funcs.go is excluded from the build (it transitively requires
// filecoin-ffi sealing primitives). PDP-only consumers (Curio Core)
// still need the structural surface of *SealCalls and *storageProvider
// so that lib/ffi/piece_funcs.go (which is pure Go) compiles.
//
// This file provides only what piece_funcs.go and task_storage.go
// reference: SealCalls, storageProvider, NewSealCalls, ensureOneCopy,
// plus the `log` package global. The sealing-pipeline methods
// (GenerateSDR, TreeRC, PoRepSnark, etc.) are absent; callers that
// need them must build with CGO_ENABLED=1.

package ffi

import (
	"context"

	logging "github.com/ipfs/go-log/v2"
	"github.com/puzpuzpuz/xsync/v2"
	"github.com/samber/lo"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/curio/harmony/harmonytask"
	"github.com/filecoin-project/curio/lib/paths"
	"github.com/filecoin-project/curio/lib/storiface"
)

var log = logging.Logger("cu/ffi")

// SealCalls — non-CGo build retains the type so callers like piece_funcs.go
// and task_storage.go can define methods on it. Real sealing/proof
// method implementations live in sdr_funcs.go behind //go:build cgo.
type SealCalls struct {
	Sectors *storageProvider
}

// NewSealCalls is the canonical constructor. Identical signature to the
// cgo build.
func NewSealCalls(st *paths.Remote, ls *paths.Local, si paths.SectorIndex) *SealCalls {
	return &SealCalls{
		Sectors: &storageProvider{
			storage:             st,
			localStore:          ls,
			sindex:              si,
			storageReservations: xsync.NewIntegerMapOf[harmonytask.TaskID, []*StorageReservation](),
		},
	}
}

type storageProvider struct {
	storage             *paths.Remote
	localStore          *paths.Local
	sindex              paths.SectorIndex
	storageReservations *xsync.MapOf[harmonytask.TaskID, []*StorageReservation]
}

// AcquireSector — under !cgo we keep the pure-Go body (no sealing-pipeline
// dependencies; this is plain storage acquisition logic). Body mirrors the
// cgo build verbatim except for tracing/log noise that's irrelevant here.
func (l *storageProvider) AcquireSector(ctx context.Context, taskID *harmonytask.TaskID, sector storiface.SectorRef, existing, allocate storiface.SectorFileType, sealing storiface.PathType) (fspaths, ids storiface.SectorPaths, release func(dontDeclare ...storiface.SectorFileType), err error) {
	var sectorPaths, storageIDs storiface.SectorPaths
	var releaseStorage func()

	var ok bool
	var resv *StorageReservation
	if taskID != nil {
		resvs, rok := l.storageReservations.Load(*taskID)
		if rok {
			resv, ok = lo.Find(resvs, func(res *StorageReservation) bool {
				return res.SectorRef.ID() == sector.ID
			})
		}
	}
	if ok && resv != nil {
		if resv.Alloc != allocate || resv.Existing != existing {
			return storiface.SectorPaths{}, storiface.SectorPaths{}, nil, xerrors.Errorf("storage reservation type mismatch")
		}
		sectorPaths = resv.Paths
		storageIDs = resv.PathIDs
		releaseStorage = resv.Release
	} else {
		sectorPaths, storageIDs, err = l.localStore.AcquireSector(ctx, sector, existing, allocate, sealing, storiface.AcquireMove)
		if err != nil {
			return storiface.SectorPaths{}, storiface.SectorPaths{}, nil, xerrors.Errorf("acquire sector: %w", err)
		}
		releaseStorage = func() {}
	}

	return sectorPaths, storageIDs, func(dontDeclare ...storiface.SectorFileType) {
		releaseStorage()
	}, nil
}

// ensureOneCopy is referenced by piece_funcs.go. Pure-Go path-keep logic
// (no CGo). Behaviour mirrors the cgo build's implementation.
func (sb *SealCalls) ensureOneCopy(ctx context.Context, sid abi.SectorID, pathIDs storiface.SectorPaths, fts storiface.SectorFileType) error {
	if !pathIDs.HasAllSet(fts) {
		return xerrors.Errorf("ensure one copy: not all paths are set")
	}

	for _, fileType := range fts.AllSet() {
		pid := storiface.PathByType(pathIDs, fileType)
		keepIn := []storiface.ID{storiface.ID(pid)}

		if err := sb.Sectors.storage.Remove(ctx, sid, fileType, true, keepIn); err != nil {
			return err
		}
	}

	return nil
}

// silence the unused-import linter when nothing in this file touches
// abi/log directly; both are kept available for piece_funcs.go.
var _ = abi.ChainEpoch(0)
var _ = log
