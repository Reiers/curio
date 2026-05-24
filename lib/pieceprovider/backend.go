package pieceprovider

import (
	"context"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/curio/lib/storiface"
)

// PieceParkBackend is the minimum interface surface lib/cachedreader
// uses against a piece-park reader. *PieceParkReader satisfies this
// naturally (via the paths.Remote + paths.SectorIndex cluster-aware
// storage layer). Alternative single-node implementations \u2014 notably
// curio-core's local-file piece-park reader, which serves bytes
// directly from the streaming-upload stash without the cluster
// abstraction \u2014 satisfy the interface via this method.
//
// Surface: one method. The cachedreader call site at
// lib/cachedreader/cachedreader.go calls only ReadPiece; no other
// state on the concrete PieceParkReader type is observed.
type PieceParkBackend interface {
	// ReadPiece returns a storiface.Reader over the bytes of the given
	// piece, identified by its parked_pieces.id (encoded as
	// storiface.PieceNumber). pieceSize is the raw (unpadded) size in
	// bytes; pc is the piece's content CID (PieceCIDv1 by callers in
	// the cachedreader path).
	//
	// Returns an error if the piece can't be found, can't be opened,
	// or the size doesn't match expectations.
	ReadPiece(ctx context.Context, pieceParkID storiface.PieceNumber, pieceSize int64, pc cid.Cid) (storiface.Reader, error)
}

// Compile-time guard: *PieceParkReader must satisfy PieceParkBackend
// so existing upstream callers can be migrated to the interface
// incrementally without code change at the use site.
var _ PieceParkBackend = (*PieceParkReader)(nil)
