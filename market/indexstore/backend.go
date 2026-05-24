package indexstore

import (
	"context"

	"github.com/ipfs/go-cid"
)

// Backend is the minimum interface surface that the curio code paths
// outside the production Cassandra cluster (notably curio-core's
// embedded SQLite shape) need to satisfy. The upstream concrete
// *IndexStore satisfies this interface naturally; alternative
// implementations (curio-core's internal/sqliteindex) satisfy it via
// methods with matching signatures.
//
// Scope: only the methods the active pdpv0 + cachedreader code paths
// call today. The full *IndexStore type has ~18 methods covering
// payload-to-piece indexing, IPNI, aggregate writes, etc.; alternative
// backends don't need those for hot-storage-only deployments.
//
// Method-set summary:
//
//	AddPDPLayer            ProveTask cache-write path
//	GetPDPLayer            ProveTask proof-generation
//	GetPDPLayerIndex       SaveCache idempotency check + ProveTask layer lookup
//	GetPDPNode             ProveTask single-leaf reads
//	RemoveIndexes          Piece-deletion cleanup
//	FindPieceInAggregate   cachedreader fallthrough (mk20-aware; pdpv0
//	                        backends return empty)
//
// All NodeDigest / Record / cid.Cid types are defined in this package
// and shared across backends.
type Backend interface {
	// AddPDPLayer inserts a precomputed Merkle layer for a piece.
	AddPDPLayer(ctx context.Context, pieceCidV2 cid.Cid, layer []NodeDigest) error

	// GetPDPLayer returns all leaves for (piece, layer), sorted by
	// leaf index ascending.
	GetPDPLayer(ctx context.Context, pieceCidV2 cid.Cid, layerIdx int) ([]NodeDigest, error)

	// GetPDPLayerIndex reports whether the piece has a cached layer
	// and returns its layer index. (has=false, idx=0, nil) on absent.
	GetPDPLayerIndex(ctx context.Context, pieceCidV2 cid.Cid) (bool, int, error)

	// GetPDPNode returns a single leaf. (has=false, nil, nil) on absent.
	GetPDPNode(ctx context.Context, pieceCidV2 cid.Cid, layerIdx int, index int64) (bool, *NodeDigest, error)

	// RemoveIndexes drops all index rows for a piece. Best-effort
	// across whatever index tables the backend maintains.
	RemoveIndexes(ctx context.Context, pieceCidV2 cid.Cid) error

	// FindPieceInAggregate looks up the aggregate-piece mapping for
	// the given piece. mk20-only; backends used by pdpv0-only
	// deployments return an empty slice cleanly.
	FindPieceInAggregate(ctx context.Context, pieceCid cid.Cid) ([]Record, error)
}

// Compile-time guard: *IndexStore must satisfy Backend so existing
// upstream callers can be migrated to the interface incrementally
// without code change at the use site (Backend = *IndexStore).
var _ Backend = (*IndexStore)(nil)
