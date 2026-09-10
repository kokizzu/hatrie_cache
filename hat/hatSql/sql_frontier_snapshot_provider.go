package hatSql

import (
	"context"
	"errors"
)

var ErrSQLFrontierSnapshotProviderUnsupported = errors.New("hatSql: SQL frontier snapshot provider is unsupported")

// SQLFrontierSnapshotProvider is the explicit storage contract for opening an
// immutable SQL view at a caller-selected source frontier.
//
// Implementations must atomically bind every source read in the returned view
// to frontier. The ordinary SQLSnapshotProvider contract remains unchanged.
type SQLFrontierSnapshotProvider interface {
	SQLSourceResolver

	BeginSQLSnapshotAt(ctx context.Context, frontier uint64) (resolver SQLSourceResolver, release func(), err error)
}

// BeginSQLFrontierSnapshot waits for every configured source partition to
// observe frontier, then asks resolver for a snapshot explicitly bound to that
// frontier. It is opt-in and does not change ordinary SQL execution.
func BeginSQLFrontierSnapshot(ctx context.Context, resolver SQLSourceResolver, barrier *SQLSourceFrontierBarrier, frontier uint64) (SQLSourceResolver, func(), error) {
	if barrier == nil {
		return nil, nil, ErrSQLSourceFrontierBarrierNil
	}
	if ctx == nil {
		return nil, nil, ErrSQLSourceFrontierBarrierContextNil
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	provider, ok := resolver.(SQLFrontierSnapshotProvider)
	if !ok {
		return nil, nil, ErrSQLFrontierSnapshotProviderUnsupported
	}
	if _, err := barrier.WaitForFrontier(ctx, frontier); err != nil {
		return nil, nil, err
	}
	snapshotResolver, release, err := provider.BeginSQLSnapshotAt(ctx, frontier)
	if err != nil {
		return nil, nil, err
	}
	if snapshotResolver == nil {
		if release != nil {
			release()
		}
		return nil, nil, ErrSQLSnapshotResolverNil
	}
	return snapshotResolver, release, nil
}
