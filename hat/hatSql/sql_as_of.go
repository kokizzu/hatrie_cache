package hatSql

import (
	"context"
	"errors"
)

// ErrSQLAsOfUnsupported reports that a historical query was requested for a
// resolver that cannot bind reads to an exact frontier.
var ErrSQLAsOfUnsupported = errors.New("hatSql: SQL AS OF requires a frontier snapshot provider")

func beginSQLAsOfSnapshot(ctx context.Context, resolver SQLSourceResolver, frontier *uint64) (SQLSourceResolver, func(), error) {
	if frontier == nil {
		return resolver, nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	provider, ok := resolver.(SQLFrontierSnapshotProvider)
	if !ok {
		return nil, nil, ErrSQLAsOfUnsupported
	}
	snapshotResolver, release, err := provider.BeginSQLSnapshotAt(ctx, *frontier)
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
