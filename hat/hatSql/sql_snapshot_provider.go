package hatSql

import (
	"context"
	"errors"
)

// ErrSQLSnapshotResolverNil reports that a snapshot provider did not return a
// resolver for the query's immutable read view.
var ErrSQLSnapshotResolverNil = errors.New("SQL snapshot resolver is nil")

// SQLSnapshotProvider optionally creates one immutable resolver view for the
// complete SQL execution. Providers can use this to pin independent source
// partitions to one common frontier without copying rows. The returned
// release function is called exactly once after materialized or streamed
// execution, including execution errors.
type SQLSnapshotProvider interface {
	BeginSQLSnapshot(ctx context.Context) (resolver SQLSourceResolver, release func(), err error)
}

func beginSQLSnapshot(ctx context.Context, resolver SQLSourceResolver) (SQLSourceResolver, func(), error) {
	provider, ok := resolver.(SQLSnapshotProvider)
	if !ok {
		return resolver, nil, nil
	}
	snapshotResolver, release, err := provider.BeginSQLSnapshot(ctx)
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
