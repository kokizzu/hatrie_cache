package hatSql

import (
	"context"
	"errors"
)

var (
	ErrSQLDistributedFrontierCoordinatorNil        = errors.New("hatSql: distributed frontier coordinator is nil")
	ErrSQLDistributedFrontierCoordinatorEmpty      = errors.New("hatSql: distributed frontier coordinator requires a barrier")
	ErrSQLDistributedFrontierCoordinatorBarrierNil = errors.New("hatSql: distributed frontier coordinator contains a nil barrier")
)

// SQLDistributedFrontierCoordinator combines independent source barriers into
// one common frontier. It is opt-in; callers still decide when to open a
// snapshot or execute a query at the returned frontier.
type SQLDistributedFrontierCoordinator struct {
	barriers []*SQLSourceFrontierBarrier
}

// NewSQLDistributedFrontierCoordinator creates a coordinator over independent
// source-barrier groups. The input slice is copied and later mutations do not
// change the coordinator.
func NewSQLDistributedFrontierCoordinator(barriers []*SQLSourceFrontierBarrier) (*SQLDistributedFrontierCoordinator, error) {
	if len(barriers) == 0 {
		return nil, ErrSQLDistributedFrontierCoordinatorEmpty
	}
	for _, barrier := range barriers {
		if barrier == nil {
			return nil, ErrSQLDistributedFrontierCoordinatorBarrierNil
		}
	}
	return &SQLDistributedFrontierCoordinator{barriers: append([]*SQLSourceFrontierBarrier(nil), barriers...)}, nil
}

// CommonFrontier returns the minimum common frontier across all barrier groups.
// ready is true only when every group has observed all of its partitions. A
// ready group may still be ahead of the returned minimum frontier.
func (coordinator *SQLDistributedFrontierCoordinator) CommonFrontier() (frontier uint64, ready bool) {
	if coordinator == nil || len(coordinator.barriers) == 0 {
		return 0, false
	}
	ready = true
	for index, barrier := range coordinator.barriers {
		if barrier == nil {
			return 0, false
		}
		current, currentReady := barrier.CommonFrontier()
		if index == 0 || current < frontier {
			frontier = current
		}
		if !currentReady {
			ready = false
		}
	}
	return frontier, ready
}

// ReadyAt reports whether every independent barrier group has observed
// frontier. It is allocation-free on the ready path.
func (coordinator *SQLDistributedFrontierCoordinator) ReadyAt(frontier uint64) bool {
	if coordinator == nil || len(coordinator.barriers) == 0 {
		return false
	}
	for _, barrier := range coordinator.barriers {
		if barrier == nil {
			return false
		}
		if !barrier.ReadyAt(frontier) {
			return false
		}
	}
	return true
}

// WaitForFrontier waits for every independent barrier group to observe
// frontier. The returned frontier is the requested common frontier.
func (coordinator *SQLDistributedFrontierCoordinator) WaitForFrontier(ctx context.Context, frontier uint64) (uint64, error) {
	if coordinator == nil {
		return 0, ErrSQLDistributedFrontierCoordinatorNil
	}
	if len(coordinator.barriers) == 0 {
		return 0, ErrSQLDistributedFrontierCoordinatorEmpty
	}
	if ctx == nil {
		return 0, ErrSQLSourceFrontierBarrierContextNil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	for _, barrier := range coordinator.barriers {
		if barrier == nil {
			return 0, ErrSQLDistributedFrontierCoordinatorBarrierNil
		}
		if _, err := barrier.WaitForFrontier(ctx, frontier); err != nil {
			return 0, err
		}
	}
	return frontier, nil
}

// BeginSQLDistributedFrontierSnapshot waits for every independent source
// barrier, then opens one immutable provider view at the exact common
// frontier. Existing single-barrier snapshot behavior is unchanged.
func BeginSQLDistributedFrontierSnapshot(ctx context.Context, provider SQLFrontierSnapshotProvider, coordinator *SQLDistributedFrontierCoordinator, frontier uint64) (SQLSourceResolver, func(), error) {
	if coordinator == nil {
		return nil, nil, ErrSQLDistributedFrontierCoordinatorNil
	}
	if ctx == nil {
		return nil, nil, ErrSQLSourceFrontierBarrierContextNil
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if provider == nil {
		return nil, nil, ErrSQLFrontierSnapshotProviderUnsupported
	}
	if _, err := coordinator.WaitForFrontier(ctx, frontier); err != nil {
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
