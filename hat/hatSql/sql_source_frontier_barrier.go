package hatSql

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrSQLSourceFrontierBarrierNil        = errors.New("hatSql: SQL source frontier barrier is nil")
	ErrSQLSourceFrontierBarrierTrackerNil = errors.New("hatSql: SQL source frontier barrier tracker is nil")
	ErrSQLSourceFrontierBarrierContextNil = errors.New("hatSql: SQL source frontier barrier context is nil")
)

// SQLSourceFrontierBarrier waits until every configured source partition has
// observed a requested common frontier. Use its Observe methods to publish
// updates so blocked waiters are notified.
type SQLSourceFrontierBarrier struct {
	tracker *SQLSourceFrontierTracker

	mu      sync.Mutex
	changed chan struct{}
	common  atomic.Uint64
	ready   atomic.Bool
}

// NewSQLSourceFrontierBarrier wraps an existing fixed-partition frontier
// tracker. Updates made directly on tracker cannot notify barrier waiters;
// publish them through this barrier instead.
func NewSQLSourceFrontierBarrier(tracker *SQLSourceFrontierTracker) (*SQLSourceFrontierBarrier, error) {
	if tracker == nil {
		return nil, ErrSQLSourceFrontierBarrierTrackerNil
	}
	frontier, ready := tracker.CommonFrontier()
	barrier := &SQLSourceFrontierBarrier{
		tracker: tracker,
		changed: make(chan struct{}),
	}
	barrier.common.Store(frontier)
	barrier.ready.Store(ready)
	return barrier, nil
}

// NewSQLSourceFrontierBarrierFromPartitions constructs a tracker and barrier
// together for callers that do not need the tracker separately.
func NewSQLSourceFrontierBarrierFromPartitions(partitions []SQLSourceFrontierPartition) (*SQLSourceFrontierBarrier, error) {
	tracker, err := NewSQLSourceFrontierTracker(partitions)
	if err != nil {
		return nil, err
	}
	return NewSQLSourceFrontierBarrier(tracker)
}

// Observe publishes one source-partition frontier and reports whether the
// tracker state advanced.
func (barrier *SQLSourceFrontierBarrier) Observe(frontier SQLSourceFrontier) (bool, error) {
	if barrier == nil {
		return false, ErrSQLSourceFrontierBarrierNil
	}
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	changed, err := barrier.tracker.Observe(frontier)
	if err != nil {
		return false, err
	}
	if changed {
		barrier.refreshLocked()
		barrier.signalLocked()
	}
	return changed, nil
}

// ObserveBatch atomically publishes a batch of source-partition frontiers and
// reports how many tracker entries advanced. Invalid batches do not notify
// waiters or change tracker state.
func (barrier *SQLSourceFrontierBarrier) ObserveBatch(frontiers []SQLSourceFrontier) (int, error) {
	if barrier == nil {
		return 0, ErrSQLSourceFrontierBarrierNil
	}
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	changed, err := barrier.tracker.ObserveBatch(frontiers)
	if err != nil {
		return 0, err
	}
	if changed > 0 {
		barrier.refreshLocked()
		barrier.signalLocked()
	}
	return changed, nil
}

// WaitForFrontier blocks until every configured partition has observed at
// least frontier, or ctx is canceled. It returns the current common frontier.
func (barrier *SQLSourceFrontierBarrier) WaitForFrontier(ctx context.Context, frontier uint64) (uint64, error) {
	if barrier == nil {
		return 0, ErrSQLSourceFrontierBarrierNil
	}
	if ctx == nil {
		return 0, ErrSQLSourceFrontierBarrierContextNil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if common, ready := barrier.cachedReadyAt(frontier); ready {
		return common, nil
	}

	for {
		barrier.mu.Lock()
		common, ready := barrier.tracker.CommonFrontier()
		if ready && common >= frontier {
			barrier.mu.Unlock()
			return common, nil
		}
		wait := barrier.changed
		barrier.mu.Unlock()

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-wait:
		}
	}
}

// CommonFrontier returns the tracker's current common frontier and whether all
// configured partitions have produced at least one observation.
func (barrier *SQLSourceFrontierBarrier) CommonFrontier() (uint64, bool) {
	if barrier == nil {
		return 0, false
	}
	return barrier.tracker.CommonFrontier()
}

// ReadyAt reports whether all configured partitions have reached frontier.
func (barrier *SQLSourceFrontierBarrier) ReadyAt(frontier uint64) bool {
	if barrier == nil {
		return false
	}
	return barrier.tracker.ReadyAt(frontier)
}

// Frontier returns the observed frontier for one configured source partition.
func (barrier *SQLSourceFrontierBarrier) Frontier(source, partition string) (uint64, bool) {
	if barrier == nil {
		return 0, false
	}
	return barrier.tracker.Frontier(source, partition)
}

// Snapshot returns the tracker's deterministic partition snapshot.
func (barrier *SQLSourceFrontierBarrier) Snapshot() []SQLSourceFrontierSnapshot {
	if barrier == nil {
		return nil
	}
	return barrier.tracker.Snapshot()
}

func (barrier *SQLSourceFrontierBarrier) cachedReadyAt(frontier uint64) (uint64, bool) {
	if !barrier.ready.Load() {
		return 0, false
	}
	common := barrier.common.Load()
	return common, common >= frontier
}

func (barrier *SQLSourceFrontierBarrier) refreshLocked() {
	frontier, ready := barrier.tracker.CommonFrontier()
	barrier.common.Store(frontier)
	barrier.ready.Store(ready)
}

func (barrier *SQLSourceFrontierBarrier) signalLocked() {
	close(barrier.changed)
	barrier.changed = make(chan struct{})
}
