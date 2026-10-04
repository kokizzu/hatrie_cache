package hatSql

import (
	"context"
	"sync"
)

// TypedTableAggregateArrangementHydrationStatus is a point-in-time hydration
// and query-admission snapshot for one aggregate arrangement.
type TypedTableAggregateArrangementHydrationStatus struct {
	Checkpoint     uint64 `json:"checkpoint"`
	SourceSequence uint64 `json:"source_sequence"`
	Remaining      uint64 `json:"remaining"`
	Ready          bool   `json:"ready"`
	Error          string `json:"error,omitempty"`
}

// TypedTableJoinArrangementHydrationStatus is a point-in-time hydration and
// query-admission snapshot for both inputs of one join arrangement.
type TypedTableJoinArrangementHydrationStatus struct {
	LeftCheckpoint      uint64 `json:"left_checkpoint"`
	LeftSourceSequence  uint64 `json:"left_source_sequence"`
	LeftRemaining       uint64 `json:"left_remaining"`
	RightCheckpoint     uint64 `json:"right_checkpoint"`
	RightSourceSequence uint64 `json:"right_source_sequence"`
	RightRemaining      uint64 `json:"right_remaining"`
	Ready               bool   `json:"ready"`
	Error               string `json:"error,omitempty"`
}

type typedTableHydrationAdmission struct {
	notify  chan struct{}
	waiters int
	err     error
}

// HydrationStatus returns the aggregate arrangement's current admission
// state. Ready means its checkpoint covers the source changefeed tail.
func (arrangement *TypedTableAggregateArrangement) HydrationStatus() (TypedTableAggregateArrangementHydrationStatus, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableAggregateArrangementHydrationStatus{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return typedTableAggregateHydrationStatusLocked(entry), nil
}

// WaitHydrated blocks until the aggregate arrangement covers its current
// source changefeed tail or ctx is canceled. A hydration error is returned to
// prevent admitting reads from an incomplete arrangement.
func (arrangement *TypedTableAggregateArrangement) WaitHydrated(ctx context.Context) error {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return err
	}
	return waitTypedTableHydration(ctx, &entry.mu, &entry.hydration, func() bool {
		return typedTableAggregateHydrationReadyLocked(entry)
	})
}

// HydrationStatus returns the join arrangement's current admission state.
// Ready means both input checkpoints cover their source changefeed tails.
func (arrangement *TypedTableJoinArrangement) HydrationStatus() (TypedTableJoinArrangementHydrationStatus, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableJoinArrangementHydrationStatus{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return typedTableJoinHydrationStatusLocked(entry), nil
}

// WaitHydrated blocks until both join inputs cover their current source
// changefeed tails or ctx is canceled.
func (arrangement *TypedTableJoinArrangement) WaitHydrated(ctx context.Context) error {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return err
	}
	return waitTypedTableHydration(ctx, &entry.mu, &entry.hydration, func() bool {
		return typedTableJoinHydrationReadyLocked(entry)
	})
}

func typedTableAggregateHydrationStatusLocked(entry *typedTableAggregateArrangementEntry) TypedTableAggregateArrangementHydrationStatus {
	sourceSequence := typedTableHydrationSourceSequence(entry.aggregate.table)
	checkpoint := entry.aggregate.checkpoint
	status := TypedTableAggregateArrangementHydrationStatus{
		Checkpoint:     checkpoint,
		SourceSequence: sourceSequence,
		Ready:          checkpoint >= sourceSequence,
	}
	if sourceSequence > checkpoint {
		status.Remaining = sourceSequence - checkpoint
	}
	if entry.hydration.err != nil {
		status.Error = entry.hydration.err.Error()
	}
	return status
}

func typedTableJoinHydrationStatusLocked(entry *typedTableJoinArrangementEntry) TypedTableJoinArrangementHydrationStatus {
	leftSourceSequence := typedTableHydrationSourceSequence(entry.join.left)
	rightSourceSequence := typedTableHydrationSourceSequence(entry.join.right)
	leftCheckpoint := entry.join.LeftCheckpoint()
	rightCheckpoint := entry.join.RightCheckpoint()
	status := TypedTableJoinArrangementHydrationStatus{
		LeftCheckpoint:      leftCheckpoint,
		LeftSourceSequence:  leftSourceSequence,
		RightCheckpoint:     rightCheckpoint,
		RightSourceSequence: rightSourceSequence,
		Ready:               leftCheckpoint >= leftSourceSequence && rightCheckpoint >= rightSourceSequence,
	}
	if leftSourceSequence > leftCheckpoint {
		status.LeftRemaining = leftSourceSequence - leftCheckpoint
	}
	if rightSourceSequence > rightCheckpoint {
		status.RightRemaining = rightSourceSequence - rightCheckpoint
	}
	if entry.hydration.err != nil {
		status.Error = entry.hydration.err.Error()
	}
	return status
}

func typedTableAggregateHydrationReadyLocked(entry *typedTableAggregateArrangementEntry) bool {
	return entry.aggregate.checkpoint >= typedTableHydrationSourceSequence(entry.aggregate.table)
}

func typedTableJoinHydrationReadyLocked(entry *typedTableJoinArrangementEntry) bool {
	return entry.join.LeftCheckpoint() >= typedTableHydrationSourceSequence(entry.join.left) &&
		entry.join.RightCheckpoint() >= typedTableHydrationSourceSequence(entry.join.right)
}

func typedTableHydrationSourceSequence(table *TypedTable) uint64 {
	table.mu.RLock()
	defer table.mu.RUnlock()
	return table.sequence
}

func waitTypedTableHydration(ctx context.Context, mutex *sync.Mutex, admission *typedTableHydrationAdmission, ready func() bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		mutex.Lock()
		if admission.err != nil {
			err := admission.err
			mutex.Unlock()
			return err
		}
		if ready() {
			mutex.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			mutex.Unlock()
			return err
		}
		if admission.notify == nil {
			admission.notify = make(chan struct{})
		}
		wait := admission.notify
		admission.waiters++
		mutex.Unlock()

		select {
		case <-ctx.Done():
			removeTypedTableHydrationWaiter(mutex, admission)
			return ctx.Err()
		case <-wait:
			removeTypedTableHydrationWaiter(mutex, admission)
		}
	}
}

func (admission *typedTableHydrationAdmission) recordLocked(err error) {
	admission.err = err
	if admission.notify == nil || admission.waiters == 0 {
		return
	}
	close(admission.notify)
	admission.notify = make(chan struct{})
}

func removeTypedTableHydrationWaiter(mutex *sync.Mutex, admission *typedTableHydrationAdmission) {
	mutex.Lock()
	defer mutex.Unlock()
	admission.waiters--
	if admission.waiters == 0 {
		admission.notify = nil
	}
}
