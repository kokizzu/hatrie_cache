package hatSql

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrTypedTableArrangementHydrationTargetAhead means the requested target
	// is newer than the current source tail and cannot be reached by hydration
	// until the caller observes a later source sequence.
	ErrTypedTableArrangementHydrationTargetAhead = errors.New("typed table arrangement hydration target is ahead of source")
)

// TypedTableAggregateArrangementHydrationStatus is the admission-facing
// checkpoint state of one aggregate arrangement.
type TypedTableAggregateArrangementHydrationStatus struct {
	Checkpoint     uint64 `json:"checkpoint"`
	SourceSequence uint64 `json:"source_sequence"`
	Target         uint64 `json:"target"`
	Ready          bool   `json:"ready"`
	Stale          bool   `json:"stale"`
}

// TypedTableJoinArrangementHydrationStatus is the admission-facing
// checkpoint state of both inputs of one join arrangement.
type TypedTableJoinArrangementHydrationStatus struct {
	LeftCheckpoint      uint64 `json:"left_checkpoint"`
	LeftSourceSequence  uint64 `json:"left_source_sequence"`
	RightCheckpoint     uint64 `json:"right_checkpoint"`
	RightSourceSequence uint64 `json:"right_source_sequence"`
	Ready               bool   `json:"ready"`
	Stale               bool   `json:"stale"`
}

// HydrationStatus reports the current arrangement checkpoint and source tail.
// Ready is true only when the arrangement has caught up to the observed tail.
func (arrangement *TypedTableAggregateArrangement) HydrationStatus() (TypedTableAggregateArrangementHydrationStatus, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableAggregateArrangementHydrationStatus{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	sourceSequence := typedTableArrangementSourceSequence(entry.aggregate.table)
	checkpoint := entry.aggregate.checkpoint
	return TypedTableAggregateArrangementHydrationStatus{
		Checkpoint:     checkpoint,
		SourceSequence: sourceSequence,
		Target:         sourceSequence,
		Ready:          checkpoint >= sourceSequence,
		Stale:          checkpoint < sourceSequence,
	}, nil
}

// WaitReady blocks query admission until the aggregate checkpoint reaches
// target. A caller must run Hydrate or Apply; this method starts no worker.
// The wait signal is allocated only while at least one caller is waiting.
func (arrangement *TypedTableAggregateArrangement) WaitReady(ctx context.Context, target uint64) (TypedTableAggregateArrangementHydrationStatus, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableAggregateArrangementHydrationStatus{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		entry.mu.Lock()
		sourceSequence := typedTableArrangementSourceSequence(entry.aggregate.table)
		checkpoint := entry.aggregate.checkpoint
		status := TypedTableAggregateArrangementHydrationStatus{
			Checkpoint:     checkpoint,
			SourceSequence: sourceSequence,
			Target:         target,
			Ready:          checkpoint >= target,
			Stale:          checkpoint < target,
		}
		if target > sourceSequence {
			entry.mu.Unlock()
			return status, fmt.Errorf("%w: target %d is newer than source sequence %d", ErrTypedTableArrangementHydrationTargetAhead, target, sourceSequence)
		}
		if status.Ready {
			entry.mu.Unlock()
			return status, nil
		}
		if entry.hydrationErr != nil {
			hydrationErr := entry.hydrationErr
			entry.mu.Unlock()
			return status, hydrationErr
		}
		signal := entry.ready
		if signal == nil {
			signal = make(chan struct{})
			entry.ready = signal
		}
		entry.mu.Unlock()
		select {
		case <-signal:
		case <-ctx.Done():
			return status, ctx.Err()
		}
	}
}

// HydrationStatus reports the current checkpoint and source tails of both
// join inputs. Ready is true only when both inputs have caught up.
func (arrangement *TypedTableJoinArrangement) HydrationStatus() (TypedTableJoinArrangementHydrationStatus, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableJoinArrangementHydrationStatus{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	leftCheckpoint := entry.join.LeftCheckpoint()
	rightCheckpoint := entry.join.RightCheckpoint()
	leftSourceSequence := typedTableArrangementSourceSequence(entry.join.left)
	rightSourceSequence := typedTableArrangementSourceSequence(entry.join.right)
	return TypedTableJoinArrangementHydrationStatus{
		LeftCheckpoint: leftCheckpoint, LeftSourceSequence: leftSourceSequence,
		RightCheckpoint: rightCheckpoint, RightSourceSequence: rightSourceSequence,
		Ready: leftCheckpoint >= leftSourceSequence && rightCheckpoint >= rightSourceSequence,
		Stale: leftCheckpoint < leftSourceSequence || rightCheckpoint < rightSourceSequence,
	}, nil
}

// WaitReady blocks query admission until both join checkpoints reach their
// requested targets. A caller must run Hydrate or ApplyLeft/ApplyRight.
func (arrangement *TypedTableJoinArrangement) WaitReady(ctx context.Context, leftTarget, rightTarget uint64) (TypedTableJoinArrangementHydrationStatus, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableJoinArrangementHydrationStatus{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		entry.mu.Lock()
		leftCheckpoint := entry.join.LeftCheckpoint()
		rightCheckpoint := entry.join.RightCheckpoint()
		leftSourceSequence := typedTableArrangementSourceSequence(entry.join.left)
		rightSourceSequence := typedTableArrangementSourceSequence(entry.join.right)
		status := TypedTableJoinArrangementHydrationStatus{
			LeftCheckpoint: leftCheckpoint, LeftSourceSequence: leftSourceSequence,
			RightCheckpoint: rightCheckpoint, RightSourceSequence: rightSourceSequence,
			Ready: leftCheckpoint >= leftTarget && rightCheckpoint >= rightTarget,
			Stale: leftCheckpoint < leftTarget || rightCheckpoint < rightTarget,
		}
		if leftTarget > leftSourceSequence || rightTarget > rightSourceSequence {
			entry.mu.Unlock()
			return status, fmt.Errorf("%w: targets %d/%d are newer than source sequences %d/%d", ErrTypedTableArrangementHydrationTargetAhead, leftTarget, rightTarget, leftSourceSequence, rightSourceSequence)
		}
		if status.Ready {
			entry.mu.Unlock()
			return status, nil
		}
		if entry.hydrationErr != nil {
			hydrationErr := entry.hydrationErr
			entry.mu.Unlock()
			return status, hydrationErr
		}
		signal := entry.ready
		if signal == nil {
			signal = make(chan struct{})
			entry.ready = signal
		}
		entry.mu.Unlock()
		select {
		case <-signal:
		case <-ctx.Done():
			return status, ctx.Err()
		}
	}
}

func typedTableArrangementSourceSequence(table *TypedTable) uint64 {
	if table == nil {
		return 0
	}
	table.mu.RLock()
	sequence := table.sequence
	table.mu.RUnlock()
	return sequence
}

func (entry *typedTableAggregateArrangementEntry) signalHydrationLocked(err error) {
	entry.hydrationErr = err
	if entry.ready != nil {
		close(entry.ready)
		entry.ready = nil
	}
}

func (entry *typedTableJoinArrangementEntry) signalHydrationLocked(err error) {
	entry.hydrationErr = err
	if entry.ready != nil {
		close(entry.ready)
		entry.ready = nil
	}
}
