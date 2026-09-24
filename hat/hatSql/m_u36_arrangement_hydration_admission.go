package hatSql

import (
	"context"
	"errors"
	"fmt"
)

const (
	// TypedTableArrangementHydrationStateReady means the arrangement has
	// applied every retained source change visible at the status read.
	TypedTableArrangementHydrationStateReady = "ready"
	// TypedTableArrangementHydrationStateStale means source changes remain to
	// be applied before the arrangement can admit a read.
	TypedTableArrangementHydrationStateStale = "stale"
	// TypedTableArrangementHydrationStateFailed means the last hydration
	// attempt failed and the arrangement cannot admit a read until retry.
	TypedTableArrangementHydrationStateFailed = "failed"
)

var (
	// ErrTypedTableArrangementHydrationContextNil reports a nil wait context.
	ErrTypedTableArrangementHydrationContextNil = errors.New("hatSql: arrangement hydration context is nil")
	// ErrTypedTableArrangementHydrationFailed reports a terminal last hydration
	// error returned by WaitForHydration.
	ErrTypedTableArrangementHydrationFailed = errors.New("hatSql: arrangement hydration failed")
)

// TypedTableAggregateArrangementHydrationStatus reports bounded progress and
// admission state for one shared aggregate arrangement.
type TypedTableAggregateArrangementHydrationStatus struct {
	Checkpoint     uint64 `json:"checkpoint"`
	SourceSequence uint64 `json:"source_sequence"`
	Remaining      uint64 `json:"remaining"`
	Generation     uint64 `json:"generation"`
	Ready          bool   `json:"ready"`
	Failed         bool   `json:"failed"`
	State          string `json:"state"`
}

// TypedTableJoinArrangementHydrationStatus reports bounded progress and
// admission state for both inputs of one shared join arrangement.
type TypedTableJoinArrangementHydrationStatus struct {
	LeftCheckpoint      uint64 `json:"left_checkpoint"`
	LeftSourceSequence  uint64 `json:"left_source_sequence"`
	LeftRemaining       uint64 `json:"left_remaining"`
	RightCheckpoint     uint64 `json:"right_checkpoint"`
	RightSourceSequence uint64 `json:"right_source_sequence"`
	RightRemaining      uint64 `json:"right_remaining"`
	Generation          uint64 `json:"generation"`
	Ready               bool   `json:"ready"`
	Failed              bool   `json:"failed"`
	State               string `json:"state"`
}

type typedTableArrangementHydrationState struct {
	notify     chan struct{}
	generation uint64
	failure    error
}

func newTypedTableArrangementHydrationState() typedTableArrangementHydrationState {
	return typedTableArrangementHydrationState{notify: make(chan struct{})}
}

func (state *typedTableArrangementHydrationState) resetLocked() {
	if state.notify == nil {
		state.notify = make(chan struct{})
	}
	state.failure = nil
}

func (state *typedTableArrangementHydrationState) completeLocked() {
	state.failure = nil
	state.bumpLocked()
}

func (state *typedTableArrangementHydrationState) failLocked(err error) {
	state.failure = err
	state.bumpLocked()
}

func (state *typedTableArrangementHydrationState) bumpLocked() {
	if state.notify == nil {
		state.notify = make(chan struct{})
	}
	state.generation++
	if state.generation == 0 {
		state.generation = 1
	}
	close(state.notify)
	state.notify = make(chan struct{})
}

// HydrationStatus returns one detached status read. It does not reread source
// rows; it only compares the arrangement checkpoint with the table's current
// in-memory change sequence.
func (arrangement *TypedTableAggregateArrangement) HydrationStatus() (TypedTableAggregateArrangementHydrationStatus, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableAggregateArrangementHydrationStatus{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return typedTableAggregateArrangementHydrationStatusLocked(entry), nil
}

// WaitForHydration blocks query admission until the aggregate arrangement is
// current, hydration fails, or ctx is canceled. It does not perform source
// reads or trigger hydration itself.
func (arrangement *TypedTableAggregateArrangement) WaitForHydration(ctx context.Context) (TypedTableAggregateArrangementHydrationStatus, error) {
	if ctx == nil {
		return TypedTableAggregateArrangementHydrationStatus{}, ErrTypedTableArrangementHydrationContextNil
	}
	if err := ctx.Err(); err != nil {
		return TypedTableAggregateArrangementHydrationStatus{}, err
	}
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableAggregateArrangementHydrationStatus{}, err
	}
	for {
		entry.mu.Lock()
		status := typedTableAggregateArrangementHydrationStatusLocked(entry)
		if status.Ready {
			entry.mu.Unlock()
			return status, nil
		}
		if status.Failed {
			hydrationErr := entry.hydration.failure
			entry.mu.Unlock()
			return status, fmt.Errorf("%w: %v", ErrTypedTableArrangementHydrationFailed, hydrationErr)
		}
		notify := entry.hydration.notify
		entry.mu.Unlock()
		select {
		case <-ctx.Done():
			return status, ctx.Err()
		case <-notify:
		}
	}
}

// HydrationStatus returns one detached status read for both join inputs. It
// does not reread source rows.
func (arrangement *TypedTableJoinArrangement) HydrationStatus() (TypedTableJoinArrangementHydrationStatus, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableJoinArrangementHydrationStatus{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return typedTableJoinArrangementHydrationStatusLocked(entry), nil
}

// WaitForHydration blocks query admission until both join inputs are current,
// hydration fails, or ctx is canceled. It does not perform source reads or
// trigger hydration itself.
func (arrangement *TypedTableJoinArrangement) WaitForHydration(ctx context.Context) (TypedTableJoinArrangementHydrationStatus, error) {
	if ctx == nil {
		return TypedTableJoinArrangementHydrationStatus{}, ErrTypedTableArrangementHydrationContextNil
	}
	if err := ctx.Err(); err != nil {
		return TypedTableJoinArrangementHydrationStatus{}, err
	}
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableJoinArrangementHydrationStatus{}, err
	}
	for {
		entry.mu.Lock()
		status := typedTableJoinArrangementHydrationStatusLocked(entry)
		if status.Ready {
			entry.mu.Unlock()
			return status, nil
		}
		if status.Failed {
			hydrationErr := entry.hydration.failure
			entry.mu.Unlock()
			return status, fmt.Errorf("%w: %v", ErrTypedTableArrangementHydrationFailed, hydrationErr)
		}
		notify := entry.hydration.notify
		entry.mu.Unlock()
		select {
		case <-ctx.Done():
			return status, ctx.Err()
		case <-notify:
		}
	}
}

func typedTableAggregateArrangementHydrationStatusLocked(entry *typedTableAggregateArrangementEntry) TypedTableAggregateArrangementHydrationStatus {
	if entry == nil || entry.aggregate == nil || entry.aggregate.table == nil {
		return TypedTableAggregateArrangementHydrationStatus{State: TypedTableArrangementHydrationStateFailed, Failed: true}
	}
	checkpoint := entry.aggregate.checkpoint
	entry.aggregate.table.mu.RLock()
	sourceSequence := entry.aggregate.table.sequence
	entry.aggregate.table.mu.RUnlock()
	status := TypedTableAggregateArrangementHydrationStatus{
		Checkpoint:     checkpoint,
		SourceSequence: sourceSequence,
		Generation:     entry.hydration.generation,
		Ready:          checkpoint >= sourceSequence,
		Failed:         entry.hydration.failure != nil,
	}
	if checkpoint < sourceSequence {
		status.Remaining = sourceSequence - checkpoint
	}
	switch {
	case status.Failed:
		status.State = TypedTableArrangementHydrationStateFailed
	case status.Ready:
		status.State = TypedTableArrangementHydrationStateReady
	default:
		status.State = TypedTableArrangementHydrationStateStale
	}
	return status
}

func typedTableJoinArrangementHydrationStatusLocked(entry *typedTableJoinArrangementEntry) TypedTableJoinArrangementHydrationStatus {
	if entry == nil || entry.join == nil || entry.join.left == nil || entry.join.right == nil {
		return TypedTableJoinArrangementHydrationStatus{State: TypedTableArrangementHydrationStateFailed, Failed: true}
	}
	leftCheckpoint := entry.join.LeftCheckpoint()
	rightCheckpoint := entry.join.RightCheckpoint()
	entry.join.left.mu.RLock()
	leftSourceSequence := entry.join.left.sequence
	entry.join.left.mu.RUnlock()
	entry.join.right.mu.RLock()
	rightSourceSequence := entry.join.right.sequence
	entry.join.right.mu.RUnlock()
	status := TypedTableJoinArrangementHydrationStatus{
		LeftCheckpoint:      leftCheckpoint,
		LeftSourceSequence:  leftSourceSequence,
		RightCheckpoint:     rightCheckpoint,
		RightSourceSequence: rightSourceSequence,
		Generation:          entry.hydration.generation,
		Ready:               leftCheckpoint >= leftSourceSequence && rightCheckpoint >= rightSourceSequence,
		Failed:              entry.hydration.failure != nil,
	}
	if leftCheckpoint < leftSourceSequence {
		status.LeftRemaining = leftSourceSequence - leftCheckpoint
	}
	if rightCheckpoint < rightSourceSequence {
		status.RightRemaining = rightSourceSequence - rightCheckpoint
	}
	switch {
	case status.Failed:
		status.State = TypedTableArrangementHydrationStateFailed
	case status.Ready:
		status.State = TypedTableArrangementHydrationStateReady
	default:
		status.State = TypedTableArrangementHydrationStateStale
	}
	return status
}
