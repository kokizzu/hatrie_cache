package hatSql

import (
	"context"
	"errors"
)

// ErrTypedTableArrangementNotReady is returned when a caller tries to admit a
// query against an arrangement that still has retained source changes to
// replay.
var ErrTypedTableArrangementNotReady = errors.New("typed table arrangement is not hydrated")

// TypedTableArrangementHydrationStatus is a point-in-time admission snapshot
// for an aggregate arrangement.
type TypedTableArrangementHydrationStatus struct {
	Checkpoint     uint64 `json:"checkpoint"`
	SourceSequence uint64 `json:"source_sequence"`
	Pending        uint64 `json:"pending"`
	Ready          bool   `json:"ready"`
}

// Admit returns nil when the arrangement can serve a query at this snapshot.
// Callers that need a stronger consistency boundary should retain their own
// source/version fence and re-check the status at that boundary.
func (status TypedTableArrangementHydrationStatus) Admit() error {
	if status.Ready {
		return nil
	}
	return ErrTypedTableArrangementNotReady
}

// TypedTableJoinArrangementHydrationStatus is a point-in-time admission
// snapshot for both inputs of a join arrangement.
type TypedTableJoinArrangementHydrationStatus struct {
	LeftCheckpoint      uint64 `json:"left_checkpoint"`
	LeftSourceSequence  uint64 `json:"left_source_sequence"`
	LeftPending         uint64 `json:"left_pending"`
	RightCheckpoint     uint64 `json:"right_checkpoint"`
	RightSourceSequence uint64 `json:"right_source_sequence"`
	RightPending        uint64 `json:"right_pending"`
	Ready               bool   `json:"ready"`
}

// Admit returns nil when both join inputs are hydrated at this snapshot.
func (status TypedTableJoinArrangementHydrationStatus) Admit() error {
	if status.Ready {
		return nil
	}
	return ErrTypedTableArrangementNotReady
}

// HydrationStatus reports the current aggregate checkpoint and retained
// replay distance without changing arrangement state.
func (arrangement *TypedTableAggregateArrangement) HydrationStatus() (TypedTableArrangementHydrationStatus, error) {
	freshness, err := arrangement.Freshness()
	if err != nil {
		return TypedTableArrangementHydrationStatus{}, err
	}
	return TypedTableArrangementHydrationStatus{
		Checkpoint:     freshness.Checkpoint,
		SourceSequence: freshness.SourceSequence,
		Pending:        typedTableArrangementPending(freshness.Checkpoint, freshness.SourceSequence),
		Ready:          !freshness.Stale,
	}, nil
}

// HydrateUntilReady replays retained source changes in bounded batches until
// the aggregate is admissible or ctx is canceled. It never waits for future
// source writes; callers should invoke it again after a later status check.
func (arrangement *TypedTableAggregateArrangement) HydrateUntilReady(ctx context.Context, limit int) (TypedTableArrangementHydrationStatus, error) {
	if limit < 0 {
		return TypedTableArrangementHydrationStatus{}, errors.New("typed table arrangement hydration batch cannot be negative")
	}
	if ctx == nil {
		return TypedTableArrangementHydrationStatus{}, errors.New("typed table arrangement hydration context is nil")
	}
	for {
		if err := ctx.Err(); err != nil {
			return TypedTableArrangementHydrationStatus{}, err
		}
		status, err := arrangement.HydrationStatus()
		if err != nil {
			return TypedTableArrangementHydrationStatus{}, err
		}
		if status.Ready {
			return status, nil
		}
		if _, err := arrangement.Hydrate(limit); err != nil {
			return status, err
		}
	}
}

// HydrationStatus reports both input checkpoints and retained replay distance
// without changing join arrangement state.
func (arrangement *TypedTableJoinArrangement) HydrationStatus() (TypedTableJoinArrangementHydrationStatus, error) {
	freshness, err := arrangement.Freshness()
	if err != nil {
		return TypedTableJoinArrangementHydrationStatus{}, err
	}
	return TypedTableJoinArrangementHydrationStatus{
		LeftCheckpoint:      freshness.LeftCheckpoint,
		LeftSourceSequence:  freshness.LeftSourceSequence,
		LeftPending:         typedTableArrangementPending(freshness.LeftCheckpoint, freshness.LeftSourceSequence),
		RightCheckpoint:     freshness.RightCheckpoint,
		RightSourceSequence: freshness.RightSourceSequence,
		RightPending:        typedTableArrangementPending(freshness.RightCheckpoint, freshness.RightSourceSequence),
		Ready:               !freshness.Stale,
	}, nil
}

// HydrateUntilReady replays retained changes from both join inputs in bounded
// batches until the join is admissible or ctx is canceled.
func (arrangement *TypedTableJoinArrangement) HydrateUntilReady(ctx context.Context, limit int) (TypedTableJoinArrangementHydrationStatus, error) {
	if limit < 0 {
		return TypedTableJoinArrangementHydrationStatus{}, errors.New("typed table join arrangement hydration batch cannot be negative")
	}
	if ctx == nil {
		return TypedTableJoinArrangementHydrationStatus{}, errors.New("typed table join hydration context is nil")
	}
	for {
		if err := ctx.Err(); err != nil {
			return TypedTableJoinArrangementHydrationStatus{}, err
		}
		status, err := arrangement.HydrationStatus()
		if err != nil {
			return TypedTableJoinArrangementHydrationStatus{}, err
		}
		if status.Ready {
			return status, nil
		}
		if _, err := arrangement.Hydrate(limit); err != nil {
			return status, err
		}
	}
}
