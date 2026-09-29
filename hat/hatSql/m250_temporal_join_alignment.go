package hatSql

import (
	"errors"
	"fmt"
	"sync"
)

const DefaultDifferentialTemporalJoinAlignmentMaxPendingChanges = 65536

var (
	ErrDifferentialTemporalJoinAlignmentNil                = errors.New("hatSql: differential temporal join alignment is nil")
	ErrDifferentialTemporalJoinAlignmentFrontierRegression = errors.New("hatSql: differential temporal join alignment frontier moved backwards")
	ErrDifferentialTemporalJoinAlignmentPendingLimit       = errors.New("hatSql: differential temporal join alignment pending limit exceeded")
	ErrDifferentialTemporalJoinAlignmentInvalidOptions     = errors.New("hatSql: invalid differential temporal join alignment options")
	ErrDifferentialTemporalJoinAlignmentLateChange         = errors.New("hatSql: differential temporal join alignment change is before the aligned frontier")
)

// DifferentialTemporalJoinAlignmentOptions bounds rows retained while one
// input is ahead of the other. Zero selects the package default.
type DifferentialTemporalJoinAlignmentOptions struct {
	MaxPendingChanges int
}

// DifferentialTemporalJoinAlignmentStats reports input and common frontiers
// plus rows waiting for the other input to advance.
type DifferentialTemporalJoinAlignmentStats struct {
	LeftFrontier    uint64
	RightFrontier   uint64
	AlignedFrontier uint64
	PendingLeft     int
	PendingRight    int
}

// DifferentialTemporalJoinAligned gates an exact temporal join with two
// input frontiers. A row at time t is released only after both frontiers are
// greater than t. Existing DifferentialTemporalJoin ApplyLeft/ApplyRight
// behavior remains unchanged for callers that do not need frontier gating.
type DifferentialTemporalJoinAligned struct {
	mu sync.Mutex

	join              *DifferentialTemporalJoin
	maxPendingChanges int
	leftFrontier      uint64
	rightFrontier     uint64
	alignedFrontier   uint64
	pendingLeft       []DifferentialRow
	pendingRight      []DifferentialRow
}

// NewDifferentialTemporalJoinAligned creates a frontier-aligned temporal join.
// The supplied join definition is otherwise identical to
// NewDifferentialTemporalJoin. Input frontiers are exclusive: a row at exactly
// the frontier remains pending until the frontier advances.
func NewDifferentialTemporalJoinAligned(definition DifferentialTemporalJoinDefinition, options DifferentialTemporalJoinAlignmentOptions) (*DifferentialTemporalJoinAligned, error) {
	maxPendingChanges := options.MaxPendingChanges
	if maxPendingChanges == 0 {
		maxPendingChanges = DefaultDifferentialTemporalJoinAlignmentMaxPendingChanges
	}
	if maxPendingChanges < 0 {
		return nil, fmt.Errorf("%w: max pending changes must be non-negative", ErrDifferentialTemporalJoinAlignmentInvalidOptions)
	}
	join, err := NewDifferentialTemporalJoin(definition)
	if err != nil {
		return nil, err
	}
	return &DifferentialTemporalJoinAligned{
		join:              join,
		maxPendingChanges: maxPendingChanges,
	}, nil
}

// ApplyLeft observes a left input frontier and queues the supplied changes.
// Only rows below the common frontier are applied to the underlying join.
func (aligned *DifferentialTemporalJoinAligned) ApplyLeft(frontier uint64, changes []DifferentialRow) ([]DifferentialRow, error) {
	return aligned.apply(frontier, changes, true)
}

// ApplyRight observes a right input frontier and queues the supplied changes.
// Only rows below the common frontier are applied to the underlying join.
func (aligned *DifferentialTemporalJoinAligned) ApplyRight(frontier uint64, changes []DifferentialRow) ([]DifferentialRow, error) {
	return aligned.apply(frontier, changes, false)
}

// Stats returns the current detached alignment state.
func (aligned *DifferentialTemporalJoinAligned) Stats() DifferentialTemporalJoinAlignmentStats {
	if aligned == nil {
		return DifferentialTemporalJoinAlignmentStats{}
	}
	aligned.mu.Lock()
	defer aligned.mu.Unlock()
	return DifferentialTemporalJoinAlignmentStats{
		LeftFrontier:    aligned.leftFrontier,
		RightFrontier:   aligned.rightFrontier,
		AlignedFrontier: aligned.alignedFrontier,
		PendingLeft:     len(aligned.pendingLeft),
		PendingRight:    len(aligned.pendingRight),
	}
}

func (aligned *DifferentialTemporalJoinAligned) apply(frontier uint64, changes []DifferentialRow, leftSide bool) ([]DifferentialRow, error) {
	if aligned == nil {
		return nil, ErrDifferentialTemporalJoinAlignmentNil
	}
	aligned.mu.Lock()
	defer aligned.mu.Unlock()
	if leftSide {
		if frontier < aligned.leftFrontier {
			return nil, fmt.Errorf("left frontier %d follows %d: %w", frontier, aligned.leftFrontier, ErrDifferentialTemporalJoinAlignmentFrontierRegression)
		}
	} else if frontier < aligned.rightFrontier {
		return nil, fmt.Errorf("right frontier %d follows %d: %w", frontier, aligned.rightFrontier, ErrDifferentialTemporalJoinAlignmentFrontierRegression)
	}
	accepted := make([]DifferentialRow, 0, len(changes))
	for _, change := range changes {
		if change.Diff == 0 {
			continue
		}
		if change.Time < aligned.alignedFrontier {
			return nil, fmt.Errorf("change %q at time %d follows aligned frontier %d: %w", change.Key, change.Time, aligned.alignedFrontier, ErrDifferentialTemporalJoinAlignmentLateChange)
		}
		cloned := change
		cloned.Row = cloneDifferentialRow(change.Row)
		accepted = append(accepted, cloned)
	}
	pending := len(aligned.pendingLeft) + len(aligned.pendingRight)
	if len(accepted) > aligned.maxPendingChanges-pending {
		return nil, fmt.Errorf("pending changes %d plus batch %d exceeds %d: %w", pending, len(accepted), aligned.maxPendingChanges, ErrDifferentialTemporalJoinAlignmentPendingLimit)
	}
	if leftSide {
		aligned.pendingLeft = append(aligned.pendingLeft, accepted...)
		aligned.leftFrontier = frontier
	} else {
		aligned.pendingRight = append(aligned.pendingRight, accepted...)
		aligned.rightFrontier = frontier
	}
	return aligned.flushLocked()
}

func (aligned *DifferentialTemporalJoinAligned) flushLocked() ([]DifferentialRow, error) {
	common := aligned.leftFrontier
	if aligned.rightFrontier < common {
		common = aligned.rightFrontier
	}
	if common <= aligned.alignedFrontier {
		return nil, nil
	}
	readyLeft, retainedLeft := splitDifferentialTemporalJoinPending(aligned.pendingLeft, common)
	readyRight, retainedRight := splitDifferentialTemporalJoinPending(aligned.pendingRight, common)
	var emitted []DifferentialRow
	if len(readyLeft) != 0 {
		updates, err := aligned.join.applyOwnedChanges(readyLeft, true)
		if err != nil {
			return nil, err
		}
		emitted = append(emitted, updates...)
	}
	if len(readyRight) != 0 {
		updates, err := aligned.join.applyOwnedChanges(readyRight, false)
		if err != nil {
			return nil, err
		}
		emitted = append(emitted, updates...)
	}
	if _, err := aligned.join.Compact(common, common); err != nil {
		return nil, err
	}
	aligned.pendingLeft = retainedLeft
	aligned.pendingRight = retainedRight
	aligned.alignedFrontier = common
	return emitted, nil
}

func splitDifferentialTemporalJoinPending(pending []DifferentialRow, frontier uint64) (ready, retained []DifferentialRow) {
	if len(pending) == 0 {
		return nil, nil
	}
	ready = make([]DifferentialRow, 0, len(pending))
	retained = make([]DifferentialRow, 0, len(pending))
	for _, change := range pending {
		if change.Time < frontier {
			ready = append(ready, change)
			continue
		}
		retained = append(retained, change)
	}
	return ready, retained
}
