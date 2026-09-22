package hatSql

import (
	"context"
	"errors"
)

var (
	ErrSQLTemporalJoinFrontierAlignmentNil        = errors.New("hatSql: temporal join frontier alignment is nil")
	ErrSQLTemporalJoinFrontierAlignmentLeftNil    = errors.New("hatSql: temporal join frontier alignment left barrier is nil")
	ErrSQLTemporalJoinFrontierAlignmentRightNil   = errors.New("hatSql: temporal join frontier alignment right barrier is nil")
	ErrSQLTemporalJoinFrontierAlignmentContextNil = errors.New("hatSql: temporal join frontier alignment context is nil")
)

// SQLTemporalJoinFrontierResult is a consistent pair of input frontiers. The
// common frontier is the safe timestamp at which both inputs are complete.
type SQLTemporalJoinFrontierResult struct {
	LeftFrontier   uint64
	RightFrontier  uint64
	CommonFrontier uint64
}

// SQLTemporalJoinFrontierAlignment coordinates two independently updated
// source barriers. It is opt-in: callers choose when a temporal join needs a
// shared frontier and ordinary source-frontier behavior is unchanged.
type SQLTemporalJoinFrontierAlignment struct {
	left  *SQLSourceFrontierBarrier
	right *SQLSourceFrontierBarrier
}

// NewSQLTemporalJoinFrontierAlignment creates an alignment for the left and
// right inputs of a temporal join.
func NewSQLTemporalJoinFrontierAlignment(left, right *SQLSourceFrontierBarrier) (*SQLTemporalJoinFrontierAlignment, error) {
	if left == nil {
		return nil, ErrSQLTemporalJoinFrontierAlignmentLeftNil
	}
	if right == nil {
		return nil, ErrSQLTemporalJoinFrontierAlignmentRightNil
	}
	return &SQLTemporalJoinFrontierAlignment{left: left, right: right}, nil
}

// CommonFrontier returns both current input frontiers once both barriers have
// observed at least one frontier. The boolean distinguishes an unready input
// from a valid observed frontier of zero.
func (alignment *SQLTemporalJoinFrontierAlignment) CommonFrontier() (SQLTemporalJoinFrontierResult, bool) {
	if alignment == nil || alignment.left == nil || alignment.right == nil {
		return SQLTemporalJoinFrontierResult{}, false
	}
	left, leftReady := alignment.left.CommonFrontier()
	right, rightReady := alignment.right.CommonFrontier()
	if !leftReady || !rightReady {
		return SQLTemporalJoinFrontierResult{}, false
	}
	common := left
	if right < common {
		common = right
	}
	return SQLTemporalJoinFrontierResult{
		LeftFrontier:   left,
		RightFrontier:  right,
		CommonFrontier: common,
	}, true
}

// ReadyAt reports whether both inputs have reached frontier.
func (alignment *SQLTemporalJoinFrontierAlignment) ReadyAt(frontier uint64) bool {
	result, ready := alignment.CommonFrontier()
	return ready && result.CommonFrontier >= frontier
}

// WaitForFrontier waits until both inputs have reached frontier. The two
// waits run concurrently so both input conditions are evaluated from the same
// call. Already-aligned inputs do not start goroutines.
func (alignment *SQLTemporalJoinFrontierAlignment) WaitForFrontier(ctx context.Context, frontier uint64) (SQLTemporalJoinFrontierResult, error) {
	if alignment == nil {
		return SQLTemporalJoinFrontierResult{}, ErrSQLTemporalJoinFrontierAlignmentNil
	}
	if alignment.left == nil {
		return SQLTemporalJoinFrontierResult{}, ErrSQLTemporalJoinFrontierAlignmentLeftNil
	}
	if alignment.right == nil {
		return SQLTemporalJoinFrontierResult{}, ErrSQLTemporalJoinFrontierAlignmentRightNil
	}
	if ctx == nil {
		return SQLTemporalJoinFrontierResult{}, ErrSQLTemporalJoinFrontierAlignmentContextNil
	}
	if err := ctx.Err(); err != nil {
		return SQLTemporalJoinFrontierResult{}, err
	}
	leftFrontier, leftReady := sqlTemporalJoinCachedFrontier(alignment.left)
	rightFrontier, rightReady := sqlTemporalJoinCachedFrontier(alignment.right)
	if leftReady && rightReady {
		common := leftFrontier
		if rightFrontier < common {
			common = rightFrontier
		}
		if common >= frontier {
			return SQLTemporalJoinFrontierResult{
				LeftFrontier:   leftFrontier,
				RightFrontier:  rightFrontier,
				CommonFrontier: common,
			}, nil
		}
	}
	if result, ready := alignment.CommonFrontier(); ready && result.CommonFrontier >= frontier {
		return result, nil
	}

	waitContext, cancel := context.WithCancel(ctx)
	defer cancel()
	type waitResult struct {
		left     bool
		frontier uint64
		err      error
	}
	results := make(chan waitResult, 2)
	go func() {
		frontier, err := alignment.left.WaitForFrontier(waitContext, frontier)
		results <- waitResult{left: true, frontier: frontier, err: err}
	}()
	go func() {
		frontier, err := alignment.right.WaitForFrontier(waitContext, frontier)
		results <- waitResult{frontier: frontier, err: err}
	}()

	var (
		alignedLeftFrontier  uint64
		alignedRightFrontier uint64
		firstErr             error
	)
	for range 2 {
		result := <-results
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
				cancel()
			}
			continue
		}
		if result.left {
			alignedLeftFrontier = result.frontier
		} else {
			alignedRightFrontier = result.frontier
		}
	}
	if firstErr != nil {
		return SQLTemporalJoinFrontierResult{}, firstErr
	}
	common := alignedLeftFrontier
	if alignedRightFrontier < common {
		common = alignedRightFrontier
	}
	return SQLTemporalJoinFrontierResult{
		LeftFrontier:   alignedLeftFrontier,
		RightFrontier:  alignedRightFrontier,
		CommonFrontier: common,
	}, nil
}

func sqlTemporalJoinCachedFrontier(barrier *SQLSourceFrontierBarrier) (uint64, bool) {
	if barrier == nil || !barrier.ready.Load() {
		return 0, false
	}
	return barrier.common.Load(), true
}
