package hatSql

import (
	"context"
	"errors"
	"fmt"
)

const (
	// DefaultSQLRecursiveDataflowMaxIterations bounds recursive rounds when the
	// caller does not provide a workload-specific limit.
	DefaultSQLRecursiveDataflowMaxIterations = 1 << 10
	// DefaultSQLRecursiveDataflowMaxRows bounds the retained fixed-point result.
	DefaultSQLRecursiveDataflowMaxRows = 1 << 20
	// MaxSQLRecursiveDataflowIterations prevents accidental unbounded execution.
	MaxSQLRecursiveDataflowIterations = 1 << 20
	// MaxSQLRecursiveDataflowRows prevents accidental unbounded retention.
	MaxSQLRecursiveDataflowRows = 1 << 24
)

var (
	// ErrSQLRecursiveDataflowInvalid reports a nil step, nil flow, or invalid
	// bounds.
	ErrSQLRecursiveDataflowInvalid = errors.New("hatSql: recursive dataflow input is invalid")
	// ErrSQLRecursiveDataflowContextRequired reports a nil execution context.
	ErrSQLRecursiveDataflowContextRequired = errors.New("hatSql: recursive dataflow context is required")
	// ErrSQLRecursiveDataflowLimit reports an iteration or retained-row bound.
	ErrSQLRecursiveDataflowLimit = errors.New("hatSql: recursive dataflow limit exceeded")
)

// SQLRecursiveDataflowOptions bounds one fixed-point execution. Zero selects
// the bounded defaults. A recursive step must eventually return no new values;
// limits make non-converging or adversarial steps fail closed.
type SQLRecursiveDataflowOptions struct {
	MaxIterations int
	MaxRows       int
}

// SQLRecursiveDataflowStep receives the complete result and only the values
// newly added in the previous round. It returns candidate values for the next
// round. The input slices are read-only and ownership of the returned slice is
// transferred to the executor.
type SQLRecursiveDataflowStep[T comparable] func(context.Context, []T, []T) ([]T, error)

// SQLRecursiveDataflow maintains a bounded monotone fixed point. Values are
// deduplicated by Go equality and results preserve first-seen order, which
// makes cycles deterministic without rescanning the complete result each round.
type SQLRecursiveDataflow[T comparable] struct {
	seed          []T
	step          SQLRecursiveDataflowStep[T]
	maxIterations int
	maxRows       int
}

// NewSQLRecursiveDataflow validates and snapshots a recursive seed and step.
// Duplicate seed values are retained once in first-seen order.
func NewSQLRecursiveDataflow[T comparable](seed []T, step SQLRecursiveDataflowStep[T], options SQLRecursiveDataflowOptions) (*SQLRecursiveDataflow[T], error) {
	if step == nil {
		return nil, ErrSQLRecursiveDataflowInvalid
	}
	if options.MaxIterations == 0 {
		options.MaxIterations = DefaultSQLRecursiveDataflowMaxIterations
	}
	if options.MaxRows == 0 {
		options.MaxRows = DefaultSQLRecursiveDataflowMaxRows
	}
	if options.MaxIterations < 1 || options.MaxIterations > MaxSQLRecursiveDataflowIterations || options.MaxRows < 1 || options.MaxRows > MaxSQLRecursiveDataflowRows {
		return nil, fmt.Errorf("%w: iterations=%d rows=%d", ErrSQLRecursiveDataflowInvalid, options.MaxIterations, options.MaxRows)
	}
	values := make([]T, 0, len(seed))
	seen := make(map[T]struct{}, len(seed))
	for _, value := range seed {
		if _, ok := seen[value]; ok {
			continue
		}
		if len(values) == options.MaxRows {
			return nil, ErrSQLRecursiveDataflowLimit
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	return &SQLRecursiveDataflow[T]{
		seed:          values,
		step:          step,
		maxIterations: options.MaxIterations,
		maxRows:       options.MaxRows,
	}, nil
}

// Run computes one independent fixed point. The returned slice is owned by
// the caller; subsequent runs start from the immutable seed.
func (flow *SQLRecursiveDataflow[T]) Run(ctx context.Context) ([]T, error) {
	if flow == nil || flow.step == nil {
		return nil, ErrSQLRecursiveDataflowInvalid
	}
	if ctx == nil {
		return nil, ErrSQLRecursiveDataflowContextRequired
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	all := append([]T(nil), flow.seed...)
	if len(all) == 0 {
		return all, nil
	}
	seen := make(map[T]struct{}, len(all))
	for _, value := range all {
		seen[value] = struct{}{}
	}
	delta := append([]T(nil), all...)
	for iteration := 0; ; iteration++ {
		if iteration >= flow.maxIterations {
			return nil, fmt.Errorf("%w: iterations=%d", ErrSQLRecursiveDataflowLimit, flow.maxIterations)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		candidates, err := flow.step(ctx, all, delta)
		if err != nil {
			return nil, fmt.Errorf("recursive dataflow iteration %d: %w", iteration, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(candidates) == 0 {
			return all, nil
		}
		nextDelta := candidates[:0]
		for _, candidate := range candidates {
			if _, ok := seen[candidate]; ok {
				continue
			}
			if len(all)+len(nextDelta) >= flow.maxRows {
				return nil, fmt.Errorf("%w: rows=%d", ErrSQLRecursiveDataflowLimit, flow.maxRows)
			}
			seen[candidate] = struct{}{}
			nextDelta = append(nextDelta, candidate)
		}
		if len(nextDelta) == 0 {
			return all, nil
		}
		all = append(all, nextDelta...)
		delta = nextDelta
	}
}
