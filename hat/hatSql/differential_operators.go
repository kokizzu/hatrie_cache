package hatSql

import (
	"errors"
	"fmt"
)

var (
	// ErrDifferentialRowKeyRequired reports an update without a stable input identity.
	ErrDifferentialRowKeyRequired = errors.New("hatSql: differential row key is required")
	// ErrDifferentialFilterCallbackRequired reports a missing filter callback.
	ErrDifferentialFilterCallbackRequired = errors.New("hatSql: differential filter callback is required")
	// ErrDifferentialMapCallbackRequired reports a missing map callback.
	ErrDifferentialMapCallbackRequired = errors.New("hatSql: differential map callback is required")
	// ErrDifferentialJoinMatchRequired reports a missing join predicate.
	ErrDifferentialJoinMatchRequired = errors.New("hatSql: differential join match callback is required")
	// ErrDifferentialJoinProjectRequired reports a missing join projection.
	ErrDifferentialJoinProjectRequired = errors.New("hatSql: differential join project callback is required")
	// ErrDifferentialJoinWeightOverflow reports an unrepresentable signed weight product.
	ErrDifferentialJoinWeightOverflow = errors.New("hatSql: differential join weight overflow")
)

// DifferentialFilterFunc decides whether one weighted row remains in the
// stream. The row is a private clone and must be treated as read-only.
type DifferentialFilterFunc func(Row) (bool, error)

// DifferentialMapFunc maps one row to an output identity and row. Multiple
// input rows may map to the same output identity; their signed weights are
// consolidated after mapping.
type DifferentialMapFunc func(Row) (key string, row Row, err error)

// DifferentialFlatMapResult is one output row from a one-to-many transform.
type DifferentialFlatMapResult struct {
	Key string
	Row Row
}

// DifferentialFlatMapFunc expands one input row. Each result inherits the
// input timestamp and signed weight.
type DifferentialFlatMapFunc func(Row) ([]DifferentialFlatMapResult, error)

// DifferentialJoinMatchFunc identifies matching rows. Both rows are private
// clones and must be treated as read-only.
type DifferentialJoinMatchFunc func(left, right Row) (bool, error)

// DifferentialJoinProjectFunc creates the output identity and row for one
// matching pair. Both rows are private clones and must be treated as
// read-only. The output timestamp is the later input timestamp, matching the
// usual differential-dataflow join timestamp rule.
type DifferentialJoinProjectFunc func(left, right Row) (key string, row Row, err error)

// FilterDifferentialRows applies a selection while preserving every non-zero
// signed weight and duplicate update. It returns no partial output on error.
func FilterDifferentialRows(rows []DifferentialRow, keep DifferentialFilterFunc) ([]DifferentialRow, error) {
	if keep == nil {
		return nil, ErrDifferentialFilterCallbackRequired
	}
	filtered := make([]DifferentialRow, 0, len(rows))
	for _, update := range rows {
		if update.Key == "" {
			return nil, ErrDifferentialRowKeyRequired
		}
		if update.Diff == 0 {
			continue
		}
		row := cloneDifferentialRow(update.Row)
		selected, err := keep(row)
		if err != nil {
			return nil, fmt.Errorf("filter differential row %q: %w", update.Key, err)
		}
		if selected {
			update.Row = row
			filtered = append(filtered, update)
		}
	}
	if len(filtered) == 0 {
		return nil, nil
	}
	return filtered, nil
}

// MapDifferentialRows applies a one-to-one projection while preserving
// timestamps and signed weights. Output rows with the same key and timestamp
// are consolidated, so duplicate multiplicity and negative updates remain
// exact without requiring a caller-side deduplication pass.
func MapDifferentialRows(rows []DifferentialRow, mapRow DifferentialMapFunc) ([]DifferentialRow, error) {
	if mapRow == nil {
		return nil, ErrDifferentialMapCallbackRequired
	}
	mapped := make([]DifferentialRow, 0, len(rows))
	for _, update := range rows {
		if update.Key == "" {
			return nil, ErrDifferentialRowKeyRequired
		}
		if update.Diff == 0 {
			continue
		}
		key, row, err := mapRow(cloneDifferentialRow(update.Row))
		if err != nil {
			return nil, fmt.Errorf("map differential row %q: %w", update.Key, err)
		}
		if key == "" {
			return nil, ErrDifferentialRowKeyRequired
		}
		mapped = append(mapped, DifferentialRow{Key: key, Time: update.Time, Diff: update.Diff, Row: cloneDifferentialRow(row)})
	}
	return consolidateDifferentialRows(mapped, false)
}

// FlatMapDifferentialRows applies a one-to-many transform while preserving
// each input update's timestamp and signed weight. Output identities are
// consolidated after expansion.
func FlatMapDifferentialRows(rows []DifferentialRow, expand DifferentialFlatMapFunc) ([]DifferentialRow, error) {
	if expand == nil {
		return nil, ErrDifferentialMapCallbackRequired
	}
	expanded := make([]DifferentialRow, 0, len(rows))
	for _, update := range rows {
		if update.Key == "" {
			return nil, ErrDifferentialRowKeyRequired
		}
		if update.Diff == 0 {
			continue
		}
		results, err := expand(cloneDifferentialRow(update.Row))
		if err != nil {
			return nil, fmt.Errorf("flat-map differential row %q: %w", update.Key, err)
		}
		for _, result := range results {
			if result.Key == "" {
				return nil, ErrDifferentialRowKeyRequired
			}
			expanded = append(expanded, DifferentialRow{
				Key:  result.Key,
				Time: update.Time,
				Diff: update.Diff,
				Row:  cloneDifferentialRow(result.Row),
			})
		}
	}
	return consolidateDifferentialRows(expanded, false)
}

// UnionDifferentialRows combines UNION ALL-style batches. Signed weights and
// duplicate multiplicity are retained, while equal identities are compacted.
func UnionDifferentialRows(batches ...[]DifferentialRow) ([]DifferentialRow, error) {
	length := 0
	for _, batch := range batches {
		if len(batch) > int(^uint(0)>>1)-length {
			return nil, fmt.Errorf("union differential rows: input length overflow")
		}
		length += len(batch)
	}
	combined := make([]DifferentialRow, 0, length)
	for _, batch := range batches {
		for _, update := range batch {
			update.Row = cloneDifferentialRow(update.Row)
			combined = append(combined, update)
		}
	}
	return consolidateDifferentialRows(combined, false)
}

// JoinDifferentialRows computes an inner join over two weighted batches. A
// matching pair contributes left.Diff*right.Diff, so negative updates and
// duplicate multiplicity follow the relational multiset product. The output
// timestamp is max(left.Time, right.Time), and the result is consolidated by
// output identity and timestamp. It returns no partial output on error.
func JoinDifferentialRows(left, right []DifferentialRow, match DifferentialJoinMatchFunc, project DifferentialJoinProjectFunc) ([]DifferentialRow, error) {
	if match == nil {
		return nil, ErrDifferentialJoinMatchRequired
	}
	if project == nil {
		return nil, ErrDifferentialJoinProjectRequired
	}
	leftRows, err := ConsolidateDifferentialRows(left)
	if err != nil {
		return nil, fmt.Errorf("consolidate left differential rows: %w", err)
	}
	rightRows, err := ConsolidateDifferentialRows(right)
	if err != nil {
		return nil, fmt.Errorf("consolidate right differential rows: %w", err)
	}
	joined := make([]DifferentialRow, 0)
	for _, leftUpdate := range leftRows {
		for _, rightUpdate := range rightRows {
			matches, err := match(leftUpdate.Row, rightUpdate.Row)
			if err != nil {
				return nil, fmt.Errorf("match differential rows %q and %q: %w", leftUpdate.Key, rightUpdate.Key, err)
			}
			if !matches {
				continue
			}
			weight, ok := multiplyDifferentialCounts(leftUpdate.Diff, rightUpdate.Diff)
			if !ok {
				return nil, fmt.Errorf("join differential rows %q and %q: %w", leftUpdate.Key, rightUpdate.Key, ErrDifferentialJoinWeightOverflow)
			}
			key, row, err := project(leftUpdate.Row, rightUpdate.Row)
			if err != nil {
				return nil, fmt.Errorf("project differential rows %q and %q: %w", leftUpdate.Key, rightUpdate.Key, err)
			}
			if key == "" {
				return nil, ErrDifferentialRowKeyRequired
			}
			time := leftUpdate.Time
			if rightUpdate.Time > time {
				time = rightUpdate.Time
			}
			joined = append(joined, DifferentialRow{Key: key, Time: time, Diff: weight, Row: cloneDifferentialRow(row)})
		}
	}
	return consolidateDifferentialRows(joined, false)
}
