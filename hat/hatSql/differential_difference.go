package hatSql

import (
	"errors"
	"fmt"
	"math"
)

var (
	// ErrDifferentialDifferenceOverflow reports a subtraction that cannot be
	// represented because negating math.MinInt64 is not possible.
	ErrDifferentialDifferenceOverflow = errors.New("hatSql: differential difference weight overflow")
)

// NegateDifferentialRows reverses the signed weight of every non-zero update.
// Row values are cloned, so the input batch remains safe to reuse. A zero
// weight is a no-op and is omitted from the result.
func NegateDifferentialRows(rows []DifferentialRow) ([]DifferentialRow, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	negated := make([]DifferentialRow, 0, len(rows))
	for _, update := range rows {
		if update.Key == "" {
			return nil, ErrDifferentialRowKeyRequired
		}
		if update.Diff == 0 {
			continue
		}
		if update.Diff == math.MinInt64 {
			return nil, ErrDifferentialDifferenceOverflow
		}
		update.Diff = -update.Diff
		update.Row = cloneDifferentialRow(update.Row)
		negated = append(negated, update)
	}
	if len(negated) == 0 {
		return nil, nil
	}
	return negated, nil
}

// ExceptDifferentialRows computes the signed differential difference
// left-right. Equal keys at the same logical time are consolidated in input
// order, duplicate multiplicity is preserved in the resulting weight, and
// zero-sum updates are removed. Neither input batch is mutated.
func ExceptDifferentialRows(left, right []DifferentialRow) ([]DifferentialRow, error) {
	if len(right) > int(^uint(0)>>1)-len(left) {
		return nil, fmt.Errorf("differential difference rows: input length overflow")
	}
	capacity := len(left) + len(right)
	if capacity == 0 {
		return nil, nil
	}
	indexes := make(map[differentialRowKey]int, capacity)
	result := make([]DifferentialRow, 0, capacity)
	appendUpdate := func(update DifferentialRow, negate bool) error {
		if update.Key == "" {
			return ErrDifferentialRowKeyRequired
		}
		diff := update.Diff
		if negate {
			if diff == math.MinInt64 {
				return ErrDifferentialDifferenceOverflow
			}
			diff = -diff
		}
		if diff == 0 {
			return nil
		}
		identity := differentialRowKey{key: update.Key, time: update.Time}
		if outputIndex, exists := indexes[identity]; exists {
			combined, ok := addDifferentialCounts(result[outputIndex].Diff, diff)
			if !ok {
				return fmt.Errorf("differential difference row %q at time %d overflows diff: %w", update.Key, update.Time, ErrDifferentialDifferenceOverflow)
			}
			result[outputIndex].Diff = combined
			if combined == 0 {
				delete(indexes, identity)
			}
			return nil
		}
		update.Diff = diff
		update.Row = cloneDifferentialRow(update.Row)
		indexes[identity] = len(result)
		result = append(result, update)
		return nil
	}

	for _, update := range left {
		if err := appendUpdate(update, false); err != nil {
			return nil, err
		}
	}
	for _, update := range right {
		if err := appendUpdate(update, true); err != nil {
			return nil, err
		}
	}
	remaining := result[:0]
	for _, update := range result {
		if update.Diff != 0 {
			remaining = append(remaining, update)
		}
	}
	if len(remaining) == 0 {
		return nil, nil
	}
	return remaining, nil
}
