package hatSql

import (
	"errors"
	"fmt"
)

var (
	ErrDifferentialGroupByValueRequired = errors.New("hatSql: differential group-by value callback is required")
	ErrDifferentialGroupBySumOverflow   = errors.New("hatSql: differential group-by sum overflowed")
)

// DifferentialInt64ValueFunc extracts the signed integer value contributed by
// one differential row. Retraction rows must provide the same value as the
// insertion they retract.
type DifferentialInt64ValueFunc func(SQLRow) (int64, error)

type differentialGroupSumState struct {
	count int64
	sum   int64
}

// GroupSumInt64DifferentialRows maintains an exact differential SUM grouped
// by a caller-provided key. Each changed group is represented by a retraction
// of its previous sum and an insertion of its new sum. A group entering the
// result emits only an insertion, and a group leaving it emits only a
// retraction. The input order and update timestamps are preserved.
//
// Group presence is tracked independently from the sum, so a group whose sum
// is zero still emits an aggregate row while it has positive multiplicity.
// The function is batch-scoped, does not mutate input rows, and returns no
// partial output when a count or sum would become invalid or overflow.
func GroupSumInt64DifferentialRows(rows []DifferentialRow, groupKey DifferentialGroupByKeyFunc, value DifferentialInt64ValueFunc) ([]DifferentialRow, error) {
	if groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if value == nil {
		return nil, ErrDifferentialGroupByValueRequired
	}
	if len(rows) == 0 {
		return nil, nil
	}

	states := make(map[string]differentialGroupSumState, len(rows))
	emitted := make([]DifferentialRow, 0, len(rows)*2)
	for _, update := range rows {
		if update.Diff == 0 {
			continue
		}
		key := groupKey(update.Row)
		current := states[key]
		nextCount, ok := addDifferentialCounts(current.count, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByCountOverflow)
		}
		if nextCount < 0 {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByNegativeCount)
		}

		rowValue, err := value(update.Row)
		if err != nil {
			return nil, fmt.Errorf("group %q: differential value: %w", key, err)
		}
		delta, ok := multiplyDifferentialInt64(rowValue, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupBySumOverflow)
		}
		nextSum, ok := addDifferentialCounts(current.sum, delta)
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupBySumOverflow)
		}

		if current.count > 0 {
			emitted = append(emitted, DifferentialRow{
				Key:  key,
				Time: update.Time,
				Diff: -1,
				Row:  Row{"sum": current.sum},
			})
		}
		if nextCount > 0 {
			emitted = append(emitted, DifferentialRow{
				Key:  key,
				Time: update.Time,
				Diff: 1,
				Row:  Row{"sum": nextSum},
			})
			states[key] = differentialGroupSumState{count: nextCount, sum: nextSum}
		} else {
			delete(states, key)
		}
	}
	if len(emitted) == 0 {
		return nil, nil
	}
	return emitted, nil
}

const differentialMaxInt64 = int64(^uint64(0) >> 1)
const differentialMinInt64 = -differentialMaxInt64 - 1

func multiplyDifferentialInt64(left, right int64) (int64, bool) {
	if left == 0 || right == 0 {
		return 0, true
	}
	if left == -1 {
		if right == differentialMinInt64 {
			return 0, false
		}
		return -right, true
	}
	if right == -1 {
		if left == differentialMinInt64 {
			return 0, false
		}
		return -left, true
	}

	switch {
	case left > 0 && right > 0:
		if left > differentialMaxInt64/right {
			return 0, false
		}
	case left > 0:
		if right < differentialMinInt64/left {
			return 0, false
		}
	case right > 0:
		if left < differentialMinInt64/right {
			return 0, false
		}
	case left < differentialMaxInt64/right:
		return 0, false
	}
	return left * right, true
}
