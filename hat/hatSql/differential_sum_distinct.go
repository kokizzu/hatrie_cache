package hatSql

import (
	"errors"
	"fmt"
)

var (
	ErrDifferentialGroupByDistinctValueMultiplicity = errors.New("hatSql: differential distinct value multiplicity became negative")
	ErrDifferentialGroupByDistinctValueOverflow     = errors.New("hatSql: differential distinct value multiplicity overflowed")
)

type differentialGroupSumDistinctState struct {
	count  int64
	values map[int64]int64
	sum    int64
}

// GroupSumDistinctInt64DifferentialRows maintains an exact signed differential
// SUM(DISTINCT value) grouped by a caller-provided key. Each value keeps a
// multiplicity so duplicate insertions and retractions do not change the
// visible sum until that value enters or leaves the group.
//
// A changed group emits a retraction of its previous sum followed by an
// insertion of its new sum. A group entering the result emits only an
// insertion, and a group leaving it emits only a retraction. The function is
// batch-scoped, preserves input order and update timestamps, does not mutate
// input rows, and returns no partial output on invalid counts, callback
// errors, or checked int64 overflow.
func GroupSumDistinctInt64DifferentialRows(rows []DifferentialRow, groupKey DifferentialGroupByKeyFunc, value DifferentialInt64ValueFunc) ([]DifferentialRow, error) {
	if groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if value == nil {
		return nil, ErrDifferentialGroupByValueRequired
	}
	if len(rows) == 0 {
		return nil, nil
	}

	states := make(map[string]differentialGroupSumDistinctState)
	emitted := make([]DifferentialRow, 0, differentialSumDistinctOutputCapacity(len(rows)))
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
		currentValueCount := current.values[rowValue]
		nextValueCount, ok := addDifferentialCounts(currentValueCount, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group %q value %d: %w", key, rowValue, ErrDifferentialGroupByDistinctValueOverflow)
		}
		if nextValueCount < 0 {
			return nil, fmt.Errorf("group %q value %d: %w", key, rowValue, ErrDifferentialGroupByDistinctValueMultiplicity)
		}

		nextSum := current.sum
		switch {
		case currentValueCount == 0 && nextValueCount > 0:
			nextSum, ok = addDifferentialCounts(current.sum, rowValue)
		case currentValueCount > 0 && nextValueCount == 0:
			nextSum, ok = subtractDifferentialInt64(current.sum, rowValue)
		}
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupBySumOverflow)
		}

		previous := current
		if current.values == nil {
			current.values = make(map[int64]int64)
		}
		if nextValueCount == 0 {
			delete(current.values, rowValue)
		} else {
			current.values[rowValue] = nextValueCount
		}

		switch {
		case previous.count == 0 && nextCount > 0:
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: Row{"sum": nextSum}})
		case previous.count > 0 && nextCount == 0:
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: Row{"sum": previous.sum}})
		case previous.sum != nextSum:
			emitted = append(emitted,
				DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: Row{"sum": previous.sum}},
				DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: Row{"sum": nextSum}},
			)
		}

		if nextCount == 0 {
			delete(states, key)
		} else {
			current.count = nextCount
			current.sum = nextSum
			states[key] = current
		}
	}
	if len(emitted) == 0 {
		return nil, nil
	}
	return emitted, nil
}

func subtractDifferentialInt64(left, right int64) (int64, bool) {
	if right == differentialMinInt64 {
		if left >= 0 {
			return 0, false
		}
		return left + differentialMaxInt64 + 1, true
	}
	return addDifferentialCounts(left, -right)
}

func differentialSumDistinctOutputCapacity(length int) int {
	if length > int(^uint(0)>>1)/2 {
		return length
	}
	return length * 2
}
