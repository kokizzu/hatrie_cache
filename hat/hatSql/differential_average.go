package hatSql

import "fmt"

// GroupAverageInt64DifferentialRows maintains an exact differential AVG over
// int64 values grouped by a caller-provided key. Each changed group emits one
// retraction containing its previous average followed by one insertion
// containing the new average. A group entering the result emits only an
// insertion, and a group leaving it emits only a retraction.
//
// The count and sum are retained as exact int64 state until the output is
// materialized as float64. This preserves weighted duplicate semantics and
// rejects count, multiplication, and sum overflow without returning partial
// output. The input rows and their maps are never modified.
func GroupAverageInt64DifferentialRows(rows []DifferentialRow, groupKey DifferentialGroupByKeyFunc, value DifferentialInt64ValueFunc) ([]DifferentialRow, error) {
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
				Row:  Row{"avg": float64(current.sum) / float64(current.count)},
			})
		}
		if nextCount > 0 {
			emitted = append(emitted, DifferentialRow{
				Key:  key,
				Time: update.Time,
				Diff: 1,
				Row:  Row{"avg": float64(nextSum) / float64(nextCount)},
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
