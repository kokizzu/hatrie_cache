package hatSql

import "fmt"

// GroupCountSumInt64DifferentialRows maintains exact differential COUNT and
// signed integer SUM values grouped by a caller-provided key. Each changed
// group emits one retraction containing its previous count and sum followed by
// one insertion containing the new values. A group entering the result emits
// only an insertion, and a group leaving it emits only a retraction.
//
// The operation is batch-scoped and does not mutate input rows. Group presence
// is tracked independently from the sum, so a present group with a zero sum is
// still represented. It returns no partial output when multiplicity becomes
// negative, arithmetic overflows, or the value callback fails.
func GroupCountSumInt64DifferentialRows(rows []DifferentialRow, groupKey DifferentialGroupByKeyFunc, value DifferentialInt64ValueFunc) ([]DifferentialRow, error) {
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
				Row:  Row{"count": current.count, "sum": current.sum},
			})
		}
		if nextCount > 0 {
			emitted = append(emitted, DifferentialRow{
				Key:  key,
				Time: update.Time,
				Diff: 1,
				Row:  Row{"count": nextCount, "sum": nextSum},
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
