package hatSql

import (
	"errors"
	"fmt"
)

var ErrDifferentialGroupByDistinctCountOverflow = errors.New("hatSql: differential distinct count overflowed")

type differentialGroupCountDistinctState struct {
	count    int64
	values   map[int64]int64
	distinct int64
}

// GroupCountDistinctInt64DifferentialRows maintains an exact signed
// differential COUNT(DISTINCT value) grouped by a caller-provided key. Each
// value keeps a multiplicity so duplicate insertions and retractions do not
// change the visible count until that value enters or leaves the group.
//
// A changed group emits a retraction of its previous Row["count_distinct"]
// followed by an insertion of its new count. A group entering the result emits
// only an insertion, and a group leaving it emits only a retraction. The
// function is batch-scoped, preserves input order and update timestamps, does
// not mutate input rows, and returns no partial output on invalid counts,
// callback errors, or checked overflow.
func GroupCountDistinctInt64DifferentialRows(rows []DifferentialRow, groupKey DifferentialGroupByKeyFunc, value DifferentialInt64ValueFunc) ([]DifferentialRow, error) {
	if groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if value == nil {
		return nil, ErrDifferentialGroupByValueRequired
	}
	if len(rows) == 0 {
		return nil, nil
	}

	states := make(map[string]differentialGroupCountDistinctState)
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

		nextDistinct := current.distinct
		switch {
		case currentValueCount == 0 && nextValueCount > 0:
			nextDistinct, ok = addDifferentialCounts(current.distinct, 1)
		case currentValueCount > 0 && nextValueCount == 0:
			nextDistinct, ok = addDifferentialCounts(current.distinct, -1)
		}
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByDistinctCountOverflow)
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
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: Row{"count_distinct": nextDistinct}})
		case previous.count > 0 && nextCount == 0:
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: Row{"count_distinct": previous.distinct}})
		case previous.distinct != nextDistinct:
			emitted = append(emitted,
				DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: Row{"count_distinct": previous.distinct}},
				DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: Row{"count_distinct": nextDistinct}},
			)
		}

		if nextCount == 0 {
			delete(states, key)
		} else {
			current.count = nextCount
			current.distinct = nextDistinct
			states[key] = current
		}
	}
	if len(emitted) == 0 {
		return nil, nil
	}
	return emitted, nil
}
