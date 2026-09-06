package hatSql

import (
	"errors"
	"fmt"
)

var (
	ErrDifferentialDistinctNegativeMultiplicity = errors.New("hatSql: differential distinct multiplicity became negative")
	ErrDifferentialDistinctOverflow            = errors.New("hatSql: differential distinct multiplicity overflowed")
)

// DistinctDifferentialRows maintains set membership from signed differential
// updates. It emits +1 only when a key enters the set and -1 only when a key
// leaves it; changes while multiplicity remains positive are suppressed.
// Updates are processed in input order, and emitted row payloads are shallow
// copies of the triggering update.
func DistinctDifferentialRows(rows []DifferentialRow) ([]DifferentialRow, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	multiplicity := make(map[string]uint64, len(rows))
	emitted := make([]DifferentialRow, 0, len(rows))
	for _, update := range rows {
		if update.Diff == 0 {
			continue
		}
		current := multiplicity[update.Key]
		next, err := nextDifferentialDistinctMultiplicity(current, update.Diff)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", update.Key, err)
		}
		if next == 0 {
			delete(multiplicity, update.Key)
		} else {
			multiplicity[update.Key] = next
		}

		switch {
		case current == 0 && next > 0:
			emitted = append(emitted, DifferentialRow{Key: update.Key, Time: update.Time, Diff: 1, Row: cloneDifferentialDistinctRow(update.Row)})
		case current > 0 && next == 0:
			emitted = append(emitted, DifferentialRow{Key: update.Key, Time: update.Time, Diff: -1, Row: cloneDifferentialDistinctRow(update.Row)})
		}
	}
	if len(emitted) == 0 {
		return nil, nil
	}
	return emitted, nil
}

func nextDifferentialDistinctMultiplicity(current uint64, diff int64) (uint64, error) {
	if diff > 0 {
		increment := uint64(diff)
		if ^uint64(0)-current < increment {
			return 0, ErrDifferentialDistinctOverflow
		}
		return current + increment, nil
	}
	decrement := uint64(-(diff + 1)) + 1
	if decrement > current {
		return 0, ErrDifferentialDistinctNegativeMultiplicity
	}
	return current - decrement, nil
}

func cloneDifferentialDistinctRow(row Row) Row {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = value
	}
	return clone
}
