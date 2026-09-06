package hatSql

import (
	"errors"
	"fmt"
)

var (
	ErrDifferentialGroupByKeyRequired   = errors.New("hatSql: differential group-by key callback is required")
	ErrDifferentialGroupByNegativeCount = errors.New("hatSql: differential group-by count became negative")
	ErrDifferentialGroupByCountOverflow = errors.New("hatSql: differential group-by count overflowed")
)

// DifferentialGroupByKeyFunc returns the group identity for one input row.
// The identity is used as DifferentialRow.Key in the emitted aggregate rows.
type DifferentialGroupByKeyFunc func(SQLRow) string

// GroupCountDifferentialRows maintains an exact differential COUNT grouped by
// a caller-provided key. Each changed group is represented by a retraction of
// its previous count and an insertion of its new count. A group entering the
// result emits only an insertion, and a group leaving it emits only a
// retraction. The input order and update timestamps are preserved.
//
// The returned Row contains only the aggregate field "count". The function is
// batch-scoped and does not mutate the input rows. It returns no partial output
// when a count would become negative or overflow int64.
func GroupCountDifferentialRows(rows []DifferentialRow, groupKey DifferentialGroupByKeyFunc) ([]DifferentialRow, error) {
	if groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if len(rows) == 0 {
		return nil, nil
	}

	counts := make(map[string]int64, len(rows))
	emitted := make([]DifferentialRow, 0, len(rows))
	for _, update := range rows {
		if update.Diff == 0 {
			continue
		}
		key := groupKey(update.Row)
		current := counts[key]
		next, ok := addDifferentialCounts(current, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByCountOverflow)
		}
		if next < 0 {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByNegativeCount)
		}

		if current > 0 {
			emitted = append(emitted, DifferentialRow{
				Key:  key,
				Time: update.Time,
				Diff: -1,
				Row:  Row{"count": current},
			})
		}
		if next > 0 {
			emitted = append(emitted, DifferentialRow{
				Key:  key,
				Time: update.Time,
				Diff: 1,
				Row:  Row{"count": next},
			})
			counts[key] = next
		} else {
			delete(counts, key)
		}
	}
	if len(emitted) == 0 {
		return nil, nil
	}
	return emitted, nil
}
