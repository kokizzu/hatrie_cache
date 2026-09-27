package hatSql

import (
	"errors"
	"fmt"
	"sort"
)

// ErrIncrementalGroupCountDistinctInt64Nil reports a method call on a nil
// operator.
var ErrIncrementalGroupCountDistinctInt64Nil = errors.New("hatSql: incremental grouped distinct-count operator is nil")

// IncrementalGroupCountDistinctInt64 maintains exact differential
// COUNT(DISTINCT int64) values for each group. Per-value multiplicities are
// retained so duplicate inserts and retractions do not change the visible
// distinct count until a value enters or leaves its group. The operator is
// single-writer; callers sharing it between goroutines must synchronize access.
type IncrementalGroupCountDistinctInt64 struct {
	groupKey DifferentialGroupByKeyFunc
	value    DifferentialInt64ValueFunc
	states   map[string]incrementalGroupCountDistinctState
}

type incrementalGroupCountDistinctState struct {
	count    int64
	distinct int64
	values   map[int64]int64
}

type incrementalGroupCountDistinctPending struct {
	current incrementalGroupCountDistinctState
	next    incrementalGroupCountDistinctState
	time    uint64
}

// NewIncrementalGroupCountDistinctInt64 creates an empty retained grouped
// distinct-count operator.
func NewIncrementalGroupCountDistinctInt64(groupKey DifferentialGroupByKeyFunc, value DifferentialInt64ValueFunc) (*IncrementalGroupCountDistinctInt64, error) {
	if groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if value == nil {
		return nil, ErrDifferentialGroupByValueRequired
	}
	return &IncrementalGroupCountDistinctInt64{
		groupKey: groupKey,
		value:    value,
		states:   make(map[string]incrementalGroupCountDistinctState),
	}, nil
}

// Apply validates and applies one signed differential batch atomically. Each
// changed group's aggregate emits a retraction followed by an insertion. A
// duplicate value whose distinct membership does not change emits no output.
func (operator *IncrementalGroupCountDistinctInt64) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if operator == nil {
		return nil, ErrIncrementalGroupCountDistinctInt64Nil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 1 {
		return operator.applySingle(updates[0])
	}

	pending := make(map[string]incrementalGroupCountDistinctPending, len(updates))
	order := make([]string, 0, len(updates))
	for _, update := range updates {
		if update.Diff == 0 {
			continue
		}
		key := operator.groupKey(update.Row)
		entry, exists := pending[key]
		if !exists {
			current := operator.states[key]
			entry = incrementalGroupCountDistinctPending{
				current: current,
				next: incrementalGroupCountDistinctState{
					count:    current.count,
					distinct: current.distinct,
					values:   cloneIncrementalGroupCountDistinctValues(current.values),
				},
			}
			order = append(order, key)
		}

		rowValue, err := operator.value(update.Row)
		if err != nil {
			return nil, fmt.Errorf("group %q: differential value: %w", key, err)
		}
		nextCount, ok := addDifferentialCounts(entry.next.count, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByCountOverflow)
		}
		if nextCount < 0 {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByNegativeCount)
		}

		currentValueCount := entry.next.values[rowValue]
		nextValueCount, ok := addDifferentialCounts(currentValueCount, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group %q value %d: %w", key, rowValue, ErrDifferentialGroupByDistinctValueOverflow)
		}
		if nextValueCount < 0 {
			return nil, fmt.Errorf("group %q value %d: %w", key, rowValue, ErrDifferentialGroupByDistinctValueMultiplicity)
		}

		nextDistinct := entry.next.distinct
		switch {
		case currentValueCount == 0 && nextValueCount > 0:
			nextDistinct, ok = addDifferentialCounts(nextDistinct, 1)
		case currentValueCount > 0 && nextValueCount == 0:
			nextDistinct, ok = addDifferentialCounts(nextDistinct, -1)
		}
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByDistinctCountOverflow)
		}

		if nextValueCount == 0 {
			delete(entry.next.values, rowValue)
		} else {
			if entry.next.values == nil {
				entry.next.values = make(map[int64]int64)
			}
			entry.next.values[rowValue] = nextValueCount
		}
		entry.next.count = nextCount
		entry.next.distinct = nextDistinct
		entry.time = update.Time
		pending[key] = entry
	}
	if len(order) == 0 {
		return nil, nil
	}

	changes := make([]DifferentialRow, 0, len(order)*2)
	for _, key := range order {
		entry := pending[key]
		changes = appendIncrementalGroupCountDistinctChanges(changes, key, entry.time, entry.current, entry.next)
		if entry.next.count == 0 {
			delete(operator.states, key)
		} else {
			operator.states[key] = entry.next
		}
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}

func (operator *IncrementalGroupCountDistinctInt64) applySingle(update DifferentialRow) ([]DifferentialRow, error) {
	if update.Diff == 0 {
		return nil, nil
	}
	key := operator.groupKey(update.Row)
	current := operator.states[key]
	rowValue, err := operator.value(update.Row)
	if err != nil {
		return nil, fmt.Errorf("group %q: differential value: %w", key, err)
	}
	nextCount, ok := addDifferentialCounts(current.count, update.Diff)
	if !ok {
		return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByCountOverflow)
	}
	if nextCount < 0 {
		return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByNegativeCount)
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
		nextDistinct, ok = addDifferentialCounts(nextDistinct, 1)
	case currentValueCount > 0 && nextValueCount == 0:
		nextDistinct, ok = addDifferentialCounts(nextDistinct, -1)
	}
	if !ok {
		return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByDistinctCountOverflow)
	}

	next := current
	next.count = nextCount
	next.distinct = nextDistinct
	if nextValueCount == 0 {
		if next.values != nil {
			delete(next.values, rowValue)
		}
	} else {
		if next.values == nil {
			next.values = make(map[int64]int64)
		}
		next.values[rowValue] = nextValueCount
	}
	changes := appendIncrementalGroupCountDistinctChanges(nil, key, update.Time, current, next)
	if next.count == 0 {
		delete(operator.states, key)
	} else {
		operator.states[key] = next
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}

func appendIncrementalGroupCountDistinctChanges(changes []DifferentialRow, key string, time uint64, current, next incrementalGroupCountDistinctState) []DifferentialRow {
	switch {
	case current.count == 0 && next.count > 0:
		return append(changes, DifferentialRow{
			Key:  key,
			Time: time,
			Diff: 1,
			Row:  Row{"count_distinct": next.distinct},
		})
	case current.count > 0 && next.count == 0:
		return append(changes, DifferentialRow{
			Key:  key,
			Time: time,
			Diff: -1,
			Row:  Row{"count_distinct": current.distinct},
		})
	case current.distinct != next.distinct:
		return append(changes,
			DifferentialRow{Key: key, Time: time, Diff: -1, Row: Row{"count_distinct": current.distinct}},
			DifferentialRow{Key: key, Time: time, Diff: 1, Row: Row{"count_distinct": next.distinct}},
		)
	default:
		return changes
	}
}

// Snapshot returns one positive aggregate row per retained group in lexical
// group-key order. The returned rows do not expose retained value maps.
func (operator *IncrementalGroupCountDistinctInt64) Snapshot() []DifferentialRow {
	if operator == nil || len(operator.states) == 0 {
		return nil
	}
	keys := make([]string, 0, len(operator.states))
	for key := range operator.states {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]DifferentialRow, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, DifferentialRow{
			Key:  key,
			Diff: 1,
			Row:  Row{"count_distinct": operator.states[key].distinct},
		})
	}
	return rows
}

func cloneIncrementalGroupCountDistinctValues(values map[int64]int64) map[int64]int64 {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[int64]int64, len(values))
	for key, count := range values {
		cloned[key] = count
	}
	return cloned
}
