package hatSql

import (
	"errors"
	"sort"
)

// ErrIncrementalGroupMinMaxNil reports a method call on a nil operator.
var ErrIncrementalGroupMinMaxNil = errors.New("hatSql: incremental group min-max operator is nil")

type incrementalGroupMinMaxState struct {
	count  int64
	values map[int64]int64
	min    int64
	max    int64
}

type incrementalGroupMinMaxPending struct {
	current incrementalGroupMinMaxState
	next    incrementalGroupMinMaxState
	time    uint64
}

// IncrementalGroupMinMaxInt64 maintains exact grouped MIN and MAX values from
// signed differential updates. Value multiplicities are retained so removing
// one endpoint restores the next endpoint without rebuilding unrelated groups.
// It is single-writer; callers sharing one instance between goroutines must
// provide synchronization around Apply and Snapshot.
type IncrementalGroupMinMaxInt64 struct {
	groupKey DifferentialGroupByKeyFunc
	value    DifferentialInt64ValueFunc
	states   map[string]incrementalGroupMinMaxState
}

// NewIncrementalGroupMinMaxInt64 creates an empty retained grouped extrema
// operator.
func NewIncrementalGroupMinMaxInt64(groupKey DifferentialGroupByKeyFunc, value DifferentialInt64ValueFunc) (*IncrementalGroupMinMaxInt64, error) {
	if groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if value == nil {
		return nil, ErrDifferentialGroupByValueRequired
	}
	return &IncrementalGroupMinMaxInt64{
		groupKey: groupKey,
		value:    value,
		states:   make(map[string]incrementalGroupMinMaxState),
	}, nil
}

// Apply validates and applies one signed differential batch atomically. Each
// changed visible group emits a retraction of its previous extrema followed by
// an insertion of its new extrema. A group whose multiplicity changes without
// changing MIN or MAX updates retained value counts without emitting a row.
func (operator *IncrementalGroupMinMaxInt64) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if operator == nil {
		return nil, ErrIncrementalGroupMinMaxNil
	}
	if len(updates) == 0 {
		return nil, nil
	}

	pending := make(map[string]incrementalGroupMinMaxPending, len(updates))
	order := make([]string, 0, len(updates))
	for _, update := range updates {
		if update.Diff == 0 {
			continue
		}
		key := operator.groupKey(update.Row)
		rowValue, err := operator.value(update.Row)
		if err != nil {
			return nil, err
		}
		entry, exists := pending[key]
		if !exists {
			current := operator.states[key]
			entry = incrementalGroupMinMaxPending{
				current: current,
				next:    cloneIncrementalGroupMinMaxState(current),
			}
			order = append(order, key)
		}

		nextCount, ok := addDifferentialCounts(entry.next.count, update.Diff)
		if !ok {
			return nil, ErrDifferentialGroupByCountOverflow
		}
		if nextCount < 0 {
			return nil, ErrDifferentialGroupByNegativeCount
		}
		currentValueCount := entry.next.values[rowValue]
		nextValueCount, ok := addDifferentialCounts(currentValueCount, update.Diff)
		if !ok {
			return nil, ErrDifferentialGroupByValueCountOverflow
		}
		if nextValueCount < 0 {
			return nil, ErrDifferentialGroupByValueMultiplicity
		}
		if entry.next.values == nil {
			entry.next.values = make(map[int64]int64)
		}
		if nextValueCount == 0 {
			delete(entry.next.values, rowValue)
		} else {
			entry.next.values[rowValue] = nextValueCount
		}

		previous := entry.next
		if nextCount == 0 {
			entry.next = incrementalGroupMinMaxState{}
		} else {
			entry.next.count = nextCount
			entry.next.min, entry.next.max = incrementalNextGroupMinMax(entry.next.values, previous, rowValue, update.Diff, nextValueCount)
		}
		entry.time = update.Time
		pending[key] = entry
	}
	if len(order) == 0 {
		return nil, nil
	}

	changes := make([]DifferentialRow, 0, len(order)*2)
	for _, key := range order {
		entry := pending[key]
		if entry.current.count > 0 && entry.next.count > 0 && entry.current.min == entry.next.min && entry.current.max == entry.next.max {
			if entry.next.count > 0 {
				operator.states[key] = entry.next
			}
			continue
		}
		if entry.current.count == 0 && entry.next.count == 0 {
			continue
		}
		switch {
		case entry.current.count == 0 && entry.next.count > 0:
			changes = append(changes, DifferentialRow{Key: key, Time: entry.time, Diff: 1, Row: incrementalGroupMinMaxRow(entry.next)})
		case entry.current.count > 0 && entry.next.count == 0:
			changes = append(changes, DifferentialRow{Key: key, Time: entry.time, Diff: -1, Row: incrementalGroupMinMaxRow(entry.current)})
		case entry.current.count > 0 && entry.next.count > 0:
			changes = append(changes,
				DifferentialRow{Key: key, Time: entry.time, Diff: -1, Row: incrementalGroupMinMaxRow(entry.current)},
				DifferentialRow{Key: key, Time: entry.time, Diff: 1, Row: incrementalGroupMinMaxRow(entry.next)},
			)
		}
		if entry.next.count > 0 {
			operator.states[key] = entry.next
		} else {
			delete(operator.states, key)
		}
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}

// Snapshot returns one positive row per retained group in lexical key order.
func (operator *IncrementalGroupMinMaxInt64) Snapshot() []DifferentialRow {
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
		state := operator.states[key]
		rows = append(rows, DifferentialRow{Key: key, Diff: 1, Row: incrementalGroupMinMaxRow(state)})
	}
	return rows
}

func cloneIncrementalGroupMinMaxState(state incrementalGroupMinMaxState) incrementalGroupMinMaxState {
	if len(state.values) == 0 {
		state.values = nil
		return state
	}
	values := make(map[int64]int64, len(state.values))
	for value, count := range state.values {
		values[value] = count
	}
	state.values = values
	return state
}

func incrementalNextGroupMinMax(values map[int64]int64, previous incrementalGroupMinMaxState, rowValue, diff int64, nextValueCount int64) (int64, int64) {
	if previous.count == 0 {
		return rowValue, rowValue
	}
	minValue, maxValue := previous.min, previous.max
	if diff > 0 {
		if rowValue < minValue {
			minValue = rowValue
		}
		if rowValue > maxValue {
			maxValue = rowValue
		}
		return minValue, maxValue
	}
	if nextValueCount > 0 || (rowValue != previous.min && rowValue != previous.max) {
		return minValue, maxValue
	}
	return incrementalScanGroupMinMax(values)
}

func incrementalScanGroupMinMax(values map[int64]int64) (int64, int64) {
	first := true
	var minValue, maxValue int64
	for value, count := range values {
		if count <= 0 {
			continue
		}
		if first || value < minValue {
			minValue = value
		}
		if first || value > maxValue {
			maxValue = value
		}
		first = false
	}
	return minValue, maxValue
}

func incrementalGroupMinMaxRow(state incrementalGroupMinMaxState) Row {
	return Row{"min": state.min, "max": state.max}
}
