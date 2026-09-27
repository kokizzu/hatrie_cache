package hatSql

import (
	"errors"
	"fmt"
	"sort"
)

// ErrIncrementalGroupCountSumInt64Nil reports a method call on a nil operator.
var ErrIncrementalGroupCountSumInt64Nil = errors.New("hatSql: incremental group count-sum operator is nil")

// IncrementalGroupCountSumInt64 maintains exact differential COUNT and signed
// int64 SUM values for each group. It is single-writer; callers sharing one
// instance between goroutines must provide synchronization.
type IncrementalGroupCountSumInt64 struct {
	groupKey DifferentialGroupByKeyFunc
	value    DifferentialInt64ValueFunc
	states   map[string]incrementalGroupCountSumState
}

type incrementalGroupCountSumState struct {
	count int64
	sum   int64
}

type incrementalGroupCountSumPending struct {
	current incrementalGroupCountSumState
	next    incrementalGroupCountSumState
	time    uint64
}

// NewIncrementalGroupCountSumInt64 creates an empty retained COUNT+SUM
// operator using the supplied group and value callbacks.
func NewIncrementalGroupCountSumInt64(groupKey DifferentialGroupByKeyFunc, value DifferentialInt64ValueFunc) (*IncrementalGroupCountSumInt64, error) {
	if groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if value == nil {
		return nil, ErrDifferentialGroupByValueRequired
	}
	return &IncrementalGroupCountSumInt64{
		groupKey: groupKey,
		value:    value,
		states:   make(map[string]incrementalGroupCountSumState),
	}, nil
}

// Apply validates and applies one signed differential batch atomically. Each
// changed group emits a retraction for its old aggregate row followed by an
// insertion for its new aggregate row. The timestamp is the last update time
// seen for that group in the batch.
func (operator *IncrementalGroupCountSumInt64) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if operator == nil {
		return nil, ErrIncrementalGroupCountSumInt64Nil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 1 {
		return operator.applySingle(updates[0])
	}

	pending := make(map[string]incrementalGroupCountSumPending, len(updates))
	order := make([]string, 0, len(updates))
	for _, update := range updates {
		if update.Diff == 0 {
			continue
		}
		key := operator.groupKey(update.Row)
		entry, exists := pending[key]
		if !exists {
			current := operator.states[key]
			entry = incrementalGroupCountSumPending{current: current, next: current}
			order = append(order, key)
		}

		rowValue, err := operator.value(update.Row)
		if err != nil {
			return nil, fmt.Errorf("group %q: differential value: %w", key, err)
		}
		delta, ok := multiplyDifferentialInt64(rowValue, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupBySumOverflow)
		}
		nextCount, ok := addDifferentialCounts(entry.next.count, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByCountOverflow)
		}
		if nextCount < 0 {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByNegativeCount)
		}
		nextSum, ok := addDifferentialCounts(entry.next.sum, delta)
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupBySumOverflow)
		}
		entry.next = incrementalGroupCountSumState{count: nextCount, sum: nextSum}
		entry.time = update.Time
		pending[key] = entry
	}
	if len(order) == 0 {
		return nil, nil
	}

	changes := make([]DifferentialRow, 0, len(order)*2)
	for _, key := range order {
		entry := pending[key]
		if entry.current == entry.next {
			continue
		}
		if entry.current.count > 0 {
			changes = append(changes, DifferentialRow{
				Key:  key,
				Time: entry.time,
				Diff: -1,
				Row:  Row{"count": entry.current.count, "sum": entry.current.sum},
			})
		}
		if entry.next.count > 0 {
			changes = append(changes, DifferentialRow{
				Key:  key,
				Time: entry.time,
				Diff: 1,
				Row:  Row{"count": entry.next.count, "sum": entry.next.sum},
			})
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

func (operator *IncrementalGroupCountSumInt64) applySingle(update DifferentialRow) ([]DifferentialRow, error) {
	if update.Diff == 0 {
		return nil, nil
	}
	key := operator.groupKey(update.Row)
	current := operator.states[key]
	rowValue, err := operator.value(update.Row)
	if err != nil {
		return nil, fmt.Errorf("group %q: differential value: %w", key, err)
	}
	delta, ok := multiplyDifferentialInt64(rowValue, update.Diff)
	if !ok {
		return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupBySumOverflow)
	}
	nextCount, ok := addDifferentialCounts(current.count, update.Diff)
	if !ok {
		return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByCountOverflow)
	}
	if nextCount < 0 {
		return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByNegativeCount)
	}
	nextSum, ok := addDifferentialCounts(current.sum, delta)
	if !ok {
		return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupBySumOverflow)
	}
	next := incrementalGroupCountSumState{count: nextCount, sum: nextSum}
	if current == next {
		return nil, nil
	}

	changes := make([]DifferentialRow, 0, 2)
	if current.count > 0 {
		changes = append(changes, DifferentialRow{
			Key:  key,
			Time: update.Time,
			Diff: -1,
			Row:  Row{"count": current.count, "sum": current.sum},
		})
	}
	if next.count > 0 {
		changes = append(changes, DifferentialRow{
			Key:  key,
			Time: update.Time,
			Diff: 1,
			Row:  Row{"count": next.count, "sum": next.sum},
		})
		operator.states[key] = next
	} else {
		delete(operator.states, key)
	}
	return changes, nil
}

// Snapshot returns one positive aggregate row per retained group in lexical
// group-key order. The returned row maps are independent of operator state.
func (operator *IncrementalGroupCountSumInt64) Snapshot() []DifferentialRow {
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
		rows = append(rows, DifferentialRow{
			Key:  key,
			Diff: 1,
			Row:  Row{"count": state.count, "sum": state.sum},
		})
	}
	return rows
}
