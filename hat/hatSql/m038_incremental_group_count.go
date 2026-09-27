package hatSql

import (
	"errors"
	"sort"
)

var ErrIncrementalGroupCountNil = errors.New("hatSql: incremental group-count operator is nil")

// IncrementalGroupCount maintains the current COUNT for each group across
// signed differential updates. It is single-writer; callers sharing one
// instance between goroutines must provide synchronization.
type IncrementalGroupCount struct {
	groupKey DifferentialGroupByKeyFunc
	counts   map[string]int64
}

type incrementalGroupCountPending struct {
	current int64
	next    int64
	time    uint64
}

// NewIncrementalGroupCount creates an empty retained group-count operator.
func NewIncrementalGroupCount(groupKey DifferentialGroupByKeyFunc) (*IncrementalGroupCount, error) {
	if groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	return &IncrementalGroupCount{
		groupKey: groupKey,
		counts:   make(map[string]int64),
	}, nil
}

// Apply validates and applies one signed differential batch atomically. Each
// changed group emits a retraction of its previous aggregate row followed by
// an insertion of its new aggregate row. A group entering the result emits
// only an insertion; a group leaving it emits only a retraction. The timestamp
// on each emitted transition is the timestamp of the last update for that
// group in the batch.
func (operator *IncrementalGroupCount) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if operator == nil {
		return nil, ErrIncrementalGroupCountNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 1 {
		return operator.applySingle(updates[0])
	}

	pending := make(map[string]incrementalGroupCountPending, len(updates))
	order := make([]string, 0, len(updates))
	for _, update := range updates {
		if update.Diff == 0 {
			continue
		}
		key := operator.groupKey(update.Row)
		entry, exists := pending[key]
		if !exists {
			current := operator.counts[key]
			entry = incrementalGroupCountPending{current: current, next: current}
			order = append(order, key)
		}
		next, ok := addDifferentialCounts(entry.next, update.Diff)
		if !ok {
			return nil, ErrDifferentialGroupByCountOverflow
		}
		if next < 0 {
			return nil, ErrDifferentialGroupByNegativeCount
		}
		entry.next = next
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
		if entry.current > 0 {
			changes = append(changes, DifferentialRow{
				Key:  key,
				Time: entry.time,
				Diff: -1,
				Row:  Row{"count": entry.current},
			})
		}
		if entry.next > 0 {
			changes = append(changes, DifferentialRow{
				Key:  key,
				Time: entry.time,
				Diff: 1,
				Row:  Row{"count": entry.next},
			})
			operator.counts[key] = entry.next
		} else {
			delete(operator.counts, key)
		}
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}

func (operator *IncrementalGroupCount) applySingle(update DifferentialRow) ([]DifferentialRow, error) {
	if update.Diff == 0 {
		return nil, nil
	}
	key := operator.groupKey(update.Row)
	current := operator.counts[key]
	next, ok := addDifferentialCounts(current, update.Diff)
	if !ok {
		return nil, ErrDifferentialGroupByCountOverflow
	}
	if next < 0 {
		return nil, ErrDifferentialGroupByNegativeCount
	}
	if current == next {
		return nil, nil
	}

	changes := make([]DifferentialRow, 0, 2)
	if current > 0 {
		changes = append(changes, DifferentialRow{
			Key:  key,
			Time: update.Time,
			Diff: -1,
			Row:  Row{"count": current},
		})
	}
	if next > 0 {
		changes = append(changes, DifferentialRow{
			Key:  key,
			Time: update.Time,
			Diff: 1,
			Row:  Row{"count": next},
		})
		operator.counts[key] = next
	} else {
		delete(operator.counts, key)
	}
	return changes, nil
}

// Snapshot returns one positive row per retained group in lexical key order.
// The returned rows are independent maps and can be mutated by the caller.
func (operator *IncrementalGroupCount) Snapshot() []DifferentialRow {
	if operator == nil || len(operator.counts) == 0 {
		return nil
	}
	keys := make([]string, 0, len(operator.counts))
	for key := range operator.counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]DifferentialRow, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, DifferentialRow{
			Key:  key,
			Diff: 1,
			Row:  Row{"count": operator.counts[key]},
		})
	}
	return rows
}
