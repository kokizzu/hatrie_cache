package hatSql

import (
	"errors"
	"fmt"
	"sort"
)

var (
	ErrIncrementalGroupCountSumInt64Nil           = errors.New("incremental group count sum operator is nil")
	ErrIncrementalGroupCountSumInt64Negative      = errors.New("incremental group count sum became negative")
	ErrIncrementalGroupCountSumInt64CountOverflow = errors.New("incremental group count sum count overflowed")
	ErrIncrementalGroupCountSumInt64SumOverflow   = errors.New("incremental group count sum sum overflowed")
)

type incrementalGroupCountSumEntry struct {
	count int64
	sum   int64
	time  uint64
}

// IncrementalGroupCountSumInt64 maintains exact grouped COUNT and int64 SUM
// values across signed differential batches. It retains one compact entry per
// active group, allowing callers that need both aggregates (or an AVG derived
// from them) to share one key/value arrangement.
type IncrementalGroupCountSumInt64 struct {
	groupKey DifferentialGroupByKeyFunc
	value    DifferentialInt64ValueFunc
	groups   map[string]incrementalGroupCountSumEntry
}

// NewIncrementalGroupCountSumInt64 creates an empty stateful grouped
// COUNT+SUM operator.
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
		groups:   make(map[string]incrementalGroupCountSumEntry),
	}, nil
}

// Apply atomically applies signed weighted rows. Each changed group emits a
// retraction of its previous count and sum followed by an insertion of the new
// values. A negative multiplicity, count overflow, sum overflow, or callback
// error leaves the operator unchanged.
func (groupCountSum *IncrementalGroupCountSumInt64) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if groupCountSum == nil {
		return nil, ErrIncrementalGroupCountSumInt64Nil
	}
	if groupCountSum.groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if groupCountSum.value == nil {
		return nil, ErrDifferentialGroupByValueRequired
	}
	if groupCountSum.groups == nil {
		groupCountSum.groups = make(map[string]incrementalGroupCountSumEntry)
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 1 {
		return groupCountSum.applySingle(updates[0])
	}

	pending := make(map[string]incrementalGroupCountSumEntry, len(updates))
	changes := make([]DifferentialRow, 0, incrementalGroupCountSumOutputCapacity(len(updates)))
	for index, update := range updates {
		if update.Diff == 0 {
			continue
		}
		key := groupCountSum.groupKey(update.Row)
		current, ok := pending[key]
		if !ok {
			current = groupCountSum.groups[key]
		}
		next, err := groupCountSum.nextEntry(key, current, update, index)
		if err != nil {
			return nil, err
		}
		appendIncrementalGroupCountSumChanges(&changes, key, update.Time, current, next)
		pending[key] = next
	}

	for key, entry := range pending {
		if entry.count == 0 {
			delete(groupCountSum.groups, key)
			continue
		}
		groupCountSum.groups[key] = entry
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}

func (groupCountSum *IncrementalGroupCountSumInt64) applySingle(update DifferentialRow) ([]DifferentialRow, error) {
	if update.Diff == 0 {
		return nil, nil
	}
	key := groupCountSum.groupKey(update.Row)
	current := groupCountSum.groups[key]
	next, err := groupCountSum.nextEntry(key, current, update, 0)
	if err != nil {
		return nil, err
	}

	changes := make([]DifferentialRow, 0, 2)
	appendIncrementalGroupCountSumChanges(&changes, key, update.Time, current, next)
	if next.count == 0 {
		delete(groupCountSum.groups, key)
	} else {
		groupCountSum.groups[key] = next
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}

func (groupCountSum *IncrementalGroupCountSumInt64) nextEntry(key string, current incrementalGroupCountSumEntry, update DifferentialRow, index int) (incrementalGroupCountSumEntry, error) {
	nextCount, ok := addDifferentialCounts(current.count, update.Diff)
	if !ok {
		return incrementalGroupCountSumEntry{}, fmt.Errorf("group count sum update %d group %q: %w", index, key, ErrIncrementalGroupCountSumInt64CountOverflow)
	}
	if nextCount < 0 {
		return incrementalGroupCountSumEntry{}, fmt.Errorf("group count sum update %d group %q: %w", index, key, ErrIncrementalGroupCountSumInt64Negative)
	}

	rowValue, err := groupCountSum.value(update.Row)
	if err != nil {
		return incrementalGroupCountSumEntry{}, fmt.Errorf("group count sum update %d group %q: differential value: %w", index, key, err)
	}
	delta, ok := multiplyDifferentialInt64(rowValue, update.Diff)
	if !ok {
		return incrementalGroupCountSumEntry{}, fmt.Errorf("group count sum update %d group %q: %w", index, key, ErrIncrementalGroupCountSumInt64SumOverflow)
	}
	nextSum, ok := addDifferentialCounts(current.sum, delta)
	if !ok {
		return incrementalGroupCountSumEntry{}, fmt.Errorf("group count sum update %d group %q: %w", index, key, ErrIncrementalGroupCountSumInt64SumOverflow)
	}
	if nextCount == 0 {
		nextSum = 0
	}
	return incrementalGroupCountSumEntry{count: nextCount, sum: nextSum, time: update.Time}, nil
}

func appendIncrementalGroupCountSumChanges(changes *[]DifferentialRow, key string, time uint64, current, next incrementalGroupCountSumEntry) {
	if current.count > 0 {
		*changes = append(*changes, DifferentialRow{
			Key:  key,
			Time: time,
			Diff: -1,
			Row:  Row{"count": current.count, "sum": current.sum},
		})
	}
	if next.count > 0 {
		*changes = append(*changes, DifferentialRow{
			Key:  key,
			Time: time,
			Diff: 1,
			Row:  Row{"count": next.count, "sum": next.sum},
		})
	}
}

func incrementalGroupCountSumOutputCapacity(length int) int {
	if length > int(^uint(0)>>1)/2 {
		return length
	}
	return length * 2
}

// Snapshot returns one positive row for each active group, sorted by group
// key for deterministic replay. The returned rows are detached from state.
func (groupCountSum *IncrementalGroupCountSumInt64) Snapshot() []DifferentialRow {
	if groupCountSum == nil || len(groupCountSum.groups) == 0 {
		return nil
	}
	keys := make([]string, 0, len(groupCountSum.groups))
	for key := range groupCountSum.groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]DifferentialRow, 0, len(keys))
	for _, key := range keys {
		entry := groupCountSum.groups[key]
		result = append(result, DifferentialRow{
			Key:  key,
			Time: entry.time,
			Diff: 1,
			Row:  Row{"count": entry.count, "sum": entry.sum},
		})
	}
	return result
}

// AllRows is an alias for Snapshot for relation-style callers.
func (groupCountSum *IncrementalGroupCountSumInt64) AllRows() []DifferentialRow {
	return groupCountSum.Snapshot()
}
