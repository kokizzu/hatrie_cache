package hatSql

import (
	"errors"
	"fmt"
	"sort"
)

var (
	ErrIncrementalGroupSumInt64Nil           = errors.New("incremental group sum operator is nil")
	ErrIncrementalGroupSumInt64Negative      = errors.New("incremental group sum became negative")
	ErrIncrementalGroupSumInt64CountOverflow = errors.New("incremental group sum count overflowed")
	ErrIncrementalGroupSumInt64SumOverflow   = errors.New("incremental group sum sum overflowed")
)

type incrementalGroupSumEntry struct {
	count int64
	sum   int64
	time  uint64
}

// IncrementalGroupSumInt64 maintains an exact grouped int64 SUM across signed
// differential batches. It retains one count, sum, and timestamp per active
// group instead of retaining the input rows or rebuilding prior batches.
type IncrementalGroupSumInt64 struct {
	groupKey DifferentialGroupByKeyFunc
	value    DifferentialInt64ValueFunc
	groups   map[string]incrementalGroupSumEntry
}

// NewIncrementalGroupSumInt64 creates an empty stateful grouped SUM operator.
func NewIncrementalGroupSumInt64(groupKey DifferentialGroupByKeyFunc, value DifferentialInt64ValueFunc) (*IncrementalGroupSumInt64, error) {
	if groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if value == nil {
		return nil, ErrDifferentialGroupByValueRequired
	}
	return &IncrementalGroupSumInt64{
		groupKey: groupKey,
		value:    value,
		groups:   make(map[string]incrementalGroupSumEntry),
	}, nil
}

// Apply atomically applies signed weighted rows. Each changed group emits a
// retraction of its previous sum followed by an insertion of its new sum. A
// negative multiplicity, count overflow, sum overflow, or value callback error
// leaves the operator unchanged.
func (groupSum *IncrementalGroupSumInt64) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if groupSum == nil {
		return nil, ErrIncrementalGroupSumInt64Nil
	}
	if groupSum.groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if groupSum.value == nil {
		return nil, ErrDifferentialGroupByValueRequired
	}
	if groupSum.groups == nil {
		groupSum.groups = make(map[string]incrementalGroupSumEntry)
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 1 {
		return groupSum.applySingle(updates[0])
	}

	pending := make(map[string]incrementalGroupSumEntry, len(updates))
	changes := make([]DifferentialRow, 0, incrementalGroupSumOutputCapacity(len(updates)))
	for index, update := range updates {
		if update.Diff == 0 {
			continue
		}
		key := groupSum.groupKey(update.Row)
		current, ok := pending[key]
		if !ok {
			current = groupSum.groups[key]
		}
		next, err := groupSum.nextEntry(key, current, update, index)
		if err != nil {
			return nil, err
		}
		appendIncrementalGroupSumChanges(&changes, key, update.Time, current, next)
		pending[key] = next
	}

	for key, entry := range pending {
		if entry.count == 0 {
			delete(groupSum.groups, key)
			continue
		}
		groupSum.groups[key] = entry
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}

func (groupSum *IncrementalGroupSumInt64) applySingle(update DifferentialRow) ([]DifferentialRow, error) {
	if update.Diff == 0 {
		return nil, nil
	}
	key := groupSum.groupKey(update.Row)
	current := groupSum.groups[key]
	next, err := groupSum.nextEntry(key, current, update, 0)
	if err != nil {
		return nil, err
	}

	changes := make([]DifferentialRow, 0, 2)
	appendIncrementalGroupSumChanges(&changes, key, update.Time, current, next)
	if next.count == 0 {
		delete(groupSum.groups, key)
	} else {
		groupSum.groups[key] = next
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}

func (groupSum *IncrementalGroupSumInt64) nextEntry(key string, current incrementalGroupSumEntry, update DifferentialRow, index int) (incrementalGroupSumEntry, error) {
	nextCount, ok := addDifferentialCounts(current.count, update.Diff)
	if !ok {
		return incrementalGroupSumEntry{}, fmt.Errorf("group sum update %d group %q: %w", index, key, ErrIncrementalGroupSumInt64CountOverflow)
	}
	if nextCount < 0 {
		return incrementalGroupSumEntry{}, fmt.Errorf("group sum update %d group %q: %w", index, key, ErrIncrementalGroupSumInt64Negative)
	}

	rowValue, err := groupSum.value(update.Row)
	if err != nil {
		return incrementalGroupSumEntry{}, fmt.Errorf("group sum update %d group %q: differential value: %w", index, key, err)
	}
	delta, ok := multiplyDifferentialInt64(rowValue, update.Diff)
	if !ok {
		return incrementalGroupSumEntry{}, fmt.Errorf("group sum update %d group %q: %w", index, key, ErrIncrementalGroupSumInt64SumOverflow)
	}
	nextSum, ok := addDifferentialCounts(current.sum, delta)
	if !ok {
		return incrementalGroupSumEntry{}, fmt.Errorf("group sum update %d group %q: %w", index, key, ErrIncrementalGroupSumInt64SumOverflow)
	}
	if nextCount == 0 {
		nextSum = 0
	}
	return incrementalGroupSumEntry{count: nextCount, sum: nextSum, time: update.Time}, nil
}

func appendIncrementalGroupSumChanges(changes *[]DifferentialRow, key string, time uint64, current, next incrementalGroupSumEntry) {
	if current.count > 0 {
		*changes = append(*changes, DifferentialRow{
			Key:  key,
			Time: time,
			Diff: -1,
			Row:  Row{"sum": current.sum},
		})
	}
	if next.count > 0 {
		*changes = append(*changes, DifferentialRow{
			Key:  key,
			Time: time,
			Diff: 1,
			Row:  Row{"sum": next.sum},
		})
	}
}

func incrementalGroupSumOutputCapacity(length int) int {
	if length > int(^uint(0)>>1)/2 {
		return length
	}
	return length * 2
}

// Snapshot returns one positive row for each active group, sorted by group
// key for deterministic replay. The returned rows are detached from state.
func (groupSum *IncrementalGroupSumInt64) Snapshot() []DifferentialRow {
	if groupSum == nil || len(groupSum.groups) == 0 {
		return nil
	}
	keys := make([]string, 0, len(groupSum.groups))
	for key := range groupSum.groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]DifferentialRow, 0, len(keys))
	for _, key := range keys {
		entry := groupSum.groups[key]
		result = append(result, DifferentialRow{
			Key:  key,
			Time: entry.time,
			Diff: 1,
			Row:  Row{"sum": entry.sum},
		})
	}
	return result
}

// AllRows is an alias for Snapshot for relation-style callers.
func (groupSum *IncrementalGroupSumInt64) AllRows() []DifferentialRow {
	return groupSum.Snapshot()
}
