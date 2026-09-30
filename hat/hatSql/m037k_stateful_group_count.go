package hatSql

import (
	"errors"
	"fmt"
	"sort"
)

var (
	ErrIncrementalGroupCountNil      = errors.New("incremental group count operator is nil")
	ErrIncrementalGroupCountNegative = errors.New("incremental group count became negative")
	ErrIncrementalGroupCountOverflow = errors.New("incremental group count overflowed")
)

type incrementalGroupCountEntry struct {
	count int64
	time  uint64
}

// IncrementalGroupCount maintains an exact grouped COUNT across signed
// differential batches. It retains one compact count and timestamp per active
// group, rather than retaining the input rows or rebuilding prior batches.
type IncrementalGroupCount struct {
	groupKey DifferentialGroupByKeyFunc
	groups   map[string]incrementalGroupCountEntry
}

// NewIncrementalGroupCount creates an empty stateful grouped COUNT operator.
func NewIncrementalGroupCount(groupKey DifferentialGroupByKeyFunc) (*IncrementalGroupCount, error) {
	if groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	return &IncrementalGroupCount{
		groupKey: groupKey,
		groups:   make(map[string]incrementalGroupCountEntry),
	}, nil
}

// Apply atomically applies signed weighted rows. Each changed group emits a
// retraction of its previous count followed by an insertion of its new count.
// State is committed only after every update in the batch validates, so a
// negative count or overflow cannot leave a partial update behind.
func (groupCount *IncrementalGroupCount) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if groupCount == nil {
		return nil, ErrIncrementalGroupCountNil
	}
	if groupCount.groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if groupCount.groups == nil {
		groupCount.groups = make(map[string]incrementalGroupCountEntry)
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 1 {
		return groupCount.applySingle(updates[0])
	}

	pending := make(map[string]incrementalGroupCountEntry, len(updates))
	changes := make([]DifferentialRow, 0, incrementalGroupCountOutputCapacity(len(updates)))
	for index, update := range updates {
		if update.Diff == 0 {
			continue
		}
		key := groupCount.groupKey(update.Row)
		current, ok := pending[key]
		if !ok {
			current = groupCount.groups[key]
		}
		next, ok := addDifferentialCounts(current.count, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group count update %d group %q: %w", index, key, ErrIncrementalGroupCountOverflow)
		}
		if next < 0 {
			return nil, fmt.Errorf("group count update %d group %q: %w", index, key, ErrIncrementalGroupCountNegative)
		}
		appendIncrementalGroupCountChanges(&changes, key, update.Time, current.count, next)
		pending[key] = incrementalGroupCountEntry{count: next, time: update.Time}
	}

	for key, entry := range pending {
		if entry.count == 0 {
			delete(groupCount.groups, key)
			continue
		}
		groupCount.groups[key] = entry
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}

func (groupCount *IncrementalGroupCount) applySingle(update DifferentialRow) ([]DifferentialRow, error) {
	if update.Diff == 0 {
		return nil, nil
	}
	key := groupCount.groupKey(update.Row)
	current := groupCount.groups[key]
	next, ok := addDifferentialCounts(current.count, update.Diff)
	if !ok {
		return nil, fmt.Errorf("group %q: %w", key, ErrIncrementalGroupCountOverflow)
	}
	if next < 0 {
		return nil, fmt.Errorf("group %q: %w", key, ErrIncrementalGroupCountNegative)
	}

	changes := make([]DifferentialRow, 0, 2)
	appendIncrementalGroupCountChanges(&changes, key, update.Time, current.count, next)
	if next == 0 {
		delete(groupCount.groups, key)
	} else {
		groupCount.groups[key] = incrementalGroupCountEntry{count: next, time: update.Time}
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}

func appendIncrementalGroupCountChanges(changes *[]DifferentialRow, key string, time uint64, current, next int64) {
	if current > 0 {
		*changes = append(*changes, DifferentialRow{
			Key:  key,
			Time: time,
			Diff: -1,
			Row:  Row{"count": current},
		})
	}
	if next > 0 {
		*changes = append(*changes, DifferentialRow{
			Key:  key,
			Time: time,
			Diff: 1,
			Row:  Row{"count": next},
		})
	}
}

func incrementalGroupCountOutputCapacity(length int) int {
	if length > int(^uint(0)>>1)/2 {
		return length
	}
	return length * 2
}

// Snapshot returns one positive row for each active group, sorted by group
// key for deterministic replay. The returned rows are detached from state.
func (groupCount *IncrementalGroupCount) Snapshot() []DifferentialRow {
	if groupCount == nil || len(groupCount.groups) == 0 {
		return nil
	}
	keys := make([]string, 0, len(groupCount.groups))
	for key := range groupCount.groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]DifferentialRow, 0, len(keys))
	for _, key := range keys {
		entry := groupCount.groups[key]
		result = append(result, DifferentialRow{
			Key:  key,
			Time: entry.time,
			Diff: 1,
			Row:  Row{"count": entry.count},
		})
	}
	return result
}

// AllRows is an alias for Snapshot for relation-style callers.
func (groupCount *IncrementalGroupCount) AllRows() []DifferentialRow {
	return groupCount.Snapshot()
}
