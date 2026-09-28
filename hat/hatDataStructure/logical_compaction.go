package hatDataStructure

import (
	"errors"
	"sort"
)

var (
	ErrLogicalCompactionNil         = errors.New("hatriecache: logical compaction is nil")
	ErrLogicalCompactionRegression  = errors.New("hatriecache: logical compaction frontier moved backwards")
	ErrLogicalCompactionBeforeSince = errors.New("hatriecache: logical compaction update is before since frontier")
	ErrLogicalCompactionOverflow    = errors.New("hatriecache: logical compaction diff overflows int64")
)

type logicalCompactionKey[T comparable] struct {
	data T
	time uint64
}

// LogicalCompaction retains differential updates and folds all records through
// a monotone frontier into one record per data value at that frontier. It is
// intentionally caller-synchronized, like DifferentialMultiset.
type LogicalCompaction[T comparable] struct {
	entries map[logicalCompactionKey[T]]int64
	since   uint64
}

// NewLogicalCompaction creates an empty logical-compaction state.
func NewLogicalCompaction[T comparable]() *LogicalCompaction[T] {
	return &LogicalCompaction[T]{entries: make(map[logicalCompactionKey[T]]int64)}
}

// Add applies one differential update. Updates older than the current since
// frontier are rejected because that history has already been compacted.
func (compaction *LogicalCompaction[T]) Add(data T, timestamp uint64, diff int64) error {
	if compaction == nil {
		return ErrLogicalCompactionNil
	}
	if timestamp < compaction.since {
		return ErrLogicalCompactionBeforeSince
	}
	if diff == 0 {
		return nil
	}
	if compaction.entries == nil {
		compaction.entries = make(map[logicalCompactionKey[T]]int64)
	}
	key := logicalCompactionKey[T]{data: data, time: timestamp}
	current := compaction.entries[key]
	next, ok := addLogicalCompactionDiff(current, diff)
	if !ok {
		return ErrLogicalCompactionOverflow
	}
	if next == 0 {
		delete(compaction.entries, key)
		return nil
	}
	compaction.entries[key] = next
	return nil
}

// Since returns the current logical lower frontier. Records strictly before
// it have already been folded into the frontier or canceled.
func (compaction *LogicalCompaction[T]) Since() uint64 {
	if compaction == nil {
		return 0
	}
	return compaction.since
}

// Len returns the number of retained nonzero records.
func (compaction *LogicalCompaction[T]) Len() int {
	if compaction == nil {
		return 0
	}
	return len(compaction.entries)
}

// CompactThrough advances the since frontier and folds every record at or
// before frontier into a record at frontier. It validates all sums before
// mutating state, so overflow leaves the original state unchanged. The return
// value is the number of physical records consumed by the fold.
func (compaction *LogicalCompaction[T]) CompactThrough(frontier uint64) (int, error) {
	if compaction == nil {
		return 0, ErrLogicalCompactionNil
	}
	if frontier < compaction.since {
		return 0, ErrLogicalCompactionRegression
	}
	if frontier == compaction.since {
		return 0, nil
	}

	folded := make(map[T]int64)
	removed := 0
	for key, diff := range compaction.entries {
		if key.time > frontier {
			continue
		}
		removed++
		current := folded[key.data]
		next, ok := addLogicalCompactionDiff(current, diff)
		if !ok {
			return 0, ErrLogicalCompactionOverflow
		}
		folded[key.data] = next
	}

	for key := range compaction.entries {
		if key.time <= frontier {
			delete(compaction.entries, key)
		}
	}
	for data, diff := range folded {
		if diff != 0 {
			compaction.entries[logicalCompactionKey[T]{data: data, time: frontier}] = diff
		}
	}
	compaction.since = frontier
	return removed, nil
}

// ForEach visits each retained nonzero record. Map iteration order is
// unspecified; use Records when a time-ordered copy is needed.
func (compaction *LogicalCompaction[T]) ForEach(visit func(DifferentialRecord[T])) {
	if compaction == nil || visit == nil {
		return
	}
	for key, diff := range compaction.entries {
		visit(DifferentialRecord[T]{Data: key.data, Time: key.time, Diff: diff})
	}
}

// Records returns an owned time-ordered copy of retained records. Records at
// the same timestamp have unspecified relative order because T is generic.
func (compaction *LogicalCompaction[T]) Records() []DifferentialRecord[T] {
	if compaction == nil || len(compaction.entries) == 0 {
		return nil
	}
	records := make([]DifferentialRecord[T], 0, len(compaction.entries))
	compaction.ForEach(func(record DifferentialRecord[T]) { records = append(records, record) })
	sort.SliceStable(records, func(left, right int) bool {
		return records[left].Time < records[right].Time
	})
	return records
}

func addLogicalCompactionDiff(current, delta int64) (int64, bool) {
	if delta > 0 && current > int64(^uint64(0)>>1)-delta {
		return 0, false
	}
	if delta < 0 && current < -int64(^uint64(0)>>1)-1-delta {
		return 0, false
	}
	return current + delta, true
}
