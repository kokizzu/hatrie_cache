package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
)

var (
	ErrIncrementalDistinctNil                  = errors.New("incremental distinct operator is nil")
	ErrIncrementalDistinctNegativeMultiplicity = errors.New("incremental distinct multiplicity became negative")
	ErrIncrementalDistinctOverflow             = errors.New("incremental distinct multiplicity overflowed")
	ErrIncrementalDistinctRowConflict          = errors.New("incremental distinct row conflicts with existing key")
)

// IncrementalDistinct maintains set membership across signed differential
// batches. Each key retains its full multiplicity, but the output emits only
// the transition when a key enters or leaves the distinct set.
type IncrementalDistinct struct {
	counts map[string]uint64
	rows   map[string]Row
	times  map[string]uint64
}

type incrementalDistinctPendingEntry struct {
	active        bool
	cloneOnCommit bool
	time          uint64
	row           Row
	count         uint64
}

const incrementalDistinctSmallBatchLimit = 8

// NewIncrementalDistinct creates an empty stateful distinct operator.
func NewIncrementalDistinct() *IncrementalDistinct {
	return &IncrementalDistinct{
		counts: make(map[string]uint64),
		rows:   make(map[string]Row),
		times:  make(map[string]uint64),
	}
}

// Apply validates and applies signed differential updates atomically. It
// emits +1 when a key first becomes present and -1 when its multiplicity
// reaches zero. Updates are evaluated in input order, so a batch can contain
// multiple transitions for one key.
func (distinct *IncrementalDistinct) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if distinct == nil {
		return nil, ErrIncrementalDistinctNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 1 {
		return distinct.applySingle(updates[0])
	}
	if len(updates) <= incrementalDistinctSmallBatchLimit {
		return distinct.applySmallBatch(updates)
	}
	return distinct.applyGenericBatch(updates)
}

func (distinct *IncrementalDistinct) applySmallBatch(updates []DifferentialRow) ([]DifferentialRow, error) {
	if len(updates) > incrementalDistinctSmallBatchLimit {
		return distinct.applyGenericBatch(updates)
	}
	var pending [incrementalDistinctSmallBatchLimit]incrementalDistinctPendingEntry
	var keys [incrementalDistinctSmallBatchLimit]string
	var changes [incrementalDistinctSmallBatchLimit]DifferentialRow
	pendingCount := 0
	changeCount := 0
	for index, update := range updates {
		if update.Key == "" {
			return nil, fmt.Errorf("incremental distinct update %d: differential row key is required", index)
		}
		if update.Diff == 0 {
			continue
		}

		pendingIndex := -1
		for candidate := 0; candidate < pendingCount; candidate++ {
			if keys[candidate] == update.Key {
				pendingIndex = candidate
				break
			}
		}
		if pendingIndex < 0 {
			pendingIndex = pendingCount
			keys[pendingIndex] = update.Key
			if count, exists := distinct.counts[update.Key]; exists {
				pending[pendingIndex] = incrementalDistinctPendingEntry{
					active: true,
					time:   distinct.times[update.Key],
					row:    distinct.rows[update.Key],
					count:  count,
				}
			}
			pendingCount++
		}
		entry := &pending[pendingIndex]
		if update.Diff > 0 {
			wasActive := entry.active
			if !wasActive {
				entry.active = true
				entry.cloneOnCommit = true
				entry.time = update.Time
				entry.row = update.Row
			} else if update.Row != nil && !reflect.DeepEqual(update.Row, entry.row) {
				return nil, fmt.Errorf("incremental distinct update %d key %q: %w", index, update.Key, ErrIncrementalDistinctRowConflict)
			}
			next, ok := incrementalDistinctAddMultiplicity(entry.count, update.Diff)
			if !ok {
				return nil, fmt.Errorf("incremental distinct update %d key %q: %w", index, update.Key, ErrIncrementalDistinctOverflow)
			}
			entry.count = next
			if !wasActive {
				if changeCount == len(changes) {
					return distinct.applyGenericBatch(updates)
				}
				changes[changeCount] = DifferentialRow{
					Key:  update.Key,
					Time: update.Time,
					Diff: 1,
					Row:  cloneDifferentialRow(entry.row),
				}
				changeCount++
			}
		} else {
			if !entry.active {
				return nil, fmt.Errorf("incremental distinct update %d key %q: %w", index, update.Key, ErrIncrementalDistinctNegativeMultiplicity)
			}
			decrement := incrementalDistinctMagnitude(update.Diff)
			if decrement > entry.count {
				return nil, fmt.Errorf("incremental distinct update %d key %q: %w", index, update.Key, ErrIncrementalDistinctNegativeMultiplicity)
			}
			entry.count -= decrement
			if entry.count == 0 {
				if changeCount == len(changes) {
					return distinct.applyGenericBatch(updates)
				}
				changes[changeCount] = DifferentialRow{
					Key:  update.Key,
					Time: update.Time,
					Diff: -1,
					Row:  cloneDifferentialRow(entry.row),
				}
				changeCount++
				entry.active = false
			}
		}
	}

	for index := 0; index < pendingCount; index++ {
		key := keys[index]
		entry := pending[index]
		if !entry.active {
			delete(distinct.counts, key)
			delete(distinct.rows, key)
			delete(distinct.times, key)
			continue
		}
		distinct.counts[key] = entry.count
		if entry.cloneOnCommit {
			distinct.rows[key] = cloneDifferentialRow(entry.row)
		} else {
			distinct.rows[key] = entry.row
		}
		distinct.times[key] = entry.time
	}
	if changeCount == 0 {
		return nil, nil
	}
	return changes[:changeCount], nil
}

func (distinct *IncrementalDistinct) applyGenericBatch(updates []DifferentialRow) ([]DifferentialRow, error) {

	pending := make(map[string]incrementalDistinctPendingEntry, len(updates))
	changes := make([]DifferentialRow, 0, len(updates))
	for index, update := range updates {
		if update.Key == "" {
			return nil, fmt.Errorf("incremental distinct update %d: differential row key is required", index)
		}
		if update.Diff == 0 {
			continue
		}

		entry, ok := pending[update.Key]
		if !ok {
			if count, exists := distinct.counts[update.Key]; exists {
				entry = incrementalDistinctPendingEntry{
					active: true,
					time:   distinct.times[update.Key],
					row:    distinct.rows[update.Key],
					count:  count,
				}
			}
		}

		if update.Diff > 0 {
			wasActive := entry.active
			if !wasActive {
				entry = incrementalDistinctPendingEntry{
					active: true,
					time:   update.Time,
					row:    cloneDifferentialRow(update.Row),
				}
			} else if update.Row != nil && !reflect.DeepEqual(update.Row, entry.row) {
				return nil, fmt.Errorf("incremental distinct update %d key %q: %w", index, update.Key, ErrIncrementalDistinctRowConflict)
			}
			next, ok := incrementalDistinctAddMultiplicity(entry.count, update.Diff)
			if !ok {
				return nil, fmt.Errorf("incremental distinct update %d key %q: %w", index, update.Key, ErrIncrementalDistinctOverflow)
			}
			entry.count = next
			if !wasActive {
				changes = append(changes, DifferentialRow{
					Key:  update.Key,
					Time: update.Time,
					Diff: 1,
					Row:  cloneDifferentialRow(entry.row),
				})
			}
		} else {
			if !entry.active {
				return nil, fmt.Errorf("incremental distinct update %d key %q: %w", index, update.Key, ErrIncrementalDistinctNegativeMultiplicity)
			}
			decrement := incrementalDistinctMagnitude(update.Diff)
			if decrement > entry.count {
				return nil, fmt.Errorf("incremental distinct update %d key %q: %w", index, update.Key, ErrIncrementalDistinctNegativeMultiplicity)
			}
			entry.count -= decrement
			if entry.count == 0 {
				changes = append(changes, DifferentialRow{
					Key:  update.Key,
					Time: update.Time,
					Diff: -1,
					Row:  cloneDifferentialRow(entry.row),
				})
				entry.active = false
			}
		}
		pending[update.Key] = entry
	}

	for key, entry := range pending {
		if !entry.active {
			delete(distinct.counts, key)
			delete(distinct.rows, key)
			delete(distinct.times, key)
			continue
		}
		distinct.counts[key] = entry.count
		distinct.rows[key] = entry.row
		distinct.times[key] = entry.time
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}

func (distinct *IncrementalDistinct) applySingle(update DifferentialRow) ([]DifferentialRow, error) {
	if update.Key == "" {
		return nil, errors.New("incremental distinct update: differential row key is required")
	}
	if update.Diff == 0 {
		return nil, nil
	}

	count, exists := distinct.counts[update.Key]
	if update.Diff > 0 {
		if exists {
			if update.Row != nil && !reflect.DeepEqual(update.Row, distinct.rows[update.Key]) {
				return nil, fmt.Errorf("incremental distinct key %q: %w", update.Key, ErrIncrementalDistinctRowConflict)
			}
			next, ok := incrementalDistinctAddMultiplicity(count, update.Diff)
			if !ok {
				return nil, fmt.Errorf("incremental distinct key %q: %w", update.Key, ErrIncrementalDistinctOverflow)
			}
			distinct.counts[update.Key] = next
			return nil, nil
		}
		next, ok := incrementalDistinctAddMultiplicity(0, update.Diff)
		if !ok {
			return nil, fmt.Errorf("incremental distinct key %q: %w", update.Key, ErrIncrementalDistinctOverflow)
		}
		row := cloneDifferentialRow(update.Row)
		distinct.counts[update.Key] = next
		distinct.rows[update.Key] = row
		distinct.times[update.Key] = update.Time
		return []DifferentialRow{{
			Key:  update.Key,
			Time: update.Time,
			Diff: 1,
			Row:  cloneDifferentialRow(row),
		}}, nil
	}

	if !exists {
		return nil, fmt.Errorf("incremental distinct key %q: %w", update.Key, ErrIncrementalDistinctNegativeMultiplicity)
	}
	decrement := incrementalDistinctMagnitude(update.Diff)
	if decrement > count {
		return nil, fmt.Errorf("incremental distinct key %q: %w", update.Key, ErrIncrementalDistinctNegativeMultiplicity)
	}
	next := count - decrement
	if next > 0 {
		distinct.counts[update.Key] = next
		return nil, nil
	}
	row := distinct.rows[update.Key]
	delete(distinct.counts, update.Key)
	delete(distinct.rows, update.Key)
	delete(distinct.times, update.Key)
	return []DifferentialRow{{
		Key:  update.Key,
		Time: update.Time,
		Diff: -1,
		Row:  cloneDifferentialRow(row),
	}}, nil
}

// Snapshot returns one positive row for each currently distinct key, sorted
// by key for deterministic replay. Diff is always one; internal multiplicity
// is intentionally not exposed by the distinct relation.
func (distinct *IncrementalDistinct) Snapshot() []DifferentialRow {
	if distinct == nil || len(distinct.counts) == 0 {
		return nil
	}
	keys := make([]string, 0, len(distinct.counts))
	for key := range distinct.counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]DifferentialRow, 0, len(keys))
	for _, key := range keys {
		result = append(result, DifferentialRow{
			Key:  key,
			Time: distinct.times[key],
			Diff: 1,
			Row:  cloneDifferentialRow(distinct.rows[key]),
		})
	}
	return result
}

// AllRows is an alias for Snapshot for callers that use a relation-style
// naming convention.
func (distinct *IncrementalDistinct) AllRows() []DifferentialRow {
	return distinct.Snapshot()
}

func incrementalDistinctAddMultiplicity(current uint64, diff int64) (uint64, bool) {
	increment := uint64(diff)
	if current > uint64(math.MaxInt64)-increment {
		return 0, false
	}
	return current + increment, true
}

func incrementalDistinctMagnitude(diff int64) uint64 {
	return uint64(-(diff + 1)) + 1
}
