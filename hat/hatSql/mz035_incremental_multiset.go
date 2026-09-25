package hatSql

import (
	"errors"
	"math"
	"reflect"
	"sort"
)

var (
	// ErrIncrementalMultisetNil reports a method call on a nil operator.
	ErrIncrementalMultisetNil = errors.New("incremental multiset operator is nil")
	// ErrIncrementalMultisetInvalidKey reports an update without a stable key.
	ErrIncrementalMultisetInvalidKey = errors.New("incremental multiset key is required")
	// ErrIncrementalMultisetNegativeMultiplicity reports a retraction larger
	// than the currently retained multiplicity.
	ErrIncrementalMultisetNegativeMultiplicity = errors.New("incremental multiset multiplicity became negative")
	// ErrIncrementalMultisetOverflow reports a multiplicity above MaxInt64.
	ErrIncrementalMultisetOverflow = errors.New("incremental multiset multiplicity overflowed")
	// ErrIncrementalMultisetRowConflict reports one key being used for different
	// rows while it is still present.
	ErrIncrementalMultisetRowConflict = errors.New("incremental multiset row conflicts with existing key")
)

// IncrementalMultiset maintains exact positive multiplicities across signed
// differential batches. Unlike IncrementalDistinct, it exposes the retained
// multiplicity in both Apply deltas and Snapshot rows.
//
// The operator is single-writer. Callers sharing one instance between
// goroutines must provide synchronization.
type IncrementalMultiset struct {
	counts map[string]uint64
	rows   map[string]Row
	times  map[string]uint64
}

type incrementalMultisetPendingEntry struct {
	count uint64
	row   Row
	time  uint64
	delta int64
}

// NewIncrementalMultiset creates an empty exact multiset operator.
func NewIncrementalMultiset() *IncrementalMultiset {
	return &IncrementalMultiset{
		counts: make(map[string]uint64),
		rows:   make(map[string]Row),
		times:  make(map[string]uint64),
	}
}

// Apply validates and applies one signed differential batch atomically. The
// returned rows contain one net delta per changed key; zero-net keys are not
// emitted. Positive and negative multiplicities are preserved exactly.
func (multiset *IncrementalMultiset) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if multiset == nil {
		return nil, ErrIncrementalMultisetNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 1 {
		return multiset.applySingle(updates[0])
	}

	pending := make(map[string]incrementalMultisetPendingEntry, len(updates))
	for _, update := range updates {
		if update.Key == "" {
			return nil, ErrIncrementalMultisetInvalidKey
		}
		if update.Diff == 0 {
			continue
		}

		entry, exists := pending[update.Key]
		if !exists {
			entry = incrementalMultisetPendingEntry{
				count: multiset.counts[update.Key],
				row:   cloneDifferentialRow(multiset.rows[update.Key]),
				time:  multiset.times[update.Key],
			}
		}
		if err := validateIncrementalMultisetRow(entry.count, entry.row, update.Row); err != nil {
			return nil, err
		}

		if update.Diff > 0 {
			if entry.count == 0 && update.Row != nil {
				entry.row = cloneDifferentialRow(update.Row)
			}
			if entry.count == 0 || entry.time == 0 {
				entry.time = update.Time
			}
			count, ok := incrementalMultisetAdd(entry.count, uint64(update.Diff))
			if !ok {
				return nil, ErrIncrementalMultisetOverflow
			}
			entry.count = count
		} else {
			magnitude := incrementalMultisetMagnitude(update.Diff)
			if magnitude > entry.count {
				return nil, ErrIncrementalMultisetNegativeMultiplicity
			}
			entry.count -= magnitude
		}
		if err := incrementalMultisetAddDelta(&entry.delta, update.Diff); err != nil {
			return nil, err
		}
		if update.Time != 0 {
			entry.time = update.Time
		}
		pending[update.Key] = entry
	}

	if len(pending) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(pending))
	for key := range pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	changes := make([]DifferentialRow, 0, len(keys))
	for _, key := range keys {
		entry := pending[key]
		if entry.count == 0 {
			delete(multiset.counts, key)
			delete(multiset.rows, key)
			delete(multiset.times, key)
		} else {
			multiset.counts[key] = entry.count
			multiset.rows[key] = cloneDifferentialRow(entry.row)
			multiset.times[key] = entry.time
		}
		if entry.delta != 0 {
			changes = append(changes, DifferentialRow{
				Key:  key,
				Time: entry.time,
				Diff: entry.delta,
				Row:  cloneDifferentialRow(entry.row),
			})
		}
	}
	return changes, nil
}

func (multiset *IncrementalMultiset) applySingle(update DifferentialRow) ([]DifferentialRow, error) {
	if update.Key == "" {
		return nil, ErrIncrementalMultisetInvalidKey
	}
	if update.Diff == 0 {
		return nil, nil
	}
	count := multiset.counts[update.Key]
	row := multiset.rows[update.Key]
	if err := validateIncrementalMultisetRow(count, row, update.Row); err != nil {
		return nil, err
	}
	if update.Diff > 0 {
		if count == 0 && update.Row != nil {
			row = cloneDifferentialRow(update.Row)
		}
		var ok bool
		count, ok = incrementalMultisetAdd(count, uint64(update.Diff))
		if !ok {
			return nil, ErrIncrementalMultisetOverflow
		}
		multiset.counts[update.Key] = count
		multiset.rows[update.Key] = cloneDifferentialRow(row)
		multiset.times[update.Key] = update.Time
	} else {
		magnitude := incrementalMultisetMagnitude(update.Diff)
		if magnitude > count {
			return nil, ErrIncrementalMultisetNegativeMultiplicity
		}
		count -= magnitude
		if count == 0 {
			delete(multiset.counts, update.Key)
			delete(multiset.rows, update.Key)
			delete(multiset.times, update.Key)
		} else {
			multiset.counts[update.Key] = count
		}
	}
	return []DifferentialRow{{
		Key:  update.Key,
		Time: update.Time,
		Diff: update.Diff,
		Row:  cloneDifferentialRow(row),
	}}, nil
}

// Count returns the exact current multiplicity for key.
func (multiset *IncrementalMultiset) Count(key string) (int64, bool) {
	if multiset == nil {
		return 0, false
	}
	count, ok := multiset.counts[key]
	if !ok {
		return 0, false
	}
	return int64(count), true
}

// Snapshot returns one positive row per key, sorted by key. Diff is the full
// current multiplicity, making the result suitable for deterministic replay.
func (multiset *IncrementalMultiset) Snapshot() []DifferentialRow {
	if multiset == nil || len(multiset.counts) == 0 {
		return nil
	}
	keys := make([]string, 0, len(multiset.counts))
	for key := range multiset.counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]DifferentialRow, 0, len(keys))
	for _, key := range keys {
		result = append(result, DifferentialRow{
			Key:  key,
			Time: multiset.times[key],
			Diff: int64(multiset.counts[key]),
			Row:  cloneDifferentialRow(multiset.rows[key]),
		})
	}
	return result
}

// AllRows is an alias for Snapshot for relation-style callers.
func (multiset *IncrementalMultiset) AllRows() []DifferentialRow {
	return multiset.Snapshot()
}

func validateIncrementalMultisetRow(count uint64, current, incoming Row) error {
	if count == 0 || incoming == nil || current == nil || reflect.DeepEqual(current, incoming) {
		return nil
	}
	return ErrIncrementalMultisetRowConflict
}

func incrementalMultisetAdd(current, increment uint64) (uint64, bool) {
	if current > math.MaxInt64-increment {
		return 0, false
	}
	return current + increment, true
}

func incrementalMultisetMagnitude(diff int64) uint64 {
	if diff >= 0 {
		return uint64(diff)
	}
	return uint64(-(diff + 1)) + 1
}

func incrementalMultisetAddDelta(delta *int64, update int64) error {
	if update > 0 && *delta > math.MaxInt64-update {
		return ErrIncrementalMultisetOverflow
	}
	if update < 0 && *delta < math.MinInt64-update {
		return ErrIncrementalMultisetOverflow
	}
	*delta += update
	return nil
}
