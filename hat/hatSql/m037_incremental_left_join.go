package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

var (
	// ErrIncrementalLeftJoinNil reports use of a nil left-join operator.
	ErrIncrementalLeftJoinNil = errors.New("incremental left join operator is nil")
	// ErrIncrementalLeftJoinUnmatchedRequired reports a missing null-extension callback.
	ErrIncrementalLeftJoinUnmatchedRequired = errors.New("incremental left join unmatched callback is required")
)

// IncrementalLeftJoinUnmatchedFunc creates the null-extended output row for a
// left row that currently has no matching right multiplicity.
type IncrementalLeftJoinUnmatchedFunc func(left Row) (Row, error)

// IncrementalLeftJoinDefinition configures a signed, keyed left outer join.
// Merge creates matched rows; Unmatched creates the row emitted while a left
// row has no right match.
type IncrementalLeftJoinDefinition struct {
	LeftKey   IncrementalJoinKeyFunc
	RightKey  IncrementalJoinKeyFunc
	Merge     IncrementalJoinMergeFunc
	Unmatched IncrementalLeftJoinUnmatchedFunc
}

// IncrementalLeftJoin maintains a left outer join under signed differential
// updates. It reuses the exact inner-join index and adds only the unmatched
// transition bookkeeping required when a key's first or last right row
// appears.
type IncrementalLeftJoin struct {
	inner     *IncrementalJoin
	unmatched IncrementalLeftJoinUnmatchedFunc
}

// NewIncrementalLeftJoin creates an empty signed left outer join.
func NewIncrementalLeftJoin(definition IncrementalLeftJoinDefinition) (*IncrementalLeftJoin, error) {
	if definition.Unmatched == nil {
		return nil, ErrIncrementalLeftJoinUnmatchedRequired
	}
	inner, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  definition.LeftKey,
		RightKey: definition.RightKey,
		Merge:    definition.Merge,
	})
	if err != nil {
		return nil, err
	}
	return &IncrementalLeftJoin{inner: inner, unmatched: definition.Unmatched}, nil
}

// Apply validates and applies signed updates atomically. The returned rows are
// the matched-pair deltas plus any null-extension deltas caused by a left row
// crossing the zero-match boundary.
func (join *IncrementalLeftJoin) Apply(updates []IncrementalJoinUpdate) ([]DifferentialRow, error) {
	if join == nil {
		return nil, ErrIncrementalLeftJoinNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 1 {
		return join.applySingle(updates[0])
	}
	working := cloneIncrementalJoin(join.inner)
	output := make([]DifferentialRow, 0, len(updates))
	for index, update := range updates {
		if update.Side != IncrementalJoinLeft && update.Side != IncrementalJoinRight {
			return nil, fmt.Errorf("incremental left join update %d: %w", index, ErrIncrementalJoinSideInvalid)
		}
		if update.Row.Key == "" || strings.IndexByte(update.Row.Key, 0) >= 0 {
			if _, err := working.Apply([]IncrementalJoinUpdate{update}); err != nil {
				return nil, fmt.Errorf("incremental left join update %d: %w", index, err)
			}
			return nil, ErrIncrementalJoinKeyRequired
		}
		if update.Row.Diff == 0 {
			if _, err := working.Apply([]IncrementalJoinUpdate{update}); err != nil {
				return nil, fmt.Errorf("incremental left join update %d: %w", index, err)
			}
			continue
		}

		oldLeft, oldLeftOK := incrementalLeftJoinEntry(working.left, update.Row.Key, update.Side == IncrementalJoinLeft)
		joinKey, err := incrementalLeftJoinUpdateKey(working, update)
		if err != nil {
			if _, applyErr := working.Apply([]IncrementalJoinUpdate{update}); applyErr != nil {
				return nil, fmt.Errorf("incremental left join update %d: %w", index, applyErr)
			}
			return nil, fmt.Errorf("incremental left join update %d: %w", index, err)
		}
		oldRightCount, err := incrementalLeftJoinBucketCount(working.rightBuckets[joinKey])
		if err != nil {
			return nil, fmt.Errorf("incremental left join update %d right key %q: %w", index, joinKey, err)
		}

		matched, err := working.Apply([]IncrementalJoinUpdate{update})
		if err != nil {
			return nil, fmt.Errorf("incremental left join update %d: %w", index, err)
		}
		output = append(output, matched...)

		if update.Side == IncrementalJoinLeft {
			current, currentOK := working.left[update.Row.Key]
			newRightCount, countErr := incrementalLeftJoinBucketCount(working.rightBuckets[joinKey])
			if countErr != nil {
				return nil, fmt.Errorf("incremental left join update %d right key %q: %w", index, joinKey, countErr)
			}
			oldUnmatched := int64(0)
			if oldLeftOK && oldLeft.count > 0 && oldRightCount == 0 {
				oldUnmatched = oldLeft.count
			}
			newUnmatched := int64(0)
			if currentOK && current.count > 0 && newRightCount == 0 {
				newUnmatched = current.count
			}
			delta, ok := incrementalLeftJoinDelta(newUnmatched, oldUnmatched)
			if !ok || delta == 0 {
				if !ok {
					return nil, fmt.Errorf("incremental left join update %d key %q: %w", index, update.Row.Key, ErrIncrementalJoinOverflow)
				}
				continue
			}
			row := oldLeft.row
			time := oldLeft.time
			if currentOK {
				row = current.row
				time = current.time
			}
			output, err = join.appendUnmatched(output, update.Row.Key, time, delta, row)
			if err != nil {
				return nil, fmt.Errorf("incremental left join update %d unmatched row: %w", index, err)
			}
			continue
		}

		current, currentOK := working.right[update.Row.Key]
		newJoinKey := joinKey
		if currentOK {
			newJoinKey = current.joinKey
		}
		newRightCount, countErr := incrementalLeftJoinBucketCount(working.rightBuckets[newJoinKey])
		if countErr != nil {
			return nil, fmt.Errorf("incremental left join update %d right key %q: %w", index, newJoinKey, countErr)
		}
		if oldRightCount == 0 && newRightCount > 0 {
			output, err = join.appendBucketUnmatched(output, working.leftBuckets[newJoinKey], -1)
		} else if oldRightCount > 0 && newRightCount == 0 {
			output, err = join.appendBucketUnmatched(output, working.leftBuckets[joinKey], 1)
		}
		if err != nil {
			return nil, fmt.Errorf("incremental left join update %d unmatched rows: %w", index, err)
		}
	}
	join.inner = working
	return output, nil
}

func (join *IncrementalLeftJoin) applySingle(update IncrementalJoinUpdate) ([]DifferentialRow, error) {
	if update.Side != IncrementalJoinLeft && update.Side != IncrementalJoinRight {
		return nil, fmt.Errorf("incremental left join update: %w", ErrIncrementalJoinSideInvalid)
	}
	if update.Row.Key == "" || strings.IndexByte(update.Row.Key, 0) >= 0 {
		if _, err := join.inner.Apply([]IncrementalJoinUpdate{update}); err != nil {
			return nil, err
		}
		return nil, ErrIncrementalJoinKeyRequired
	}
	if update.Row.Diff == 0 {
		return join.inner.Apply([]IncrementalJoinUpdate{update})
	}

	transitions := make([]DifferentialRow, 0)
	if update.Side == IncrementalJoinLeft {
		entry, exists := join.inner.left[update.Row.Key]
		oldCount := int64(0)
		oldJoinKey := ""
		oldRow := Row(nil)
		oldTime := uint64(0)
		if exists {
			oldCount = entry.count
			oldJoinKey = entry.joinKey
			oldRow = entry.row
			oldTime = entry.time
		}
		joinKey, err := incrementalLeftJoinUpdateKey(join.inner, update)
		if err != nil {
			return join.inner.Apply([]IncrementalJoinUpdate{update})
		}
		if exists && joinKey != oldJoinKey {
			return nil, fmt.Errorf("incremental left join update key %q: %w", update.Row.Key, ErrIncrementalJoinRowConflict)
		}
		nextCount, ok := incrementalJoinAddCount(oldCount, update.Row.Diff)
		if !ok {
			return join.inner.Apply([]IncrementalJoinUpdate{update})
		}
		finalRow := oldRow
		finalTime := oldTime
		if oldCount == 0 {
			finalRow = cloneDifferentialRow(update.Row.Row)
			finalTime = update.Row.Time
		} else if update.Row.Row != nil {
			if !reflect.DeepEqual(update.Row.Row, oldRow) {
				return nil, fmt.Errorf("incremental left join update key %q: %w", update.Row.Key, ErrIncrementalJoinRowConflict)
			}
		}
		rightCount, err := incrementalLeftJoinBucketCount(join.inner.rightBuckets[joinKey])
		if err != nil {
			return nil, err
		}
		oldUnmatched := int64(0)
		if oldCount > 0 && rightCount == 0 {
			oldUnmatched = oldCount
		}
		newUnmatched := int64(0)
		if nextCount > 0 && rightCount == 0 {
			newUnmatched = nextCount
		}
		delta, ok := incrementalLeftJoinDelta(newUnmatched, oldUnmatched)
		if !ok {
			return join.inner.Apply([]IncrementalJoinUpdate{update})
		}
		if delta != 0 {
			row := finalRow
			time := finalTime
			if nextCount == 0 {
				row = oldRow
				time = oldTime
			}
			transitions, err = join.appendUnmatched(transitions, update.Row.Key, time, delta, row)
			if err != nil {
				return nil, err
			}
		}
	} else {
		joinKey, err := incrementalLeftJoinUpdateKey(join.inner, update)
		if err != nil {
			return join.inner.Apply([]IncrementalJoinUpdate{update})
		}
		entry := join.inner.right[update.Row.Key]
		oldCount := int64(0)
		if entry != nil {
			oldCount = entry.count
		}
		newCount, ok := incrementalJoinAddCount(oldCount, update.Row.Diff)
		if !ok {
			return join.inner.Apply([]IncrementalJoinUpdate{update})
		}
		if (oldCount == 0) == (newCount == 0) {
			matched, err := join.inner.Apply([]IncrementalJoinUpdate{update})
			return matched, err
		}
		direction := int64(1)
		if newCount > 0 {
			direction = -1
		}
		transitions, err = join.appendBucketUnmatched(transitions, join.inner.leftBuckets[joinKey], direction)
		if err != nil {
			return nil, err
		}
	}

	matched, err := join.inner.Apply([]IncrementalJoinUpdate{update})
	if err != nil {
		return nil, err
	}
	return append(matched, transitions...), nil
}

// Snapshot returns the complete current left-outer relation.
func (join *IncrementalLeftJoin) Snapshot() ([]DifferentialRow, error) {
	if join == nil {
		return nil, ErrIncrementalLeftJoinNil
	}
	result, err := join.inner.Snapshot()
	if err != nil {
		return nil, err
	}
	for _, key := range incrementalJoinSortedKeys(join.inner.left) {
		left := join.inner.left[key]
		count, err := incrementalLeftJoinBucketCount(join.inner.rightBuckets[left.joinKey])
		if err != nil {
			return nil, fmt.Errorf("left key %q: %w", key, err)
		}
		if count != 0 {
			continue
		}
		row, err := join.unmatched(cloneDifferentialRow(left.row))
		if err != nil {
			return nil, fmt.Errorf("left key %q unmatched row: %w", key, err)
		}
		result = append(result, DifferentialRow{Key: incrementalLeftJoinUnmatchedKey(key), Time: left.time, Diff: left.count, Row: cloneDifferentialRow(row)})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Key < result[right].Key })
	return result, nil
}

// AllRows is an alias for Snapshot.
func (join *IncrementalLeftJoin) AllRows() ([]DifferentialRow, error) {
	return join.Snapshot()
}

func (join *IncrementalLeftJoin) appendUnmatched(output []DifferentialRow, key string, time uint64, diff int64, left Row) ([]DifferentialRow, error) {
	if diff == 0 {
		return output, nil
	}
	row, err := join.unmatched(cloneDifferentialRow(left))
	if err != nil {
		return nil, err
	}
	return append(output, DifferentialRow{Key: incrementalLeftJoinUnmatchedKey(key), Time: time, Diff: diff, Row: cloneDifferentialRow(row)}), nil
}

func (join *IncrementalLeftJoin) appendBucketUnmatched(output []DifferentialRow, bucket map[string]*incrementalJoinEntry, direction int64) ([]DifferentialRow, error) {
	for _, key := range incrementalJoinSortedKeys(bucket) {
		entry := bucket[key]
		if entry.count <= 0 {
			continue
		}
		diff, ok := incrementalLeftJoinSignedCount(entry.count, direction)
		if !ok {
			return nil, ErrIncrementalJoinOverflow
		}
		var err error
		output, err = join.appendUnmatched(output, entry.key, entry.time, diff, entry.row)
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}

func incrementalLeftJoinEntry(entries map[string]*incrementalJoinEntry, key string, want bool) (incrementalJoinEntry, bool) {
	if !want {
		return incrementalJoinEntry{}, false
	}
	entry, ok := entries[key]
	if !ok {
		return incrementalJoinEntry{}, false
	}
	copy := *entry
	copy.row = cloneDifferentialRow(entry.row)
	return copy, true
}

func incrementalLeftJoinUpdateKey(join *IncrementalJoin, update IncrementalJoinUpdate) (string, error) {
	entries := join.left
	if update.Side == IncrementalJoinRight {
		entries = join.right
	}
	if entry, ok := entries[update.Row.Key]; ok {
		return entry.joinKey, nil
	}
	if update.Row.Row == nil {
		return "", ErrIncrementalJoinRowRequired
	}
	return join.keyForSide(update.Side, update.Row.Key, update.Row.Row)
}

func incrementalLeftJoinBucketCount(bucket map[string]*incrementalJoinEntry) (int64, error) {
	count := int64(0)
	for _, entry := range bucket {
		next, ok := incrementalJoinAddCount(count, entry.count)
		if !ok {
			return 0, ErrIncrementalJoinOverflow
		}
		count = next
	}
	return count, nil
}

func incrementalLeftJoinDelta(next, previous int64) (int64, bool) {
	if previous == 0 {
		return next, true
	}
	if next > previous {
		if next-previous < 0 {
			return 0, false
		}
		return next - previous, true
	}
	return -(previous - next), true
}

func incrementalLeftJoinSignedCount(count, direction int64) (int64, bool) {
	if direction > 0 {
		return count, true
	}
	if count == 0 {
		return 0, true
	}
	return -count, true
}

func incrementalLeftJoinUnmatchedKey(leftKey string) string {
	return "\x01" + leftKey
}

func cloneIncrementalJoin(source *IncrementalJoin) *IncrementalJoin {
	clone := &IncrementalJoin{
		leftKey:      source.leftKey,
		rightKey:     source.rightKey,
		merge:        source.merge,
		left:         make(map[string]*incrementalJoinEntry, len(source.left)),
		right:        make(map[string]*incrementalJoinEntry, len(source.right)),
		leftBuckets:  make(map[string]map[string]*incrementalJoinEntry, len(source.leftBuckets)),
		rightBuckets: make(map[string]map[string]*incrementalJoinEntry, len(source.rightBuckets)),
	}
	for key, entry := range source.left {
		copy := *entry
		copy.row = cloneDifferentialRow(entry.row)
		clone.left[key] = &copy
	}
	for key, entry := range source.right {
		copy := *entry
		copy.row = cloneDifferentialRow(entry.row)
		clone.right[key] = &copy
	}
	for joinKey, bucket := range source.leftBuckets {
		copied := make(map[string]*incrementalJoinEntry, len(bucket))
		for key := range bucket {
			copied[key] = clone.left[key]
		}
		clone.leftBuckets[joinKey] = copied
	}
	for joinKey, bucket := range source.rightBuckets {
		copied := make(map[string]*incrementalJoinEntry, len(bucket))
		for key := range bucket {
			copied[key] = clone.right[key]
		}
		clone.rightBuckets[joinKey] = copied
	}
	return clone
}
