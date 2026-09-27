package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

var (
	// ErrIncrementalFullJoinNil reports use of a nil full-join operator.
	ErrIncrementalFullJoinNil = errors.New("incremental full join operator is nil")
	// ErrIncrementalFullJoinLeftUnmatchedRequired reports a missing left-side null extension.
	ErrIncrementalFullJoinLeftUnmatchedRequired = errors.New("incremental full join left unmatched callback is required")
	// ErrIncrementalFullJoinRightUnmatchedRequired reports a missing right-side null extension.
	ErrIncrementalFullJoinRightUnmatchedRequired = errors.New("incremental full join right unmatched callback is required")
)

// IncrementalFullJoinUnmatchedFunc creates the null-extended output row for
// one input row that currently has no match on the other side.
type IncrementalFullJoinUnmatchedFunc func(Row) (Row, error)

// IncrementalFullJoinDefinition configures a signed, keyed full outer join.
// Merge creates matched rows; each unmatched callback creates the
// side-specific null extension.
type IncrementalFullJoinDefinition struct {
	LeftKey        IncrementalJoinKeyFunc
	RightKey       IncrementalJoinKeyFunc
	Merge          IncrementalJoinMergeFunc
	LeftUnmatched  IncrementalFullJoinUnmatchedFunc
	RightUnmatched IncrementalFullJoinUnmatchedFunc
}

// IncrementalFullJoin maintains a full outer join under signed differential
// updates. Both sides share one indexed inner-join state, so a key boundary
// update only changes the affected null extensions.
type IncrementalFullJoin struct {
	inner          *IncrementalJoin
	leftUnmatched  IncrementalFullJoinUnmatchedFunc
	rightUnmatched IncrementalFullJoinUnmatchedFunc
}

// NewIncrementalFullJoin creates an empty signed full outer join.
func NewIncrementalFullJoin(definition IncrementalFullJoinDefinition) (*IncrementalFullJoin, error) {
	if definition.LeftUnmatched == nil {
		return nil, ErrIncrementalFullJoinLeftUnmatchedRequired
	}
	if definition.RightUnmatched == nil {
		return nil, ErrIncrementalFullJoinRightUnmatchedRequired
	}
	inner, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  definition.LeftKey,
		RightKey: definition.RightKey,
		Merge:    definition.Merge,
	})
	if err != nil {
		return nil, err
	}
	return &IncrementalFullJoin{
		inner:          inner,
		leftUnmatched:  definition.LeftUnmatched,
		rightUnmatched: definition.RightUnmatched,
	}, nil
}

// Apply validates and applies signed updates atomically. The result contains
// matched-pair deltas and null-extension deltas for either side crossing its
// zero-match boundary.
func (join *IncrementalFullJoin) Apply(updates []IncrementalJoinUpdate) ([]DifferentialRow, error) {
	if join == nil {
		return nil, ErrIncrementalFullJoinNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 1 {
		return join.applySingle(updates[0])
	}

	working := &IncrementalFullJoin{
		inner:          cloneIncrementalJoin(join.inner),
		leftUnmatched:  join.leftUnmatched,
		rightUnmatched: join.rightUnmatched,
	}
	output := make([]DifferentialRow, 0, len(updates))
	for index, update := range updates {
		rows, err := working.applySingle(update)
		if err != nil {
			return nil, fmt.Errorf("incremental full join update %d: %w", index, err)
		}
		output = append(output, rows...)
	}
	join.inner = working.inner
	return output, nil
}

func (join *IncrementalFullJoin) applySingle(update IncrementalJoinUpdate) ([]DifferentialRow, error) {
	if update.Side != IncrementalJoinLeft && update.Side != IncrementalJoinRight {
		return nil, fmt.Errorf("incremental full join update: %w", ErrIncrementalJoinSideInvalid)
	}
	if update.Row.Key == "" || strings.IndexByte(update.Row.Key, 0) >= 0 {
		return join.inner.Apply([]IncrementalJoinUpdate{update})
	}
	if update.Row.Diff == 0 {
		return join.inner.Apply([]IncrementalJoinUpdate{update})
	}

	joinKey, err := incrementalFullJoinUpdateKey(join.inner, update)
	if err != nil {
		return join.inner.Apply([]IncrementalJoinUpdate{update})
	}
	transitions := make([]DifferentialRow, 0, 1)
	if update.Side == IncrementalJoinLeft {
		entry := join.inner.left[update.Row.Key]
		oldCount, oldRow, oldTime := incrementalFullJoinEntryState(entry)
		if err := incrementalFullJoinValidateReplacement(join.inner, update, entry); err != nil {
			return nil, err
		}
		nextCount, ok := incrementalJoinAddCount(oldCount, update.Row.Diff)
		if !ok {
			return join.inner.Apply([]IncrementalJoinUpdate{update})
		}
		finalRow, finalTime := incrementalFullJoinNextState(update, oldCount, oldRow, oldTime)
		otherCount, err := incrementalLeftJoinBucketCount(join.inner.rightBuckets[joinKey])
		if err != nil {
			return nil, err
		}
		if otherCount == 0 {
			delta, ok := incrementalLeftJoinDelta(nextCount, oldCount)
			if !ok {
				return join.inner.Apply([]IncrementalJoinUpdate{update})
			}
			row, time := finalRow, finalTime
			if nextCount == 0 {
				row, time = oldRow, oldTime
			}
			transitions, err = join.appendLeftUnmatched(transitions, update.Row.Key, time, delta, row)
			if err != nil {
				return nil, err
			}
		}
		if oldCount == 0 && nextCount > 0 {
			transitions, err = join.appendRightBucketUnmatched(transitions, join.inner.rightBuckets[joinKey], -1)
		} else if oldCount > 0 && nextCount == 0 {
			transitions, err = join.appendRightBucketUnmatched(transitions, join.inner.rightBuckets[joinKey], 1)
		}
		if err != nil {
			return nil, err
		}
	} else {
		entry := join.inner.right[update.Row.Key]
		oldCount, oldRow, oldTime := incrementalFullJoinEntryState(entry)
		if err := incrementalFullJoinValidateReplacement(join.inner, update, entry); err != nil {
			return nil, err
		}
		nextCount, ok := incrementalJoinAddCount(oldCount, update.Row.Diff)
		if !ok {
			return join.inner.Apply([]IncrementalJoinUpdate{update})
		}
		finalRow, finalTime := incrementalFullJoinNextState(update, oldCount, oldRow, oldTime)
		otherCount, err := incrementalLeftJoinBucketCount(join.inner.leftBuckets[joinKey])
		if err != nil {
			return nil, err
		}
		if otherCount == 0 {
			delta, ok := incrementalLeftJoinDelta(nextCount, oldCount)
			if !ok {
				return join.inner.Apply([]IncrementalJoinUpdate{update})
			}
			row, time := finalRow, finalTime
			if nextCount == 0 {
				row, time = oldRow, oldTime
			}
			transitions, err = join.appendRightUnmatched(transitions, update.Row.Key, time, delta, row)
			if err != nil {
				return nil, err
			}
		}
		if oldCount == 0 && nextCount > 0 {
			transitions, err = join.appendLeftBucketUnmatched(transitions, join.inner.leftBuckets[joinKey], -1)
		} else if oldCount > 0 && nextCount == 0 {
			transitions, err = join.appendLeftBucketUnmatched(transitions, join.inner.leftBuckets[joinKey], 1)
		}
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

// Snapshot returns the complete current full-outer relation.
func (join *IncrementalFullJoin) Snapshot() ([]DifferentialRow, error) {
	if join == nil {
		return nil, ErrIncrementalFullJoinNil
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
		row, err := join.leftUnmatched(cloneDifferentialRow(left.row))
		if err != nil {
			return nil, fmt.Errorf("left key %q unmatched row: %w", key, err)
		}
		result = append(result, DifferentialRow{
			Key:  incrementalFullJoinLeftUnmatchedKey(key),
			Time: left.time,
			Diff: left.count,
			Row:  cloneDifferentialRow(row),
		})
	}
	for _, key := range incrementalJoinSortedKeys(join.inner.right) {
		right := join.inner.right[key]
		count, err := incrementalLeftJoinBucketCount(join.inner.leftBuckets[right.joinKey])
		if err != nil {
			return nil, fmt.Errorf("right key %q: %w", key, err)
		}
		if count != 0 {
			continue
		}
		row, err := join.rightUnmatched(cloneDifferentialRow(right.row))
		if err != nil {
			return nil, fmt.Errorf("right key %q unmatched row: %w", key, err)
		}
		result = append(result, DifferentialRow{
			Key:  incrementalFullJoinRightUnmatchedKey(key),
			Time: right.time,
			Diff: right.count,
			Row:  cloneDifferentialRow(row),
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Key < result[right].Key })
	return result, nil
}

// AllRows is an alias for Snapshot.
func (join *IncrementalFullJoin) AllRows() ([]DifferentialRow, error) {
	return join.Snapshot()
}

func (join *IncrementalFullJoin) appendLeftUnmatched(output []DifferentialRow, key string, time uint64, diff int64, left Row) ([]DifferentialRow, error) {
	if diff == 0 {
		return output, nil
	}
	row, err := join.leftUnmatched(cloneDifferentialRow(left))
	if err != nil {
		return nil, err
	}
	return append(output, DifferentialRow{
		Key:  incrementalFullJoinLeftUnmatchedKey(key),
		Time: time,
		Diff: diff,
		Row:  cloneDifferentialRow(row),
	}), nil
}

func (join *IncrementalFullJoin) appendRightUnmatched(output []DifferentialRow, key string, time uint64, diff int64, right Row) ([]DifferentialRow, error) {
	if diff == 0 {
		return output, nil
	}
	row, err := join.rightUnmatched(cloneDifferentialRow(right))
	if err != nil {
		return nil, err
	}
	return append(output, DifferentialRow{
		Key:  incrementalFullJoinRightUnmatchedKey(key),
		Time: time,
		Diff: diff,
		Row:  cloneDifferentialRow(row),
	}), nil
}

func (join *IncrementalFullJoin) appendLeftBucketUnmatched(output []DifferentialRow, bucket map[string]*incrementalJoinEntry, direction int64) ([]DifferentialRow, error) {
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
		output, err = join.appendLeftUnmatched(output, entry.key, entry.time, diff, entry.row)
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}

func (join *IncrementalFullJoin) appendRightBucketUnmatched(output []DifferentialRow, bucket map[string]*incrementalJoinEntry, direction int64) ([]DifferentialRow, error) {
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
		output, err = join.appendRightUnmatched(output, entry.key, entry.time, diff, entry.row)
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}

func incrementalFullJoinUpdateKey(join *IncrementalJoin, update IncrementalJoinUpdate) (string, error) {
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

func incrementalFullJoinValidateReplacement(join *IncrementalJoin, update IncrementalJoinUpdate, entry *incrementalJoinEntry) error {
	if entry == nil || update.Row.Diff <= 0 || update.Row.Row == nil {
		return nil
	}
	joinKey, err := join.keyForSide(update.Side, update.Row.Key, update.Row.Row)
	if err != nil {
		return err
	}
	if joinKey != entry.joinKey || !reflect.DeepEqual(update.Row.Row, entry.row) {
		return ErrIncrementalJoinRowConflict
	}
	return nil
}

func incrementalFullJoinEntryState(entry *incrementalJoinEntry) (int64, Row, uint64) {
	if entry == nil {
		return 0, nil, 0
	}
	return entry.count, entry.row, entry.time
}

func incrementalFullJoinNextState(update IncrementalJoinUpdate, oldCount int64, oldRow Row, oldTime uint64) (Row, uint64) {
	if oldCount == 0 {
		return cloneDifferentialRow(update.Row.Row), update.Row.Time
	}
	if update.Row.Row != nil {
		return cloneDifferentialRow(update.Row.Row), update.Row.Time
	}
	return oldRow, oldTime
}

func incrementalFullJoinLeftUnmatchedKey(key string) string {
	return "\x01" + key
}

func incrementalFullJoinRightUnmatchedKey(key string) string {
	return "\x02" + key
}
