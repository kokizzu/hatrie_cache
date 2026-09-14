package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
)

var (
	ErrIncrementalJoinNil                  = errors.New("incremental join operator is nil")
	ErrIncrementalJoinLeftKeyRequired      = errors.New("incremental join left key function is required")
	ErrIncrementalJoinRightKeyRequired     = errors.New("incremental join right key function is required")
	ErrIncrementalJoinMergeRequired        = errors.New("incremental join merge function is required")
	ErrIncrementalJoinSideInvalid          = errors.New("incremental join side is invalid")
	ErrIncrementalJoinKeyRequired          = errors.New("incremental join differential row key is required")
	ErrIncrementalJoinKeyInvalid           = errors.New("incremental join differential row key contains NUL")
	ErrIncrementalJoinRowRequired          = errors.New("incremental join row is required for a new key")
	ErrIncrementalJoinNegativeMultiplicity = errors.New("incremental join multiplicity became negative")
	ErrIncrementalJoinOverflow             = errors.New("incremental join multiplicity or output overflowed")
	ErrIncrementalJoinRowConflict          = errors.New("incremental join row conflicts with existing key")
)

// IncrementalJoinSide identifies the relation changed by an update.
type IncrementalJoinSide uint8

const (
	IncrementalJoinLeft IncrementalJoinSide = iota + 1
	IncrementalJoinRight
)

// IncrementalJoinKeyFunc extracts the equality key for one input row. The
// returned string is used as an exact equality key and may be empty.
type IncrementalJoinKeyFunc func(Row) (string, error)

// IncrementalJoinMergeFunc constructs one joined output row. The input rows
// are owned by the operator and must be treated as read-only.
type IncrementalJoinMergeFunc func(left, right Row) (Row, error)

// IncrementalJoinDefinition configures an exact keyed differential join.
type IncrementalJoinDefinition struct {
	LeftKey  IncrementalJoinKeyFunc
	RightKey IncrementalJoinKeyFunc
	Merge    IncrementalJoinMergeFunc
}

// IncrementalJoinUpdate is one signed multiplicity change on one join side.
// A positive update for a new key must include Row. Row may be nil when adding
// multiplicity to an already active key or retracting an existing key.
type IncrementalJoinUpdate struct {
	Side IncrementalJoinSide
	Row  DifferentialRow
}

// IncrementalJoin maintains an exact inner join under signed differential
// updates. It stores one keyed row per active input key and indexes each side
// by its equality key.
type IncrementalJoin struct {
	leftKey      IncrementalJoinKeyFunc
	rightKey     IncrementalJoinKeyFunc
	merge        IncrementalJoinMergeFunc
	left         map[string]*incrementalJoinEntry
	right        map[string]*incrementalJoinEntry
	leftBuckets  map[string]map[string]*incrementalJoinEntry
	rightBuckets map[string]map[string]*incrementalJoinEntry
}

type incrementalJoinEntry struct {
	key     string
	time    uint64
	row     Row
	joinKey string
	count   int64
}

type incrementalJoinPendingEntry struct {
	key     string
	time    uint64
	row     Row
	joinKey string
	count   int64
}

type incrementalJoinMatch struct {
	key     string
	time    uint64
	row     Row
	joinKey string
	count   int64
}

// NewIncrementalJoin creates an empty exact inner join.
func NewIncrementalJoin(definition IncrementalJoinDefinition) (*IncrementalJoin, error) {
	if definition.LeftKey == nil {
		return nil, ErrIncrementalJoinLeftKeyRequired
	}
	if definition.RightKey == nil {
		return nil, ErrIncrementalJoinRightKeyRequired
	}
	if definition.Merge == nil {
		return nil, ErrIncrementalJoinMergeRequired
	}
	return &IncrementalJoin{
		leftKey:      definition.LeftKey,
		rightKey:     definition.RightKey,
		merge:        definition.Merge,
		left:         make(map[string]*incrementalJoinEntry),
		right:        make(map[string]*incrementalJoinEntry),
		leftBuckets:  make(map[string]map[string]*incrementalJoinEntry),
		rightBuckets: make(map[string]map[string]*incrementalJoinEntry),
	}, nil
}

// Apply validates and applies signed updates atomically, returning the joined
// differential rows caused by the batch in input order.
func (join *IncrementalJoin) Apply(updates []IncrementalJoinUpdate) ([]DifferentialRow, error) {
	if join == nil {
		return nil, ErrIncrementalJoinNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 2 &&
		(updates[0].Side == IncrementalJoinLeft || updates[0].Side == IncrementalJoinRight) &&
		updates[0].Side == updates[1].Side &&
		updates[0].Row.Key == updates[1].Row.Key &&
		updates[0].Row.Diff < 0 && updates[1].Row.Diff > 0 {
		if _, exists := join.entriesForSide(updates[0].Side)[updates[0].Row.Key]; exists {
			return join.applyReplacement(updates)
		}
	}

	leftPending := make(map[string]*incrementalJoinPendingEntry, len(updates))
	rightPending := make(map[string]*incrementalJoinPendingEntry, len(updates))
	output := make([]DifferentialRow, 0)
	for index, update := range updates {
		if update.Side != IncrementalJoinLeft && update.Side != IncrementalJoinRight {
			return nil, fmt.Errorf("incremental join update %d: %w", index, ErrIncrementalJoinSideInvalid)
		}
		if update.Row.Key == "" {
			return nil, fmt.Errorf("incremental join update %d: %w", index, ErrIncrementalJoinKeyRequired)
		}
		if strings.IndexByte(update.Row.Key, 0) >= 0 {
			return nil, fmt.Errorf("incremental join update %d key %q: %w", index, update.Row.Key, ErrIncrementalJoinKeyInvalid)
		}
		if update.Row.Diff == 0 {
			continue
		}

		pending := join.pendingEntry(update.Side, update.Row.Key, leftPending, rightPending)
		if update.Row.Diff < 0 && pending.count == 0 {
			return nil, fmt.Errorf("incremental join update %d key %q: %w", index, update.Row.Key, ErrIncrementalJoinNegativeMultiplicity)
		}
		if update.Row.Diff > 0 {
			if err := join.preparePositive(index, update.Side, update.Row, pending); err != nil {
				return nil, err
			}
		}

		nextCount, ok := incrementalJoinAddCount(pending.count, update.Row.Diff)
		if !ok {
			return nil, fmt.Errorf("incremental join update %d key %q: %w", index, update.Row.Key, ErrIncrementalJoinNegativeMultiplicity)
		}
		opposite := IncrementalJoinRight
		if update.Side == IncrementalJoinRight {
			opposite = IncrementalJoinLeft
		}
		matches := join.matches(opposite, pending.joinKey, leftPending, rightPending)
		for _, match := range matches {
			if _, ok := incrementalJoinMultiplyCounts(nextCount, match.count); !ok {
				return nil, fmt.Errorf("incremental join update %d key %q: %w", index, update.Row.Key, ErrIncrementalJoinOverflow)
			}
			delta, ok := incrementalJoinMultiply(update.Row.Diff, match.count)
			if !ok {
				return nil, fmt.Errorf("incremental join update %d key %q: %w", index, update.Row.Key, ErrIncrementalJoinOverflow)
			}
			leftEntry := incrementalJoinMatch{key: pending.key, time: pending.time, row: pending.row, joinKey: pending.joinKey, count: pending.count}
			rightEntry := match
			leftKey, rightKey := update.Row.Key, match.key
			if update.Side == IncrementalJoinRight {
				leftEntry = match
				rightEntry = incrementalJoinMatch{key: pending.key, time: pending.time, row: pending.row, joinKey: pending.joinKey, count: pending.count}
				leftKey, rightKey = match.key, update.Row.Key
			}
			merged, err := join.merge(leftEntry.row, rightEntry.row)
			if err != nil {
				return nil, fmt.Errorf("incremental join update %d pair %q: %w", index, incrementalJoinPairKey(leftKey, rightKey), err)
			}
			output = append(output, DifferentialRow{
				Key:  incrementalJoinPairKey(leftKey, rightKey),
				Time: incrementalJoinTime(leftEntry.time, rightEntry.time),
				Diff: delta,
				Row:  cloneDifferentialRow(merged),
			})
		}
		pending.count = nextCount
	}

	join.commitPending(IncrementalJoinLeft, leftPending)
	join.commitPending(IncrementalJoinRight, rightPending)
	return output, nil
}

func (join *IncrementalJoin) applyReplacement(updates []IncrementalJoinUpdate) ([]DifferentialRow, error) {
	remove := updates[0]
	insert := updates[1]
	entries := join.entriesForSide(remove.Side)
	current := entries[remove.Row.Key]
	decrement := incrementalJoinMagnitude(remove.Row.Diff)
	if decrement > uint64(current.count) {
		return nil, fmt.Errorf("incremental join update 0 key %q: %w", remove.Row.Key, ErrIncrementalJoinNegativeMultiplicity)
	}
	remaining := current.count - int64(decrement)
	nextCount, ok := incrementalJoinAddCount(remaining, insert.Row.Diff)
	if !ok {
		return nil, fmt.Errorf("incremental join update 1 key %q: %w", insert.Row.Key, ErrIncrementalJoinOverflow)
	}

	newTime := current.time
	newRow := current.row
	newJoinKey := current.joinKey
	if remaining > 0 {
		if insert.Row.Row != nil {
			candidateJoinKey, err := join.keyForSide(insert.Side, insert.Row.Key, insert.Row.Row)
			if err != nil {
				return nil, fmt.Errorf("incremental join update 1 key %q join key: %w", insert.Row.Key, err)
			}
			if candidateJoinKey != current.joinKey || !reflect.DeepEqual(insert.Row.Row, current.row) {
				return nil, fmt.Errorf("incremental join update 1 key %q: %w", insert.Row.Key, ErrIncrementalJoinRowConflict)
			}
		}
	} else {
		if insert.Row.Row == nil {
			return nil, fmt.Errorf("incremental join update 1 key %q: %w", insert.Row.Key, ErrIncrementalJoinRowRequired)
		}
		candidateJoinKey, err := join.keyForSide(insert.Side, insert.Row.Key, insert.Row.Row)
		if err != nil {
			return nil, fmt.Errorf("incremental join update 1 key %q join key: %w", insert.Row.Key, err)
		}
		newTime = insert.Row.Time
		newRow = cloneDifferentialRow(insert.Row.Row)
		newJoinKey = candidateJoinKey
	}

	oldBucket := join.bucketsForSide(remove.Side)[current.joinKey]
	newBucket := join.bucketsForSide(insert.Side)[newJoinKey]
	output := make([]DifferentialRow, 0, len(oldBucket)+len(newBucket))
	output, err := join.appendReplacementDeltas(output, remove.Side, remove.Row.Key, current.joinKey, current.time, current.row, remaining, remove.Row.Diff)
	if err != nil {
		return nil, fmt.Errorf("incremental join update 0 key %q: %w", remove.Row.Key, err)
	}
	output, err = join.appendReplacementDeltas(output, insert.Side, insert.Row.Key, newJoinKey, newTime, newRow, nextCount, insert.Row.Diff)
	if err != nil {
		return nil, fmt.Errorf("incremental join update 1 key %q: %w", insert.Row.Key, err)
	}

	if remaining > 0 {
		current.count = nextCount
		return output, nil
	}
	incrementalJoinRemoveBucket(join.bucketsForSide(remove.Side), current.joinKey, current.key)
	current.time = newTime
	current.row = newRow
	current.joinKey = newJoinKey
	current.count = nextCount
	join.addToBucket(insert.Side, current)
	return output, nil
}

// Snapshot returns the complete joined relation with one row per active pair.
func (join *IncrementalJoin) Snapshot() ([]DifferentialRow, error) {
	if join == nil {
		return nil, ErrIncrementalJoinNil
	}
	joinKeys := make(map[string]struct{}, len(join.leftBuckets)+len(join.rightBuckets))
	for key := range join.leftBuckets {
		joinKeys[key] = struct{}{}
	}
	for key := range join.rightBuckets {
		joinKeys[key] = struct{}{}
	}
	orderedJoinKeys := make([]string, 0, len(joinKeys))
	for key := range joinKeys {
		orderedJoinKeys = append(orderedJoinKeys, key)
	}
	sort.Strings(orderedJoinKeys)
	var result []DifferentialRow
	for _, joinKey := range orderedJoinKeys {
		leftKeys := incrementalJoinSortedKeys(join.leftBuckets[joinKey])
		rightKeys := incrementalJoinSortedKeys(join.rightBuckets[joinKey])
		for _, leftKey := range leftKeys {
			left := join.leftBuckets[joinKey][leftKey]
			for _, rightKey := range rightKeys {
				right := join.rightBuckets[joinKey][rightKey]
				diff, ok := incrementalJoinMultiplyCounts(left.count, right.count)
				if !ok {
					return nil, fmt.Errorf("incremental join pair %q: %w", incrementalJoinPairKey(leftKey, rightKey), ErrIncrementalJoinOverflow)
				}
				merged, err := join.merge(left.row, right.row)
				if err != nil {
					return nil, fmt.Errorf("incremental join pair %q: %w", incrementalJoinPairKey(leftKey, rightKey), err)
				}
				result = append(result, DifferentialRow{
					Key:  incrementalJoinPairKey(leftKey, rightKey),
					Time: incrementalJoinTime(left.time, right.time),
					Diff: diff,
					Row:  cloneDifferentialRow(merged),
				})
			}
		}
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].Key < result[right].Key
	})
	return result, nil
}

// AllRows is an alias for Snapshot.
func (join *IncrementalJoin) AllRows() ([]DifferentialRow, error) {
	return join.Snapshot()
}

func (join *IncrementalJoin) pendingEntry(side IncrementalJoinSide, key string, leftPending, rightPending map[string]*incrementalJoinPendingEntry) *incrementalJoinPendingEntry {
	pending := leftPending
	currentEntries := join.left
	if side == IncrementalJoinRight {
		pending = rightPending
		currentEntries = join.right
	}
	if entry, ok := pending[key]; ok {
		return entry
	}
	entry := &incrementalJoinPendingEntry{key: key}
	if current, ok := currentEntries[key]; ok {
		entry.time = current.time
		entry.row = current.row
		entry.joinKey = current.joinKey
		entry.count = current.count
	}
	pending[key] = entry
	return entry
}

func (join *IncrementalJoin) entriesForSide(side IncrementalJoinSide) map[string]*incrementalJoinEntry {
	if side == IncrementalJoinLeft {
		return join.left
	}
	return join.right
}

func (join *IncrementalJoin) bucketsForSide(side IncrementalJoinSide) map[string]map[string]*incrementalJoinEntry {
	if side == IncrementalJoinLeft {
		return join.leftBuckets
	}
	return join.rightBuckets
}

func (join *IncrementalJoin) addToBucket(side IncrementalJoinSide, entry *incrementalJoinEntry) {
	buckets := join.bucketsForSide(side)
	if bucket := buckets[entry.joinKey]; bucket != nil {
		bucket[entry.key] = entry
		return
	}
	buckets[entry.joinKey] = map[string]*incrementalJoinEntry{entry.key: entry}
}

func (join *IncrementalJoin) appendReplacementDeltas(output []DifferentialRow, side IncrementalJoinSide, ownKey, joinKey string, ownTime uint64, ownRow Row, ownCount, ownDiff int64) ([]DifferentialRow, error) {
	bucket := join.bucketsForSide(IncrementalJoinRight)[joinKey]
	if side == IncrementalJoinRight {
		bucket = join.bucketsForSide(IncrementalJoinLeft)[joinKey]
	}
	if len(bucket) == 0 {
		return output, nil
	}
	if len(bucket) == 1 {
		for _, match := range bucket {
			return join.appendReplacementDelta(output, side, ownKey, ownTime, ownRow, ownCount, ownDiff, match)
		}
	}
	keys := incrementalJoinSortedKeys(bucket)
	for _, key := range keys {
		var err error
		output, err = join.appendReplacementDelta(output, side, ownKey, ownTime, ownRow, ownCount, ownDiff, bucket[key])
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}

func (join *IncrementalJoin) appendReplacementDelta(output []DifferentialRow, side IncrementalJoinSide, ownKey string, ownTime uint64, ownRow Row, ownCount, ownDiff int64, match *incrementalJoinEntry) ([]DifferentialRow, error) {
	if _, ok := incrementalJoinMultiplyCounts(ownCount, match.count); !ok {
		return nil, ErrIncrementalJoinOverflow
	}
	delta, ok := incrementalJoinMultiply(ownDiff, match.count)
	if !ok {
		return nil, ErrIncrementalJoinOverflow
	}
	leftKey, rightKey := ownKey, match.key
	leftTime, rightTime := ownTime, match.time
	leftRow, rightRow := ownRow, match.row
	if side == IncrementalJoinRight {
		leftKey, rightKey = match.key, ownKey
		leftTime, rightTime = match.time, ownTime
		leftRow, rightRow = match.row, ownRow
	}
	merged, err := join.merge(leftRow, rightRow)
	if err != nil {
		return nil, err
	}
	return append(output, DifferentialRow{
		Key:  incrementalJoinPairKey(leftKey, rightKey),
		Time: incrementalJoinTime(leftTime, rightTime),
		Diff: delta,
		Row:  cloneDifferentialRow(merged),
	}), nil
}

func (join *IncrementalJoin) preparePositive(index int, side IncrementalJoinSide, update DifferentialRow, pending *incrementalJoinPendingEntry) error {
	if pending.count == 0 {
		if update.Row == nil {
			return fmt.Errorf("incremental join update %d key %q: %w", index, update.Key, ErrIncrementalJoinRowRequired)
		}
		key, err := join.keyForSide(side, pending.key, update.Row)
		if err != nil {
			return fmt.Errorf("incremental join update %d key %q join key: %w", index, update.Key, err)
		}
		pending.time = update.Time
		pending.row = cloneDifferentialRow(update.Row)
		pending.joinKey = key
		return nil
	}
	if update.Row == nil {
		return nil
	}
	key, err := join.keyForSide(side, pending.key, update.Row)
	if err != nil {
		return fmt.Errorf("incremental join update %d key %q join key: %w", index, update.Key, err)
	}
	if key != pending.joinKey || !reflect.DeepEqual(update.Row, pending.row) {
		return fmt.Errorf("incremental join update %d key %q: %w", index, update.Key, ErrIncrementalJoinRowConflict)
	}
	return nil
}

func (join *IncrementalJoin) matches(side IncrementalJoinSide, joinKey string, leftPending, rightPending map[string]*incrementalJoinPendingEntry) []incrementalJoinMatch {
	buckets := join.rightBuckets
	pending := rightPending
	if side == IncrementalJoinLeft {
		buckets = join.leftBuckets
		pending = leftPending
	}
	base := buckets[joinKey]
	keys := make([]string, 0, len(base)+len(pending))
	for key, current := range base {
		if entry, ok := pending[key]; ok {
			if entry.count > 0 && entry.joinKey == joinKey {
				keys = append(keys, key)
			}
			continue
		}
		if current.count > 0 {
			keys = append(keys, key)
		}
	}
	for key, entry := range pending {
		if _, exists := base[key]; exists {
			continue
		}
		if entry.count > 0 && entry.joinKey == joinKey {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := make([]incrementalJoinMatch, 0, len(keys))
	for _, key := range keys {
		if entry, ok := pending[key]; ok {
			result = append(result, incrementalJoinMatch{key: entry.key, time: entry.time, row: entry.row, joinKey: entry.joinKey, count: entry.count})
			continue
		}
		entry := base[key]
		result = append(result, incrementalJoinMatch{key: entry.key, time: entry.time, row: entry.row, joinKey: entry.joinKey, count: entry.count})
	}
	return result
}

func (join *IncrementalJoin) commitPending(side IncrementalJoinSide, pending map[string]*incrementalJoinPendingEntry) {
	entries := join.left
	buckets := join.leftBuckets
	if side == IncrementalJoinRight {
		entries = join.right
		buckets = join.rightBuckets
	}
	for key, update := range pending {
		current, exists := entries[key]
		if update.count == 0 {
			if exists {
				incrementalJoinRemoveBucket(buckets, current.joinKey, key)
				delete(entries, key)
			}
			continue
		}
		if !exists {
			current = &incrementalJoinEntry{key: key}
			entries[key] = current
		} else if current.joinKey != update.joinKey {
			incrementalJoinRemoveBucket(buckets, current.joinKey, key)
		}
		current.time = update.time
		current.row = update.row
		current.joinKey = update.joinKey
		current.count = update.count
		if bucket := buckets[update.joinKey]; bucket == nil {
			buckets[update.joinKey] = map[string]*incrementalJoinEntry{key: current}
		} else {
			bucket[key] = current
		}
	}
}

func (join *IncrementalJoin) keyForSide(side IncrementalJoinSide, key string, row Row) (string, error) {
	if side == IncrementalJoinLeft {
		return join.leftKey(row)
	}
	if side == IncrementalJoinRight {
		return join.rightKey(row)
	}
	return "", fmt.Errorf("incremental join side %d: %w", side, ErrIncrementalJoinSideInvalid)
}

func incrementalJoinAddCount(current, diff int64) (int64, bool) {
	if diff > 0 {
		if current > math.MaxInt64-diff {
			return 0, false
		}
		return current + diff, true
	}
	decrement := incrementalJoinMagnitude(diff)
	if decrement > uint64(current) {
		return 0, false
	}
	return current - int64(decrement), true
}

func incrementalJoinMagnitude(diff int64) uint64 {
	return uint64(-(diff + 1)) + 1
}

func incrementalJoinMultiply(diff, count int64) (int64, bool) {
	if diff == 0 || count <= 0 {
		return 0, true
	}
	absDiff := uint64(diff)
	if diff < 0 {
		absDiff = incrementalJoinMagnitude(diff)
	}
	if absDiff > uint64(math.MaxInt64)/uint64(count) {
		return 0, false
	}
	product := absDiff * uint64(count)
	if diff < 0 {
		return -int64(product), true
	}
	return int64(product), true
}

func incrementalJoinMultiplyCounts(left, right int64) (int64, bool) {
	if left <= 0 || right <= 0 {
		return 0, true
	}
	if uint64(left) > uint64(math.MaxInt64)/uint64(right) {
		return 0, false
	}
	return left * right, true
}

func incrementalJoinPairKey(left, right string) string {
	return left + "\x00" + right
}

func incrementalJoinTime(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}

func incrementalJoinSortedKeys(entries map[string]*incrementalJoinEntry) []string {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func incrementalJoinRemoveBucket(buckets map[string]map[string]*incrementalJoinEntry, joinKey, key string) {
	bucket := buckets[joinKey]
	delete(bucket, key)
	if len(bucket) == 0 {
		delete(buckets, joinKey)
	}
}
