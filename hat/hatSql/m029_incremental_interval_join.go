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
	ErrIncrementalIntervalJoinNil                   = errors.New("hatSql: incremental interval join is nil")
	ErrIncrementalIntervalJoinLeftKeyRequired       = errors.New("hatSql: incremental interval join left key function is required")
	ErrIncrementalIntervalJoinRightKeyRequired      = errors.New("hatSql: incremental interval join right key function is required")
	ErrIncrementalIntervalJoinLeftIntervalRequired  = errors.New("hatSql: incremental interval join left interval function is required")
	ErrIncrementalIntervalJoinRightIntervalRequired = errors.New("hatSql: incremental interval join right interval function is required")
	ErrIncrementalIntervalJoinMergeRequired         = errors.New("hatSql: incremental interval join merge function is required")
	ErrIncrementalIntervalJoinSideInvalid           = errors.New("hatSql: incremental interval join side is invalid")
	ErrIncrementalIntervalJoinKeyRequired           = errors.New("hatSql: incremental interval join key is required")
	ErrIncrementalIntervalJoinKeyInvalid            = errors.New("hatSql: incremental interval join key is invalid")
	ErrIncrementalIntervalJoinRowRequired           = errors.New("hatSql: incremental interval join row is required")
	ErrIncrementalIntervalJoinIntervalInvalid       = errors.New("hatSql: incremental interval join interval is invalid")
	ErrIncrementalIntervalJoinMultiplicity          = errors.New("hatSql: incremental interval join multiplicity is invalid")
	ErrIncrementalIntervalJoinOverflow              = errors.New("hatSql: incremental interval join multiplicity overflow")
	ErrIncrementalIntervalJoinRowConflict           = errors.New("hatSql: incremental interval join row conflicts with retained row")
)

// IncrementalIntervalJoinSide identifies the source side of an interval join
// update.
type IncrementalIntervalJoinSide uint8

const (
	IncrementalIntervalJoinLeft IncrementalIntervalJoinSide = iota + 1
	IncrementalIntervalJoinRight
)

// IncrementalIntervalJoinIntervalFunc extracts a half-open [start, end)
// validity interval from a source row. The timestamp domain is caller-owned;
// int64 permits Unix nanoseconds, logical times, or compact sequence times.
type IncrementalIntervalJoinIntervalFunc func(Row) (start, end int64, err error)

// IncrementalIntervalJoinDefinition configures an exact equi-inner interval
// join. The merge callback should treat its inputs as read-only.
type IncrementalIntervalJoinDefinition struct {
	LeftKey       IncrementalJoinKeyFunc
	RightKey      IncrementalJoinKeyFunc
	LeftInterval  IncrementalIntervalJoinIntervalFunc
	RightInterval IncrementalIntervalJoinIntervalFunc
	Merge         IncrementalJoinMergeFunc
}

// IncrementalIntervalJoinUpdate changes one source row's signed multiplicity.
// A negative update may omit Row and uses the retained row metadata. A
// positive update must include Row when introducing a source key or replacing
// a key whose prior multiplicity reached zero.
type IncrementalIntervalJoinUpdate struct {
	Side IncrementalIntervalJoinSide
	Row  DifferentialRow
}

// IncrementalIntervalJoin maintains an exact signed equi-inner join whose
// matching predicate is equality on the configured key followed by overlap of
// the configured half-open validity intervals.
//
// Apply is atomic with respect to validation and merge errors. The type is
// intentionally not synchronized; callers should serialize Apply and
// Snapshot calls just as they do for IncrementalJoin.
type IncrementalIntervalJoin struct {
	leftKey       IncrementalJoinKeyFunc
	rightKey      IncrementalJoinKeyFunc
	leftInterval  IncrementalIntervalJoinIntervalFunc
	rightInterval IncrementalIntervalJoinIntervalFunc
	merge         IncrementalJoinMergeFunc
	left          map[string]*incrementalIntervalJoinEntry
	right         map[string]*incrementalIntervalJoinEntry
	leftBuckets   map[string]*incrementalIntervalJoinBucket
	rightBuckets  map[string]*incrementalIntervalJoinBucket
}

type incrementalIntervalJoinEntry struct {
	key          string
	joinKey      string
	start        int64
	end          int64
	row          Row
	count        int64
	treePriority uint64
	treeMaxEnd   int64
	treeLeft     *incrementalIntervalJoinEntry
	treeRight    *incrementalIntervalJoinEntry
}

type incrementalIntervalJoinPendingEntry struct {
	key     string
	joinKey string
	start   int64
	end     int64
	row     Row
	count   int64
}

type incrementalIntervalJoinMatch struct {
	key   string
	row   Row
	count int64
}

type incrementalIntervalJoinBucket struct {
	root *incrementalIntervalJoinEntry
	size int
}

// NewIncrementalIntervalJoin creates an empty exact interval join.
func NewIncrementalIntervalJoin(definition IncrementalIntervalJoinDefinition) (*IncrementalIntervalJoin, error) {
	if definition.LeftKey == nil {
		return nil, ErrIncrementalIntervalJoinLeftKeyRequired
	}
	if definition.RightKey == nil {
		return nil, ErrIncrementalIntervalJoinRightKeyRequired
	}
	if definition.LeftInterval == nil {
		return nil, ErrIncrementalIntervalJoinLeftIntervalRequired
	}
	if definition.RightInterval == nil {
		return nil, ErrIncrementalIntervalJoinRightIntervalRequired
	}
	if definition.Merge == nil {
		return nil, ErrIncrementalIntervalJoinMergeRequired
	}
	return &IncrementalIntervalJoin{
		leftKey:       definition.LeftKey,
		rightKey:      definition.RightKey,
		leftInterval:  definition.LeftInterval,
		rightInterval: definition.RightInterval,
		merge:         definition.Merge,
		left:          make(map[string]*incrementalIntervalJoinEntry),
		right:         make(map[string]*incrementalIntervalJoinEntry),
		leftBuckets:   make(map[string]*incrementalIntervalJoinBucket),
		rightBuckets:  make(map[string]*incrementalIntervalJoinBucket),
	}, nil
}

// Apply atomically applies signed source updates and returns only the joined
// rows whose multiplicity changed. Joined keys are the NUL-delimited source
// keys in left/right order.
func (join *IncrementalIntervalJoin) Apply(updates []IncrementalIntervalJoinUpdate) ([]DifferentialRow, error) {
	if join == nil {
		return nil, ErrIncrementalIntervalJoinNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 2 &&
		updates[0].Side == updates[1].Side &&
		(updates[0].Side == IncrementalIntervalJoinLeft || updates[0].Side == IncrementalIntervalJoinRight) &&
		updates[0].Row.Key == updates[1].Row.Key &&
		updates[0].Row.Diff < 0 && updates[1].Row.Diff > 0 {
		if _, exists := join.incrementalIntervalJoinEntries(updates[0].Side)[updates[0].Row.Key]; exists {
			return join.applyReplacement(updates)
		}
	}

	leftPending := make(map[string]*incrementalIntervalJoinPendingEntry, len(updates))
	rightPending := make(map[string]*incrementalIntervalJoinPendingEntry, len(updates))
	deltas := make([]DifferentialRow, 0)
	for _, update := range updates {
		if update.Side != IncrementalIntervalJoinLeft && update.Side != IncrementalIntervalJoinRight {
			return nil, ErrIncrementalIntervalJoinSideInvalid
		}
		if err := validateIncrementalIntervalJoinSourceKey(update.Row.Key); err != nil {
			return nil, err
		}
		if update.Row.Diff == 0 {
			continue
		}

		pending := join.incrementalIntervalJoinPending(update.Side, leftPending, rightPending)
		entry, err := join.incrementalIntervalJoinCurrent(update.Side, update.Row.Key, pending)
		if err != nil {
			return nil, err
		}
		if update.Row.Diff > 0 {
			entry, err = join.incrementalIntervalJoinPreparePositive(update.Side, update.Row.Key, update.Row.Row, pending, entry)
		} else {
			entry, err = join.incrementalIntervalJoinPrepareRetraction(update.Side, update.Row.Key, update.Row.Row, entry)
		}
		if err != nil {
			return nil, err
		}

		if update.Row.Diff < 0 && intervalJoinAbsInt64(update.Row.Diff) > uint64(entry.count) {
			return nil, fmt.Errorf("%w: key %q", ErrIncrementalIntervalJoinMultiplicity, update.Row.Key)
		}
		nextCount, err := addIncrementalIntervalJoinCount(entry.count, update.Row.Diff)
		if err != nil {
			return nil, fmt.Errorf("%w: key %q", err, update.Row.Key)
		}

		oppositePending := rightPending
		if update.Side == IncrementalIntervalJoinRight {
			oppositePending = leftPending
		}
		matches := join.incrementalIntervalJoinMatches(update.Side, entry.joinKey, entry.start, entry.end, oppositePending)
		for _, match := range matches {
			if _, err := multiplyIncrementalIntervalJoinDelta(nextCount, match.count); err != nil {
				return nil, fmt.Errorf("%w: key %q with match %q", err, update.Row.Key, match.key)
			}
			joinedDiff, err := multiplyIncrementalIntervalJoinDelta(update.Row.Diff, match.count)
			if err != nil {
				return nil, fmt.Errorf("%w: key %q with match %q", err, update.Row.Key, match.key)
			}
			leftRow, rightRow := entry.row, match.row
			if update.Side == IncrementalIntervalJoinRight {
				leftRow, rightRow = match.row, entry.row
			}
			merged, err := join.merge(leftRow, rightRow)
			if err != nil {
				return nil, err
			}
			joinedKey := update.Row.Key + "\x00" + match.key
			if update.Side == IncrementalIntervalJoinRight {
				joinedKey = match.key + "\x00" + update.Row.Key
			}
			deltas = append(deltas, DifferentialRow{
				Key:  joinedKey,
				Diff: joinedDiff,
				Row:  cloneIncrementalIntervalJoinRow(merged),
			})
		}
		entry.count = nextCount
		pending[update.Row.Key] = entry
	}

	join.incrementalIntervalJoinCommit(IncrementalIntervalJoinLeft, leftPending)
	join.incrementalIntervalJoinCommit(IncrementalIntervalJoinRight, rightPending)
	return deltas, nil
}

// applyReplacement handles the common delete-then-insert update for one
// retained source key without constructing pending maps. It preserves the
// generic path's validation, delta order, row cloning, and atomic commit.
func (join *IncrementalIntervalJoin) applyReplacement(updates []IncrementalIntervalJoinUpdate) ([]DifferentialRow, error) {
	remove := updates[0]
	insert := updates[1]
	if remove.Side != insert.Side ||
		(remove.Side != IncrementalIntervalJoinLeft && remove.Side != IncrementalIntervalJoinRight) ||
		remove.Row.Key != insert.Row.Key || remove.Row.Diff >= 0 || insert.Row.Diff <= 0 {
		return nil, ErrIncrementalIntervalJoinMultiplicity
	}
	if err := validateIncrementalIntervalJoinSourceKey(remove.Row.Key); err != nil {
		return nil, err
	}
	if err := validateIncrementalIntervalJoinSourceKey(insert.Row.Key); err != nil {
		return nil, err
	}

	entries := join.incrementalIntervalJoinEntries(remove.Side)
	current := entries[remove.Row.Key]
	if current == nil || current.count <= 0 {
		return nil, fmt.Errorf("%w: key %q", ErrIncrementalIntervalJoinMultiplicity, remove.Row.Key)
	}
	if remove.Row.Row != nil {
		joinKey, start, end, err := join.incrementalIntervalJoinMetadata(remove.Side, remove.Row.Row)
		if err != nil {
			return nil, err
		}
		if current.joinKey != joinKey || current.start != start || current.end != end || !reflect.DeepEqual(current.row, remove.Row.Row) {
			return nil, fmt.Errorf("%w: key %q", ErrIncrementalIntervalJoinRowConflict, remove.Row.Key)
		}
	}

	decrement := intervalJoinAbsInt64(remove.Row.Diff)
	if decrement > uint64(current.count) {
		return nil, fmt.Errorf("%w: key %q", ErrIncrementalIntervalJoinMultiplicity, remove.Row.Key)
	}
	remaining := current.count - int64(decrement)

	newJoinKey := current.joinKey
	newStart := current.start
	newEnd := current.end
	newRow := current.row
	if insert.Row.Row != nil && remaining > 0 {
		joinKey, start, end, err := join.incrementalIntervalJoinMetadata(insert.Side, insert.Row.Row)
		if err != nil {
			return nil, err
		}
		if current.joinKey != joinKey || current.start != start || current.end != end || !reflect.DeepEqual(current.row, insert.Row.Row) {
			return nil, fmt.Errorf("%w: key %q", ErrIncrementalIntervalJoinRowConflict, insert.Row.Key)
		}
	} else if remaining == 0 {
		if insert.Row.Row == nil {
			return nil, fmt.Errorf("%w: key %q", ErrIncrementalIntervalJoinRowRequired, insert.Row.Key)
		}
		var err error
		newJoinKey, newStart, newEnd, err = join.incrementalIntervalJoinMetadata(insert.Side, insert.Row.Row)
		if err != nil {
			return nil, err
		}
		newRow = cloneIncrementalIntervalJoinRow(insert.Row.Row)
	}

	nextCount, err := addIncrementalIntervalJoinCount(remaining, insert.Row.Diff)
	if err != nil {
		return nil, fmt.Errorf("%w: key %q", err, insert.Row.Key)
	}

	oldMatches := join.incrementalIntervalJoinReplacementMatches(remove.Side, current.joinKey, current.start, current.end)
	newMatches := join.incrementalIntervalJoinReplacementMatches(insert.Side, newJoinKey, newStart, newEnd)
	deltas := make([]DifferentialRow, 0, len(oldMatches)+len(newMatches))
	deltas, err = join.appendIncrementalIntervalJoinReplacementDeltas(deltas, remove.Side, remove.Row.Key, current.row, remove.Row.Diff, remaining, oldMatches)
	if err != nil {
		return nil, err
	}
	deltas, err = join.appendIncrementalIntervalJoinReplacementDeltas(deltas, insert.Side, insert.Row.Key, newRow, insert.Row.Diff, nextCount, newMatches)
	if err != nil {
		return nil, err
	}

	if remaining > 0 {
		current.count = nextCount
		return deltas, nil
	}
	buckets := join.leftBuckets
	if remove.Side == IncrementalIntervalJoinRight {
		buckets = join.rightBuckets
	}
	if current.joinKey != newJoinKey || current.start != newStart || current.end != newEnd {
		join.incrementalIntervalJoinRemoveFromBucket(buckets, current)
		current.joinKey = newJoinKey
		current.start = newStart
		current.end = newEnd
		current.row = newRow
		current.count = nextCount
		join.incrementalIntervalJoinAddToBucket(buckets, current)
		return deltas, nil
	}
	current.row = newRow
	current.count = nextCount
	return deltas, nil
}

func (join *IncrementalIntervalJoin) incrementalIntervalJoinReplacementMatches(side IncrementalIntervalJoinSide, joinKey string, start, end int64) []*incrementalIntervalJoinEntry {
	buckets := join.leftBuckets
	if side == IncrementalIntervalJoinLeft {
		buckets = join.rightBuckets
	}
	bucket := buckets[joinKey]
	if bucket == nil {
		return nil
	}
	return bucket.overlap(start, end)
}

func (join *IncrementalIntervalJoin) appendIncrementalIntervalJoinReplacementDeltas(output []DifferentialRow, side IncrementalIntervalJoinSide, key string, row Row, diff, nextCount int64, matches []*incrementalIntervalJoinEntry) ([]DifferentialRow, error) {
	for _, match := range matches {
		if _, err := multiplyIncrementalIntervalJoinDelta(nextCount, match.count); err != nil {
			return nil, fmt.Errorf("%w: key %q with match %q", err, key, match.key)
		}
		joinedDiff, err := multiplyIncrementalIntervalJoinDelta(diff, match.count)
		if err != nil {
			return nil, fmt.Errorf("%w: key %q with match %q", err, key, match.key)
		}
		leftRow, rightRow := row, match.row
		joinedKey := key + "\x00" + match.key
		if side == IncrementalIntervalJoinRight {
			leftRow, rightRow = match.row, row
			joinedKey = match.key + "\x00" + key
		}
		merged, err := join.merge(leftRow, rightRow)
		if err != nil {
			return nil, err
		}
		output = append(output, DifferentialRow{
			Key:  joinedKey,
			Diff: joinedDiff,
			Row:  cloneIncrementalIntervalJoinRow(merged),
		})
	}
	return output, nil
}

// Snapshot returns the complete current joined multiset in deterministic key
// order. It is useful for recovery checks and consumers that need a full view.
func (join *IncrementalIntervalJoin) Snapshot() ([]DifferentialRow, error) {
	if join == nil {
		return nil, ErrIncrementalIntervalJoinNil
	}
	joinKeys := make(map[string]struct{}, len(join.leftBuckets)+len(join.rightBuckets))
	for key := range join.leftBuckets {
		joinKeys[key] = struct{}{}
	}
	for key := range join.rightBuckets {
		joinKeys[key] = struct{}{}
	}
	sortedJoinKeys := make([]string, 0, len(joinKeys))
	for key := range joinKeys {
		sortedJoinKeys = append(sortedJoinKeys, key)
	}
	sort.Strings(sortedJoinKeys)

	result := make([]DifferentialRow, 0)
	for _, joinKey := range sortedJoinKeys {
		leftBucket := join.leftBuckets[joinKey]
		rightBucket := join.rightBuckets[joinKey]
		if leftBucket == nil || rightBucket == nil {
			continue
		}
		leftEntries := leftBucket.activeEntries()
		for _, leftEntry := range leftEntries {
			for _, rightEntry := range rightBucket.overlap(leftEntry.start, leftEntry.end) {
				joinedDiff, err := multiplyIncrementalIntervalJoinDelta(leftEntry.count, rightEntry.count)
				if err != nil {
					return nil, err
				}
				merged, err := join.merge(leftEntry.row, rightEntry.row)
				if err != nil {
					return nil, err
				}
				result = append(result, DifferentialRow{
					Key:  leftEntry.key + "\x00" + rightEntry.key,
					Diff: joinedDiff,
					Row:  cloneIncrementalIntervalJoinRow(merged),
				})
			}
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

// AllRows is an alias for Snapshot.
func (join *IncrementalIntervalJoin) AllRows() ([]DifferentialRow, error) {
	return join.Snapshot()
}

func (join *IncrementalIntervalJoin) incrementalIntervalJoinCurrent(side IncrementalIntervalJoinSide, key string, pending map[string]*incrementalIntervalJoinPendingEntry) (*incrementalIntervalJoinPendingEntry, error) {
	if entry, ok := pending[key]; ok {
		return entry, nil
	}
	entries := join.incrementalIntervalJoinEntries(side)
	current, ok := entries[key]
	if !ok {
		return nil, nil
	}
	entry := &incrementalIntervalJoinPendingEntry{
		key:     current.key,
		joinKey: current.joinKey,
		start:   current.start,
		end:     current.end,
		row:     current.row,
		count:   current.count,
	}
	pending[key] = entry
	return entry, nil
}

func (join *IncrementalIntervalJoin) incrementalIntervalJoinPreparePositive(side IncrementalIntervalJoinSide, key string, row Row, pending map[string]*incrementalIntervalJoinPendingEntry, entry *incrementalIntervalJoinPendingEntry) (*incrementalIntervalJoinPendingEntry, error) {
	if entry != nil && entry.count > 0 {
		if row == nil {
			return entry, nil
		}
		joinKey, start, end, err := join.incrementalIntervalJoinMetadata(side, row)
		if err != nil {
			return nil, err
		}
		if entry.joinKey != joinKey || entry.start != start || entry.end != end || !reflect.DeepEqual(entry.row, row) {
			return nil, fmt.Errorf("%w: key %q", ErrIncrementalIntervalJoinRowConflict, key)
		}
		return entry, nil
	}
	if row == nil {
		return nil, fmt.Errorf("%w: key %q", ErrIncrementalIntervalJoinRowRequired, key)
	}
	joinKey, start, end, err := join.incrementalIntervalJoinMetadata(side, row)
	if err != nil {
		return nil, err
	}
	entry = &incrementalIntervalJoinPendingEntry{
		key:     key,
		joinKey: joinKey,
		start:   start,
		end:     end,
		row:     cloneIncrementalIntervalJoinRow(row),
	}
	pending[key] = entry
	return entry, nil
}

func (join *IncrementalIntervalJoin) incrementalIntervalJoinPrepareRetraction(side IncrementalIntervalJoinSide, key string, row Row, entry *incrementalIntervalJoinPendingEntry) (*incrementalIntervalJoinPendingEntry, error) {
	if entry == nil || entry.count <= 0 {
		return nil, fmt.Errorf("%w: key %q", ErrIncrementalIntervalJoinMultiplicity, key)
	}
	if row != nil {
		joinKey, start, end, err := join.incrementalIntervalJoinMetadata(side, row)
		if err != nil {
			return nil, err
		}
		if entry.joinKey != joinKey || entry.start != start || entry.end != end || !reflect.DeepEqual(entry.row, row) {
			return nil, fmt.Errorf("%w: key %q", ErrIncrementalIntervalJoinRowConflict, key)
		}
	}
	return entry, nil
}

func (join *IncrementalIntervalJoin) incrementalIntervalJoinMetadata(side IncrementalIntervalJoinSide, row Row) (string, int64, int64, error) {
	keyFunc := join.leftKey
	intervalFunc := join.leftInterval
	if side == IncrementalIntervalJoinRight {
		keyFunc = join.rightKey
		intervalFunc = join.rightInterval
	}
	joinKey, err := keyFunc(row)
	if err != nil {
		return "", 0, 0, err
	}
	if joinKey == "" {
		return "", 0, 0, ErrIncrementalIntervalJoinKeyRequired
	}
	if strings.IndexByte(joinKey, 0) >= 0 {
		return "", 0, 0, ErrIncrementalIntervalJoinKeyInvalid
	}
	start, end, err := intervalFunc(row)
	if err != nil {
		return "", 0, 0, err
	}
	if start >= end {
		return "", 0, 0, ErrIncrementalIntervalJoinIntervalInvalid
	}
	return joinKey, start, end, nil
}

func (join *IncrementalIntervalJoin) incrementalIntervalJoinMatches(side IncrementalIntervalJoinSide, joinKey string, start, end int64, pending map[string]*incrementalIntervalJoinPendingEntry) []incrementalIntervalJoinMatch {
	buckets := join.leftBuckets
	if side == IncrementalIntervalJoinLeft {
		buckets = join.rightBuckets
	}
	bucket := buckets[joinKey]
	if len(pending) == 0 {
		if bucket == nil {
			return nil
		}
		entries := bucket.overlap(start, end)
		matches := make([]incrementalIntervalJoinMatch, 0, len(entries))
		for _, entry := range entries {
			matches = append(matches, incrementalIntervalJoinMatch{key: entry.key, row: entry.row, count: entry.count})
		}
		return matches
	}

	base := make([]*incrementalIntervalJoinEntry, 0)
	if bucket != nil {
		base = bucket.overlap(start, end)
	}
	seen := make(map[string]struct{}, len(base))
	matches := make([]incrementalIntervalJoinMatch, 0, len(base)+len(pending))
	for _, current := range base {
		seen[current.key] = struct{}{}
		if changed, ok := pending[current.key]; ok {
			if changed.count > 0 && changed.joinKey == joinKey && intervalsOverlap(start, end, changed.start, changed.end) {
				matches = append(matches, incrementalIntervalJoinMatch{key: changed.key, row: changed.row, count: changed.count})
			}
			continue
		}
		matches = append(matches, incrementalIntervalJoinMatch{key: current.key, row: current.row, count: current.count})
	}
	for key, changed := range pending {
		if changed.count <= 0 || changed.joinKey != joinKey {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		if intervalsOverlap(start, end, changed.start, changed.end) {
			matches = append(matches, incrementalIntervalJoinMatch{key: changed.key, row: changed.row, count: changed.count})
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].key < matches[j].key })
	return matches
}

func (join *IncrementalIntervalJoin) incrementalIntervalJoinCommit(side IncrementalIntervalJoinSide, pending map[string]*incrementalIntervalJoinPendingEntry) {
	entries := join.left
	buckets := join.leftBuckets
	if side == IncrementalIntervalJoinRight {
		entries = join.right
		buckets = join.rightBuckets
	}
	for key, next := range pending {
		current := entries[key]
		if next.count <= 0 {
			if current != nil {
				join.incrementalIntervalJoinRemoveFromBucket(buckets, current)
				delete(entries, key)
			}
			continue
		}
		needsAdd := false
		if current == nil {
			current = &incrementalIntervalJoinEntry{key: next.key}
			entries[key] = current
			needsAdd = true
		} else if current.joinKey != next.joinKey || current.start != next.start || current.end != next.end {
			join.incrementalIntervalJoinRemoveFromBucket(buckets, current)
			needsAdd = true
		}
		current.joinKey = next.joinKey
		current.start = next.start
		current.end = next.end
		current.row = next.row
		current.count = next.count
		if needsAdd {
			join.incrementalIntervalJoinAddToBucket(buckets, current)
		}
	}
}

func (join *IncrementalIntervalJoin) incrementalIntervalJoinPending(side IncrementalIntervalJoinSide, left, right map[string]*incrementalIntervalJoinPendingEntry) map[string]*incrementalIntervalJoinPendingEntry {
	if side == IncrementalIntervalJoinLeft {
		return left
	}
	return right
}

func (join *IncrementalIntervalJoin) incrementalIntervalJoinEntries(side IncrementalIntervalJoinSide) map[string]*incrementalIntervalJoinEntry {
	if side == IncrementalIntervalJoinLeft {
		return join.left
	}
	return join.right
}

func (join *IncrementalIntervalJoin) incrementalIntervalJoinAddToBucket(buckets map[string]*incrementalIntervalJoinBucket, entry *incrementalIntervalJoinEntry) {
	bucket := buckets[entry.joinKey]
	if bucket == nil {
		bucket = &incrementalIntervalJoinBucket{}
		buckets[entry.joinKey] = bucket
	}
	bucket.add(entry)
}

func (join *IncrementalIntervalJoin) incrementalIntervalJoinRemoveFromBucket(buckets map[string]*incrementalIntervalJoinBucket, entry *incrementalIntervalJoinEntry) {
	bucket := buckets[entry.joinKey]
	if bucket == nil {
		return
	}
	bucket.remove(entry.start, entry.key)
	if bucket.size == 0 {
		delete(buckets, entry.joinKey)
	}
}

func (bucket *incrementalIntervalJoinBucket) add(entry *incrementalIntervalJoinEntry) {
	entry.treePriority = incrementalIntervalJoinPriority(entry.start, entry.key)
	entry.treeMaxEnd = entry.end
	entry.treeLeft = nil
	entry.treeRight = nil
	bucket.root = incrementalIntervalJoinTreeInsert(bucket.root, entry)
	bucket.size++
}

func (bucket *incrementalIntervalJoinBucket) remove(start int64, key string) {
	entry := incrementalIntervalJoinTreeFind(bucket.root, start, key)
	var removed bool
	bucket.root, removed = incrementalIntervalJoinTreeDelete(bucket.root, start, key)
	if removed {
		if entry != nil {
			entry.treeLeft = nil
			entry.treeRight = nil
		}
		bucket.size--
	}
}

func incrementalIntervalJoinTreeFind(root *incrementalIntervalJoinEntry, start int64, key string) *incrementalIntervalJoinEntry {
	for root != nil {
		if start == root.start && key == root.key {
			return root
		}
		if start < root.start || (start == root.start && key < root.key) {
			root = root.treeLeft
			continue
		}
		root = root.treeRight
	}
	return nil
}

func (bucket *incrementalIntervalJoinBucket) overlap(start, end int64) []*incrementalIntervalJoinEntry {
	if bucket == nil || bucket.root == nil {
		return nil
	}
	result := make([]*incrementalIntervalJoinEntry, 0)
	incrementalIntervalJoinTreeOverlap(bucket.root, start, end, &result)
	return result
}

func (bucket *incrementalIntervalJoinBucket) activeEntries() []*incrementalIntervalJoinEntry {
	if bucket == nil || bucket.root == nil {
		return nil
	}
	active := make([]*incrementalIntervalJoinEntry, 0, bucket.size)
	incrementalIntervalJoinTreeWalk(bucket.root, func(entry *incrementalIntervalJoinEntry) {
		if entry.count > 0 {
			active = append(active, entry)
		}
	})
	sort.Slice(active, func(i, j int) bool { return active[i].key < active[j].key })
	return active
}

func incrementalIntervalJoinPriority(start int64, key string) uint64 {
	hash := uint64(start) + 0x9e3779b97f4a7c15
	for index := 0; index < len(key); index++ {
		hash ^= uint64(key[index])
		hash *= 0x100000001b3
	}
	hash ^= hash >> 30
	hash *= 0xbf58476d1ce4e5b9
	hash ^= hash >> 27
	hash *= 0x94d049bb133111eb
	return hash ^ (hash >> 31)
}

func incrementalIntervalJoinEntryLess(left, right *incrementalIntervalJoinEntry) bool {
	if left.start != right.start {
		return left.start < right.start
	}
	return left.key < right.key
}

func incrementalIntervalJoinTreeInsert(root, node *incrementalIntervalJoinEntry) *incrementalIntervalJoinEntry {
	if root == nil {
		return node
	}
	if incrementalIntervalJoinEntryLess(node, root) {
		root.treeLeft = incrementalIntervalJoinTreeInsert(root.treeLeft, node)
		if root.treeLeft.treePriority < root.treePriority {
			root = incrementalIntervalJoinTreeRotateRight(root)
		}
	} else {
		root.treeRight = incrementalIntervalJoinTreeInsert(root.treeRight, node)
		if root.treeRight.treePriority < root.treePriority {
			root = incrementalIntervalJoinTreeRotateLeft(root)
		}
	}
	incrementalIntervalJoinTreeUpdate(root)
	return root
}

func incrementalIntervalJoinTreeDelete(root *incrementalIntervalJoinEntry, start int64, key string) (*incrementalIntervalJoinEntry, bool) {
	if root == nil {
		return nil, false
	}
	if start != root.start || key != root.key {
		if start < root.start || (start == root.start && key < root.key) {
			var removed bool
			root.treeLeft, removed = incrementalIntervalJoinTreeDelete(root.treeLeft, start, key)
			if removed {
				incrementalIntervalJoinTreeUpdate(root)
			}
			return root, removed
		}
		var removed bool
		root.treeRight, removed = incrementalIntervalJoinTreeDelete(root.treeRight, start, key)
		if removed {
			incrementalIntervalJoinTreeUpdate(root)
		}
		return root, removed
	}
	return incrementalIntervalJoinTreeMerge(root.treeLeft, root.treeRight), true
}

func incrementalIntervalJoinTreeMerge(left, right *incrementalIntervalJoinEntry) *incrementalIntervalJoinEntry {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if left.treePriority < right.treePriority {
		left.treeRight = incrementalIntervalJoinTreeMerge(left.treeRight, right)
		incrementalIntervalJoinTreeUpdate(left)
		return left
	}
	right.treeLeft = incrementalIntervalJoinTreeMerge(left, right.treeLeft)
	incrementalIntervalJoinTreeUpdate(right)
	return right
}

func incrementalIntervalJoinTreeRotateRight(root *incrementalIntervalJoinEntry) *incrementalIntervalJoinEntry {
	newRoot := root.treeLeft
	root.treeLeft = newRoot.treeRight
	newRoot.treeRight = root
	incrementalIntervalJoinTreeUpdate(root)
	incrementalIntervalJoinTreeUpdate(newRoot)
	return newRoot
}

func incrementalIntervalJoinTreeRotateLeft(root *incrementalIntervalJoinEntry) *incrementalIntervalJoinEntry {
	newRoot := root.treeRight
	root.treeRight = newRoot.treeLeft
	newRoot.treeLeft = root
	incrementalIntervalJoinTreeUpdate(root)
	incrementalIntervalJoinTreeUpdate(newRoot)
	return newRoot
}

func incrementalIntervalJoinTreeUpdate(node *incrementalIntervalJoinEntry) {
	if node == nil {
		return
	}
	node.treeMaxEnd = node.end
	if node.treeLeft != nil && node.treeLeft.treeMaxEnd > node.treeMaxEnd {
		node.treeMaxEnd = node.treeLeft.treeMaxEnd
	}
	if node.treeRight != nil && node.treeRight.treeMaxEnd > node.treeMaxEnd {
		node.treeMaxEnd = node.treeRight.treeMaxEnd
	}
}

func incrementalIntervalJoinTreeOverlap(node *incrementalIntervalJoinEntry, start, end int64, result *[]*incrementalIntervalJoinEntry) {
	if node == nil || node.treeMaxEnd <= start {
		return
	}
	if node.treeLeft != nil {
		incrementalIntervalJoinTreeOverlap(node.treeLeft, start, end, result)
	}
	if node.start >= end {
		return
	}
	if node.end > start {
		*result = append(*result, node)
	}
	incrementalIntervalJoinTreeOverlap(node.treeRight, start, end, result)
}

func incrementalIntervalJoinTreeWalk(node *incrementalIntervalJoinEntry, visit func(*incrementalIntervalJoinEntry)) {
	if node == nil {
		return
	}
	incrementalIntervalJoinTreeWalk(node.treeLeft, visit)
	visit(node)
	incrementalIntervalJoinTreeWalk(node.treeRight, visit)
}

func validateIncrementalIntervalJoinSourceKey(key string) error {
	if key == "" {
		return ErrIncrementalIntervalJoinKeyRequired
	}
	if strings.IndexByte(key, 0) >= 0 {
		return ErrIncrementalIntervalJoinKeyInvalid
	}
	return nil
}

func intervalsOverlap(leftStart, leftEnd, rightStart, rightEnd int64) bool {
	return leftStart < rightEnd && rightStart < leftEnd
}

func addIncrementalIntervalJoinCount(count, diff int64) (int64, error) {
	if count < 0 {
		return 0, ErrIncrementalIntervalJoinMultiplicity
	}
	if diff >= 0 {
		if uint64(diff) > uint64(math.MaxInt64-count) {
			return 0, ErrIncrementalIntervalJoinOverflow
		}
		return count + diff, nil
	}
	magnitude := intervalJoinAbsInt64(diff)
	if magnitude > uint64(count) {
		return 0, ErrIncrementalIntervalJoinMultiplicity
	}
	return count - int64(magnitude), nil
}

func multiplyIncrementalIntervalJoinDelta(delta, count int64) (int64, error) {
	if delta == 0 || count == 0 {
		return 0, nil
	}
	deltaMagnitude := intervalJoinAbsInt64(delta)
	countMagnitude := intervalJoinAbsInt64(count)
	limit := uint64(math.MaxInt64)
	if delta < 0 {
		limit++
	}
	if deltaMagnitude > limit/countMagnitude {
		return 0, ErrIncrementalIntervalJoinOverflow
	}
	product := deltaMagnitude * countMagnitude
	if delta < 0 {
		if product == uint64(math.MaxInt64)+1 {
			return math.MinInt64, nil
		}
		return -int64(product), nil
	}
	return int64(product), nil
}

func intervalJoinAbsInt64(value int64) uint64 {
	if value >= 0 {
		return uint64(value)
	}
	return uint64(-(value + 1)) + 1
}

func cloneIncrementalIntervalJoinRow(row Row) Row {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = cloneIncrementalIntervalJoinValue(value)
	}
	return clone
}

func cloneIncrementalIntervalJoinValue(value any) any {
	switch typed := value.(type) {
	case Row:
		return cloneIncrementalIntervalJoinRow(typed)
	case map[string]any:
		clone := make(map[string]any, len(typed))
		for key, nested := range typed {
			clone[key] = cloneIncrementalIntervalJoinValue(nested)
		}
		return clone
	case []byte:
		return append([]byte(nil), typed...)
	case []any:
		clone := make([]any, len(typed))
		for index, nested := range typed {
			clone[index] = cloneIncrementalIntervalJoinValue(nested)
		}
		return clone
	default:
		return value
	}
}
