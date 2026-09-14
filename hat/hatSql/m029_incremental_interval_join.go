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
	key     string
	joinKey string
	start   int64
	end     int64
	row     Row
	count   int64
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
	entries      []*incrementalIntervalJoinEntry
	prefixMaxEnd []int64
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
		leftEntries := incrementalIntervalJoinActiveEntries(leftBucket.entries)
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
	bucket.remove(entry.key)
	if len(bucket.entries) == 0 {
		delete(buckets, entry.joinKey)
	}
}

func (bucket *incrementalIntervalJoinBucket) add(entry *incrementalIntervalJoinEntry) {
	index := sort.Search(len(bucket.entries), func(index int) bool {
		current := bucket.entries[index]
		return current.start > entry.start || (current.start == entry.start && current.key >= entry.key)
	})
	bucket.entries = append(bucket.entries, nil)
	copy(bucket.entries[index+1:], bucket.entries[index:])
	bucket.entries[index] = entry
	bucket.rebuildPrefixMaxEnd()
}

func (bucket *incrementalIntervalJoinBucket) remove(key string) {
	for index, entry := range bucket.entries {
		if entry.key != key {
			continue
		}
		copy(bucket.entries[index:], bucket.entries[index+1:])
		bucket.entries[len(bucket.entries)-1] = nil
		bucket.entries = bucket.entries[:len(bucket.entries)-1]
		bucket.rebuildPrefixMaxEnd()
		return
	}
}

func (bucket *incrementalIntervalJoinBucket) rebuildPrefixMaxEnd() {
	if len(bucket.entries) == 0 {
		bucket.prefixMaxEnd = nil
		return
	}
	bucket.prefixMaxEnd = make([]int64, len(bucket.entries))
	maxEnd := bucket.entries[0].end
	for index, entry := range bucket.entries {
		if entry.end > maxEnd {
			maxEnd = entry.end
		}
		bucket.prefixMaxEnd[index] = maxEnd
	}
}

func (bucket *incrementalIntervalJoinBucket) overlap(start, end int64) []*incrementalIntervalJoinEntry {
	if bucket == nil || len(bucket.entries) == 0 {
		return nil
	}
	limit := sort.Search(len(bucket.entries), func(index int) bool {
		return bucket.entries[index].start >= end
	})
	first := sort.Search(limit, func(index int) bool {
		return bucket.prefixMaxEnd[index] > start
	})
	result := make([]*incrementalIntervalJoinEntry, 0, limit-first)
	for index := first; index < limit; index++ {
		entry := bucket.entries[index]
		if entry.end > start {
			result = append(result, entry)
		}
	}
	return result
}

func incrementalIntervalJoinActiveEntries(entries []*incrementalIntervalJoinEntry) []*incrementalIntervalJoinEntry {
	active := make([]*incrementalIntervalJoinEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.count > 0 {
			active = append(active, entry)
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i].key < active[j].key })
	return active
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
