package hatSql

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
)

var (
	ErrDifferentialTemporalJoinNil                  = errors.New("hatSql: differential temporal join is nil")
	ErrDifferentialTemporalJoinLeftKeyRequired      = errors.New("hatSql: differential temporal join left key callback is required")
	ErrDifferentialTemporalJoinRightKeyRequired     = errors.New("hatSql: differential temporal join right key callback is required")
	ErrDifferentialTemporalJoinNegativeMultiplicity = errors.New("hatSql: differential temporal join multiplicity became negative")
	ErrDifferentialTemporalJoinCountOverflow        = errors.New("hatSql: differential temporal join multiplicity overflowed")
	ErrDifferentialTemporalJoinPairDiffOverflow     = errors.New("hatSql: differential temporal join pair diff overflowed")
)

// DifferentialTemporalJoinKeyFunc returns the equality key for one side of a
// temporal join. The callback is called only for a new positive row; negative
// updates use the row identity already retained by the join.
type DifferentialTemporalJoinKeyFunc func(SQLRow) string

// DifferentialTemporalJoinDefinition configures an exact differential
// equi-join. Rows match when their callback keys are equal and their timestamps
// differ by at most MaxTimeDistance, inclusive.
type DifferentialTemporalJoinDefinition struct {
	MaxTimeDistance uint64
	LeftKey         DifferentialTemporalJoinKeyFunc
	RightKey        DifferentialTemporalJoinKeyFunc
}

type differentialTemporalJoinEntry struct {
	key      string
	groupKey string
	time     uint64
	count    int64
	row      Row
}

type differentialTemporalJoinGroupKey struct {
	group string
	key   string
}

// DifferentialTemporalJoin incrementally maintains a weighted temporal inner
// join. ApplyLeft and ApplyRight emit signed joined-pair updates for the
// changes they apply. The join is safe for concurrent ApplyLeft/ApplyRight
// calls; each batch is serialized and invalid batches leave state unchanged.
type DifferentialTemporalJoin struct {
	mu              sync.Mutex
	maxTimeDistance uint64
	leftKey         DifferentialTemporalJoinKeyFunc
	rightKey        DifferentialTemporalJoinKeyFunc
	left            map[string]differentialTemporalJoinEntry
	right           map[string]differentialTemporalJoinEntry
	leftGroups      map[string][]string
	rightGroups     map[string][]string
	leftGroupKnown  map[differentialTemporalJoinGroupKey]struct{}
	rightGroupKnown map[differentialTemporalJoinGroupKey]struct{}
}

// NewDifferentialTemporalJoin creates an empty temporal join.
func NewDifferentialTemporalJoin(definition DifferentialTemporalJoinDefinition) (*DifferentialTemporalJoin, error) {
	if definition.LeftKey == nil {
		return nil, ErrDifferentialTemporalJoinLeftKeyRequired
	}
	if definition.RightKey == nil {
		return nil, ErrDifferentialTemporalJoinRightKeyRequired
	}
	return &DifferentialTemporalJoin{
		maxTimeDistance: definition.MaxTimeDistance,
		leftKey:         definition.LeftKey,
		rightKey:        definition.RightKey,
		left:            make(map[string]differentialTemporalJoinEntry),
		right:           make(map[string]differentialTemporalJoinEntry),
		leftGroups:      make(map[string][]string),
		rightGroups:     make(map[string][]string),
		leftGroupKnown:  make(map[differentialTemporalJoinGroupKey]struct{}),
		rightGroupKnown: make(map[differentialTemporalJoinGroupKey]struct{}),
	}, nil
}

// ApplyLeft applies weighted changes to the left input and returns matching
// joined-pair changes in deterministic first-seen counterpart order.
func (join *DifferentialTemporalJoin) ApplyLeft(changes []DifferentialRow) ([]DifferentialRow, error) {
	if join == nil {
		return nil, ErrDifferentialTemporalJoinNil
	}
	join.mu.Lock()
	defer join.mu.Unlock()
	if err := join.validateChanges(changes, true); err != nil {
		return nil, err
	}
	return join.applyChanges(changes, true), nil
}

// ApplyRight applies weighted changes to the right input and returns matching
// joined-pair changes in deterministic first-seen counterpart order.
func (join *DifferentialTemporalJoin) ApplyRight(changes []DifferentialRow) ([]DifferentialRow, error) {
	if join == nil {
		return nil, ErrDifferentialTemporalJoinNil
	}
	join.mu.Lock()
	defer join.mu.Unlock()
	if err := join.validateChanges(changes, false); err != nil {
		return nil, err
	}
	return join.applyChanges(changes, false), nil
}

func (join *DifferentialTemporalJoin) validateChanges(changes []DifferentialRow, leftSide bool) error {
	if len(changes) == 0 {
		return nil
	}
	side := join.left
	other := join.right
	otherGroups := join.rightGroups
	keyFunc := join.leftKey
	if !leftSide {
		side = join.right
		other = join.left
		otherGroups = join.leftGroups
		keyFunc = join.rightKey
	}
	working := make(map[string]differentialTemporalJoinEntry, len(changes))
	for _, change := range changes {
		if change.Diff == 0 {
			continue
		}
		entry, exists := working[change.Key]
		if !exists {
			entry = side[change.Key]
		}
		current := entry.count
		next, ok := addDifferentialCounts(current, change.Diff)
		if !ok {
			return fmt.Errorf("key %q: %w", change.Key, ErrDifferentialTemporalJoinCountOverflow)
		}
		if next < 0 {
			return fmt.Errorf("key %q: %w", change.Key, ErrDifferentialTemporalJoinNegativeMultiplicity)
		}
		if change.Diff > 0 && current == 0 {
			entry = differentialTemporalJoinEntry{
				key:      change.Key,
				groupKey: keyFunc(change.Row),
				time:     change.Time,
				row:      change.Row,
			}
		}
		for _, counterpartKey := range otherGroups[entry.groupKey] {
			counterpart, found := other[counterpartKey]
			if !found || !differentialTemporalJoinTimesMatch(entry.time, counterpart.time, join.maxTimeDistance) {
				continue
			}
			if _, ok := multiplyDifferentialCounts(change.Diff, counterpart.count); !ok {
				return fmt.Errorf("key %q with counterpart %q: %w", change.Key, counterpart.key, ErrDifferentialTemporalJoinPairDiffOverflow)
			}
		}
		entry.count = next
		working[change.Key] = entry
	}
	return nil
}

func (join *DifferentialTemporalJoin) applyChanges(changes []DifferentialRow, leftSide bool) []DifferentialRow {
	side := join.left
	other := join.right
	sideGroups := join.leftGroups
	sideGroupKnown := join.leftGroupKnown
	counterpartGroups := join.rightGroups
	keyFunc := join.leftKey
	if !leftSide {
		side = join.right
		other = join.left
		sideGroups = join.rightGroups
		sideGroupKnown = join.rightGroupKnown
		counterpartGroups = join.leftGroups
		keyFunc = join.rightKey
	}
	emitted := make([]DifferentialRow, 0, len(changes))
	for _, change := range changes {
		if change.Diff == 0 {
			continue
		}
		entry, exists := side[change.Key]
		if !exists || entry.count == 0 {
			entry = differentialTemporalJoinEntry{
				key:      change.Key,
				groupKey: keyFunc(change.Row),
				time:     change.Time,
				row:      cloneDifferentialRow(change.Row),
			}
		}
		for _, counterpartKey := range counterpartGroups[entry.groupKey] {
			counterpart, found := other[counterpartKey]
			if !found || counterpart.groupKey != entry.groupKey || !differentialTemporalJoinTimesMatch(entry.time, counterpart.time, join.maxTimeDistance) {
				continue
			}
			diff, _ := multiplyDifferentialCounts(change.Diff, counterpart.count)
			if leftSide {
				emitted = append(emitted, differentialTemporalJoinUpdate(entry, counterpart, diff))
			} else {
				emitted = append(emitted, differentialTemporalJoinUpdate(counterpart, entry, diff))
			}
		}
		next, _ := addDifferentialCounts(entry.count, change.Diff)
		if next == 0 {
			delete(side, change.Key)
			continue
		}
		entry.count = next
		side[change.Key] = entry
		groupKey := differentialTemporalJoinGroupKey{group: entry.groupKey, key: entry.key}
		if _, seen := sideGroupKnown[groupKey]; !seen {
			sideGroupKnown[groupKey] = struct{}{}
			sideGroups[entry.groupKey] = append(sideGroups[entry.groupKey], entry.key)
		}
	}
	if len(emitted) == 0 {
		return nil
	}
	return emitted
}

func differentialTemporalJoinUpdate(left, right differentialTemporalJoinEntry, diff int64) DifferentialRow {
	return DifferentialRow{
		Key:  differentialTemporalJoinPairKey(left.key, right.key),
		Time: differentialTemporalJoinMaxTime(left.time, right.time),
		Diff: diff,
		Row:  differentialTemporalJoinRow(left.row, right.row),
	}
}

func differentialTemporalJoinPairKey(left, right string) string {
	return strconv.Itoa(len(left)) + ":" + left + strconv.Itoa(len(right)) + ":" + right
}

func differentialTemporalJoinRow(left, right Row) Row {
	row := make(Row, len(left)+len(right))
	for key, value := range left {
		row["left."+key] = differentialTemporalJoinValueClone(value)
	}
	for key, value := range right {
		row["right."+key] = differentialTemporalJoinValueClone(value)
	}
	return row
}

func differentialTemporalJoinValueClone(value interface{}) interface{} {
	bytes, ok := value.([]byte)
	if !ok {
		return value
	}
	cloned := make([]byte, len(bytes))
	copy(cloned, bytes)
	return cloned
}

func differentialTemporalJoinTimesMatch(left, right, maxDistance uint64) bool {
	if left >= right {
		return left-right <= maxDistance
	}
	return right-left <= maxDistance
}

func differentialTemporalJoinMaxTime(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}

func multiplyDifferentialCounts(left, right int64) (int64, bool) {
	if left == 0 || right == 0 {
		return 0, true
	}
	switch {
	case left > 0 && right > 0:
		if left > math.MaxInt64/right {
			return 0, false
		}
	case left > 0 && right < 0:
		if right < math.MinInt64/left {
			return 0, false
		}
	case left < 0 && right > 0:
		if left < math.MinInt64/right {
			return 0, false
		}
	case left < 0 && right < 0:
		if left < math.MaxInt64/right {
			return 0, false
		}
	}
	return left * right, true
}
