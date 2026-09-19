package hatSql

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
)

var (
	ErrDifferentialTemporalJoinInvalidInterval      = errors.New("hatSql: differential temporal join interval bounds are invalid")
	ErrDifferentialTemporalJoinNil                  = errors.New("hatSql: differential temporal join is nil")
	ErrDifferentialTemporalJoinLeftKeyRequired      = errors.New("hatSql: differential temporal join left key callback is required")
	ErrDifferentialTemporalJoinRightKeyRequired     = errors.New("hatSql: differential temporal join right key callback is required")
	ErrDifferentialTemporalJoinNegativeMultiplicity = errors.New("hatSql: differential temporal join multiplicity became negative")
	ErrDifferentialTemporalJoinCountOverflow        = errors.New("hatSql: differential temporal join multiplicity overflowed")
	ErrDifferentialTemporalJoinPairDiffOverflow     = errors.New("hatSql: differential temporal join pair diff overflowed")
	ErrDifferentialTemporalJoinFrontierRegression   = errors.New("hatSql: differential temporal join frontier moved backwards")
	ErrDifferentialTemporalJoinCompacted            = errors.New("hatSql: differential temporal join state was compacted")
)

// DifferentialTemporalJoinKeyFunc returns the equality key for one side of a
// temporal join. The callback is called only for a new positive row; negative
// updates use the row identity already retained by the join.
type DifferentialTemporalJoinKeyFunc func(SQLRow) string

// DifferentialTemporalJoinDefinition configures an exact differential
// equi-join. Rows match when their callback keys are equal and their timestamps
// differ by at most MaxTimeDistance, inclusive.
type DifferentialTemporalJoinDefinition struct {
	MinTimeDistance uint64
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

// DifferentialTemporalJoinCompactionStats reports one successful state
// compaction. A row is removable only after its own frontier seals its time
// and the counterpart frontier passes its maximum possible match time.
type DifferentialTemporalJoinCompactionStats struct {
	LeftFrontier  uint64
	RightFrontier uint64
	RemovedLeft   int
	RemovedRight  int
	RetainedLeft  int
	RetainedRight int
}

// DifferentialTemporalJoin incrementally maintains a weighted temporal inner
// join. ApplyLeft and ApplyRight emit signed joined-pair updates for the
// changes they apply. The join is safe for concurrent ApplyLeft/ApplyRight
// calls; each batch is serialized and invalid batches leave state unchanged.
type DifferentialTemporalJoin struct {
	mu              sync.Mutex
	minTimeDistance uint64
	maxTimeDistance uint64
	leftKey         DifferentialTemporalJoinKeyFunc
	rightKey        DifferentialTemporalJoinKeyFunc
	left            map[string]differentialTemporalJoinEntry
	right           map[string]differentialTemporalJoinEntry
	leftGroups      map[string][]string
	rightGroups     map[string][]string
	leftGroupKnown  map[differentialTemporalJoinGroupKey]struct{}
	rightGroupKnown map[differentialTemporalJoinGroupKey]struct{}
	leftFrontier    uint64
	rightFrontier   uint64
}

// NewDifferentialTemporalJoin creates an empty temporal join.
func NewDifferentialTemporalJoin(definition DifferentialTemporalJoinDefinition) (*DifferentialTemporalJoin, error) {
	if definition.LeftKey == nil {
		return nil, ErrDifferentialTemporalJoinLeftKeyRequired
	}
	if definition.RightKey == nil {
		return nil, ErrDifferentialTemporalJoinRightKeyRequired
	}
	if definition.MinTimeDistance > definition.MaxTimeDistance {
		return nil, ErrDifferentialTemporalJoinInvalidInterval
	}
	return &DifferentialTemporalJoin{
		minTimeDistance: definition.MinTimeDistance,
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

// Compact evicts rows that cannot match any future counterpart and are sealed
// by their own input frontier. Both frontiers are monotonic; a regression is
// rejected without changing state. Retractions for evicted keys return
// ErrDifferentialTemporalJoinCompacted instead of being treated as an unknown
// negative multiplicity.
func (join *DifferentialTemporalJoin) Compact(leftFrontier, rightFrontier uint64) (DifferentialTemporalJoinCompactionStats, error) {
	if join == nil {
		return DifferentialTemporalJoinCompactionStats{}, ErrDifferentialTemporalJoinNil
	}
	join.mu.Lock()
	defer join.mu.Unlock()
	if leftFrontier < join.leftFrontier || rightFrontier < join.rightFrontier {
		return DifferentialTemporalJoinCompactionStats{}, fmt.Errorf("frontiers %d/%d follow %d/%d: %w", leftFrontier, rightFrontier, join.leftFrontier, join.rightFrontier, ErrDifferentialTemporalJoinFrontierRegression)
	}
	stats := DifferentialTemporalJoinCompactionStats{
		LeftFrontier:  leftFrontier,
		RightFrontier: rightFrontier,
	}
	for key, entry := range join.left {
		if differentialTemporalJoinExpired(entry.time, leftFrontier, rightFrontier, join.maxTimeDistance) {
			delete(join.left, key)
			stats.RemovedLeft++
		}
	}
	for key, entry := range join.right {
		if differentialTemporalJoinExpired(entry.time, rightFrontier, leftFrontier, join.maxTimeDistance) {
			delete(join.right, key)
			stats.RemovedRight++
		}
	}
	differentialTemporalJoinRebuildGroups(join.left, join.leftGroups, join.leftGroupKnown)
	differentialTemporalJoinRebuildGroups(join.right, join.rightGroups, join.rightGroupKnown)
	join.leftFrontier = leftFrontier
	join.rightFrontier = rightFrontier
	stats.RetainedLeft = len(join.left)
	stats.RetainedRight = len(join.right)
	return stats, nil
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
			if change.Diff < 0 {
				frontier := join.leftFrontier
				if !leftSide {
					frontier = join.rightFrontier
				}
				if change.Time < frontier {
					return fmt.Errorf("key %q at time %d: %w", change.Key, change.Time, ErrDifferentialTemporalJoinCompacted)
				}
			}
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
			if !found || !differentialTemporalJoinTimesMatch(entry.time, counterpart.time, join.minTimeDistance, join.maxTimeDistance) {
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
			if !found || counterpart.groupKey != entry.groupKey || !differentialTemporalJoinTimesMatch(entry.time, counterpart.time, join.minTimeDistance, join.maxTimeDistance) {
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

func differentialTemporalJoinExpired(time, ownFrontier, counterpartFrontier, maxDistance uint64) bool {
	if ownFrontier <= time {
		return false
	}
	if maxDistance > ^uint64(0)-time {
		return false
	}
	return counterpartFrontier > time+maxDistance
}

func differentialTemporalJoinRebuildGroups(side map[string]differentialTemporalJoinEntry, groups map[string][]string, known map[differentialTemporalJoinGroupKey]struct{}) {
	filtered := make(map[string][]string, len(groups))
	seen := make(map[string]struct{}, len(side))
	for group, keys := range groups {
		for _, key := range keys {
			entry, exists := side[key]
			if !exists || entry.groupKey != group {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			filtered[group] = append(filtered[group], key)
			seen[key] = struct{}{}
		}
	}
	for key, entry := range side {
		if _, exists := seen[key]; exists {
			continue
		}
		filtered[entry.groupKey] = append(filtered[entry.groupKey], key)
	}
	for group := range groups {
		delete(groups, group)
	}
	for key := range known {
		delete(known, key)
	}
	for group, keys := range filtered {
		groups[group] = keys
		for _, key := range keys {
			known[differentialTemporalJoinGroupKey{group: group, key: key}] = struct{}{}
		}
	}
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

func differentialTemporalJoinTimesMatch(left, right, minDistance, maxDistance uint64) bool {
	var distance uint64
	if left >= right {
		distance = left - right
	} else {
		distance = right - left
	}
	return distance >= minDistance && distance <= maxDistance
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
