package hatSql

import (
	"fmt"
	"math"
	"sort"
)

// TypedTableJoinDelta is one differential change to a maintained join result.
// Diff is positive for an inserted result row and negative for a retracted
// result row. Left and Right are independent snapshots owned by the delta.
type TypedTableJoinDelta struct {
	LeftKey  string            `json:"left_key"`
	RightKey string            `json:"right_key"`
	Left     []TypedTableValue `json:"left"`
	Right    []TypedTableValue `json:"right"`
	Diff     int64             `json:"diff"`
}

// ApplyLeftDeltas applies left changes and returns only the join-result rows
// affected by those changes. The maintained join state is updated exactly as
// by ApplyLeft. If a batch partially applies before an error, returned deltas
// describe the applied prefix and the error is returned as well.
func (join *TypedTableJoin) ApplyLeftDeltas(changes []TypedTableChange) ([]TypedTableJoinDelta, error) {
	if join == nil {
		return nil, fmt.Errorf("typed table join is nil")
	}
	join.mu.Lock()
	defer join.mu.Unlock()
	before := join.captureLeftDeltaStateLocked(changes)
	err := join.applyLeftChangesLocked(changes)
	after := join.captureLeftDeltaStateLocked(changes)
	return typedTableJoinDeltaDiff(before, after), err
}

// ApplyRightDeltas applies right changes and returns only the join-result rows
// affected by those changes. The maintained join state is updated exactly as
// by ApplyRight. If a batch partially applies before an error, returned deltas
// describe the applied prefix and the error is returned as well.
func (join *TypedTableJoin) ApplyRightDeltas(changes []TypedTableChange) ([]TypedTableJoinDelta, error) {
	if join == nil {
		return nil, fmt.Errorf("typed table join is nil")
	}
	join.mu.Lock()
	defer join.mu.Unlock()
	before := join.captureRightDeltaStateLocked(changes)
	err := join.applyRightChangesLocked(changes)
	after := join.captureRightDeltaStateLocked(changes)
	return typedTableJoinDeltaDiff(before, after), err
}

func (join *TypedTableJoin) captureLeftDeltaStateLocked(changes []TypedTableChange) map[typedTableJoinPair]TypedTableJoinDelta {
	keys := typedTableJoinChangedKeys(changes)
	state := make(map[typedTableJoinPair]TypedTableJoinDelta)
	for key := range keys {
		values, found := join.leftRows[key]
		if !found {
			continue
		}
		valueKey, joined := typedTableJoinValue(values, join.leftField)
		if !joined {
			continue
		}
		for rightKey := range join.rightIndex[valueKey] {
			right, rightFound := join.rightRows[rightKey]
			if !rightFound {
				continue
			}
			pair := typedTableJoinPair{leftKey: key, rightKey: rightKey}
			state[pair] = TypedTableJoinDelta{
				LeftKey: key, RightKey: rightKey,
				Left: cloneTypedTableValues(values), Right: cloneTypedTableValues(right),
			}
		}
	}
	return state
}

func (join *TypedTableJoin) captureRightDeltaStateLocked(changes []TypedTableChange) map[typedTableJoinPair]TypedTableJoinDelta {
	keys := typedTableJoinChangedKeys(changes)
	state := make(map[typedTableJoinPair]TypedTableJoinDelta)
	for key := range keys {
		values, found := join.rightRows[key]
		if !found {
			continue
		}
		valueKey, joined := typedTableJoinValue(values, join.rightField)
		if !joined {
			continue
		}
		for leftKey := range join.leftIndex[valueKey] {
			left, leftFound := join.leftRows[leftKey]
			if !leftFound {
				continue
			}
			pair := typedTableJoinPair{leftKey: leftKey, rightKey: key}
			state[pair] = TypedTableJoinDelta{
				LeftKey: leftKey, RightKey: key,
				Left: cloneTypedTableValues(left), Right: cloneTypedTableValues(values),
			}
		}
	}
	return state
}

func typedTableJoinChangedKeys(changes []TypedTableChange) map[string]struct{} {
	if len(changes) == 0 {
		return nil
	}
	keys := make(map[string]struct{}, len(changes))
	for _, change := range changes {
		if change.Key != "" {
			keys[change.Key] = struct{}{}
		}
	}
	return keys
}

func typedTableJoinDeltaDiff(before, after map[typedTableJoinPair]TypedTableJoinDelta) []TypedTableJoinDelta {
	if len(before) == 0 && len(after) == 0 {
		return nil
	}
	deltas := make([]TypedTableJoinDelta, 0, len(before)+len(after))
	for pair, previous := range before {
		current, found := after[pair]
		if found && typedTableJoinDeltaRowsEqual(previous, current) {
			continue
		}
		previous.Diff = -1
		deltas = append(deltas, previous)
	}
	for pair, current := range after {
		previous, found := before[pair]
		if found && typedTableJoinDeltaRowsEqual(previous, current) {
			continue
		}
		current.Diff = 1
		deltas = append(deltas, current)
	}
	sort.Slice(deltas, func(left, right int) bool {
		if deltas[left].LeftKey != deltas[right].LeftKey {
			return deltas[left].LeftKey < deltas[right].LeftKey
		}
		if deltas[left].RightKey != deltas[right].RightKey {
			return deltas[left].RightKey < deltas[right].RightKey
		}
		return deltas[left].Diff < deltas[right].Diff
	})
	return deltas
}

func typedTableJoinDeltaRowsEqual(left, right TypedTableJoinDelta) bool {
	return typedTableJoinValuesEqual(left.Left, right.Left) && typedTableJoinValuesEqual(left.Right, right.Right)
}

func typedTableJoinValuesEqual(left, right []TypedTableValue) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Valid != right[index].Valid || left[index].Kind != right[index].Kind ||
			left[index].String != right[index].String || left[index].Int64 != right[index].Int64 ||
			left[index].Bool != right[index].Bool || math.Float64bits(left[index].Float64) != math.Float64bits(right[index].Float64) {
			return false
		}
	}
	return true
}
