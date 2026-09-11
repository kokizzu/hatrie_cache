package hatSql

import (
	"errors"
	"fmt"
	"math"
)

var (
	// ErrDifferentialIntersectNegativeMultiplicity reports an update that
	// would make one input relation's multiplicity negative.
	ErrDifferentialIntersectNegativeMultiplicity = errors.New("hatSql: differential intersect multiplicity became negative")
	// ErrDifferentialIntersectCountOverflow reports a multiplicity that cannot
	// be represented by the operator's uint64 state.
	ErrDifferentialIntersectCountOverflow = errors.New("hatSql: differential intersect multiplicity overflowed")
	// ErrDifferentialIntersectDiffOverflow reports an intersection transition
	// that cannot be represented by an int64 output weight.
	ErrDifferentialIntersectDiffOverflow = errors.New("hatSql: differential intersect output diff overflowed")
	// ErrDifferentialIntersectInputOverflow reports input lengths that cannot
	// be represented by an in-memory batch capacity.
	ErrDifferentialIntersectInputOverflow = errors.New("hatSql: differential intersect input length overflowed")
	// ErrNilDifferentialIntersect reports a method call on a nil operator.
	ErrNilDifferentialIntersect = errors.New("hatSql: nil differential intersect")
)

type differentialIntersectState struct {
	left  uint64
	right uint64
	row   Row
}

type differentialIntersectCounts struct {
	left  uint64
	right uint64
}

// DifferentialIntersect incrementally maintains the multiset intersection of
// two input streams. Apply emits only changes to min(left multiplicity, right
// multiplicity), with the left row payload used for output transitions.
//
// Apply processes the left batch before the right batch. The operator is not
// safe for concurrent use; synchronize calls when a stream has multiple
// producers. Reset discards all retained multiplicity and payload state.
type DifferentialIntersect struct {
	entries map[string]differentialIntersectState
}

// NewDifferentialIntersect returns an empty incremental multiset intersection
// operator.
func NewDifferentialIntersect() *DifferentialIntersect {
	return &DifferentialIntersect{
		entries: make(map[string]differentialIntersectState),
	}
}

// Reset removes all input multiplicities and retained left-side row payloads.
func (operator *DifferentialIntersect) Reset() {
	if operator == nil {
		return
	}
	for key := range operator.entries {
		delete(operator.entries, key)
	}
}

// Apply validates and applies one batch from each side. Validation is staged
// against a small count overlay so an invalid batch cannot partially mutate
// the operator or emit a partial result.
func (operator *DifferentialIntersect) Apply(left, right []DifferentialRow) ([]DifferentialRow, error) {
	if operator == nil {
		return nil, ErrNilDifferentialIntersect
	}
	capacity, err := differentialIntersectInputCapacity(len(left), len(right))
	if err != nil {
		return nil, err
	}
	if capacity == 0 {
		return nil, nil
	}

	working := make(map[string]differentialIntersectCounts, capacity)
	if err := operator.validateSide(working, left, true); err != nil {
		return nil, err
	}
	if err := operator.validateSide(working, right, false); err != nil {
		return nil, err
	}

	if operator.entries == nil {
		operator.entries = make(map[string]differentialIntersectState, capacity)
	}
	result := make([]DifferentialRow, 0, capacity)
	operator.applySide(left, true, &result)
	operator.applySide(right, false, &result)
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func differentialIntersectInputCapacity(left, right int) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if right > maxInt-left {
		return 0, ErrDifferentialIntersectInputOverflow
	}
	return left + right, nil
}

func (operator *DifferentialIntersect) validateSide(working map[string]differentialIntersectCounts, updates []DifferentialRow, leftSide bool) error {
	for _, update := range updates {
		if update.Key == "" {
			return ErrDifferentialRowKeyRequired
		}
		if update.Diff == 0 {
			continue
		}
		counts, exists := working[update.Key]
		if !exists {
			state := operator.entries[update.Key]
			counts = differentialIntersectCounts{left: state.left, right: state.right}
		}
		before := differentialIntersectMinimum(counts.left, counts.right)
		if leftSide {
			next, err := nextDifferentialDistinctMultiplicity(counts.left, update.Diff)
			if err != nil {
				return differentialIntersectCountError("left", update.Key, err)
			}
			counts.left = next
		} else {
			next, err := nextDifferentialDistinctMultiplicity(counts.right, update.Diff)
			if err != nil {
				return differentialIntersectCountError("right", update.Key, err)
			}
			counts.right = next
		}
		after := differentialIntersectMinimum(counts.left, counts.right)
		if _, ok := differentialIntersectDelta(before, after); !ok {
			return fmt.Errorf("%s key %q: %w", differentialIntersectSideName(leftSide), update.Key, ErrDifferentialIntersectDiffOverflow)
		}
		working[update.Key] = counts
	}
	return nil
}

func differentialIntersectCountError(side, key string, err error) error {
	if errors.Is(err, ErrDifferentialDistinctNegativeMultiplicity) {
		return fmt.Errorf("%s key %q: %w", side, key, ErrDifferentialIntersectNegativeMultiplicity)
	}
	if errors.Is(err, ErrDifferentialDistinctOverflow) {
		return fmt.Errorf("%s key %q: %w", side, key, ErrDifferentialIntersectCountOverflow)
	}
	return fmt.Errorf("%s key %q: %w", side, key, err)
}

func (operator *DifferentialIntersect) applySide(updates []DifferentialRow, leftSide bool, result *[]DifferentialRow) {
	for _, update := range updates {
		if update.Diff == 0 {
			continue
		}
		state := operator.entries[update.Key]
		before := differentialIntersectMinimum(state.left, state.right)
		if leftSide {
			next, _ := nextDifferentialDistinctMultiplicity(state.left, update.Diff)
			if state.left == 0 && next > 0 {
				state.row = cloneDifferentialRow(update.Row)
			}
			state.left = next
		} else {
			next, _ := nextDifferentialDistinctMultiplicity(state.right, update.Diff)
			state.right = next
		}
		after := differentialIntersectMinimum(state.left, state.right)
		delta, _ := differentialIntersectDelta(before, after)
		if delta != 0 {
			*result = append(*result, DifferentialRow{
				Key:  update.Key,
				Time: update.Time,
				Diff: delta,
				Row:  cloneDifferentialRow(state.row),
			})
		}
		if state.left == 0 && state.right == 0 {
			delete(operator.entries, update.Key)
		} else {
			operator.entries[update.Key] = state
		}
	}
}

func differentialIntersectMinimum(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}

func differentialIntersectDelta(before, after uint64) (int64, bool) {
	if after >= before {
		delta := after - before
		if delta > math.MaxInt64 {
			return 0, false
		}
		return int64(delta), true
	}
	delta := before - after
	if delta > math.MaxInt64 {
		return 0, false
	}
	return -int64(delta), true
}

func differentialIntersectSideName(leftSide bool) string {
	if leftSide {
		return "left"
	}
	return "right"
}
