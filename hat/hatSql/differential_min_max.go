package hatSql

import (
	"errors"
	"fmt"
)

var (
	ErrDifferentialGroupByValueMultiplicity  = errors.New("hatSql: differential group-by value multiplicity became negative")
	ErrDifferentialGroupByValueCountOverflow = errors.New("hatSql: differential group-by value count overflowed")
)

type differentialGroupMinMaxState struct {
	count  int64
	values map[int64]int64
	min    int64
	max    int64
}

// DifferentialStringValueFunc extracts a string aggregate value from one
// differential row.
type DifferentialStringValueFunc func(SQLRow) (string, error)

type differentialGroupStringMinMaxState struct {
	count  int64
	values map[string]int64
	min    string
	max    string
}

// GroupMinMaxInt64DifferentialRows maintains grouped int64 MIN and MAX values
// from signed differential updates. Value multiplicities are retained so a
// retraction can restore the next endpoint exactly. It emits an insertion for
// a new group, a retraction when a group disappears, and a retraction followed
// by an insertion only when the visible aggregate changes.
//
// The function is batch-scoped, preserves input order and update timestamps,
// does not mutate input rows, and returns no partial output on invalid counts,
// callback errors, or value multiplicity failures.
func GroupMinMaxInt64DifferentialRows(rows []DifferentialRow, groupKey DifferentialGroupByKeyFunc, value DifferentialInt64ValueFunc) ([]DifferentialRow, error) {
	if groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if value == nil {
		return nil, ErrDifferentialGroupByValueRequired
	}
	if len(rows) == 0 {
		return nil, nil
	}

	states := make(map[string]differentialGroupMinMaxState, len(rows))
	emitted := make([]DifferentialRow, 0, differentialMinMaxOutputCapacity(len(rows)))
	for _, update := range rows {
		if update.Diff == 0 {
			continue
		}
		key := groupKey(update.Row)
		rowValue, err := value(update.Row)
		if err != nil {
			return nil, fmt.Errorf("group %q: differential value: %w", key, err)
		}

		current := states[key]
		nextCount, ok := addDifferentialCounts(current.count, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByCountOverflow)
		}
		if nextCount < 0 {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByNegativeCount)
		}

		currentValueCount := current.values[rowValue]
		nextValueCount, ok := addDifferentialCounts(currentValueCount, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group %q value %d: %w", key, rowValue, ErrDifferentialGroupByValueCountOverflow)
		}
		if nextValueCount < 0 {
			return nil, fmt.Errorf("group %q value %d: %w", key, rowValue, ErrDifferentialGroupByValueMultiplicity)
		}

		previous := current
		if current.values == nil {
			current.values = make(map[int64]int64)
		}
		if nextValueCount == 0 {
			delete(current.values, rowValue)
		} else {
			current.values[rowValue] = nextValueCount
		}

		if nextCount == 0 {
			delete(states, key)
		} else {
			current.count = nextCount
			current.min, current.max = differentialNextMinMax(current, previous, rowValue, update.Diff, nextValueCount)
			states[key] = current
		}

		switch {
		case previous.count == 0:
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: differentialMinMaxRow(current.min, current.max)})
		case nextCount == 0:
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: differentialMinMaxRow(previous.min, previous.max)})
		case previous.min != current.min || previous.max != current.max:
			emitted = append(emitted,
				DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: differentialMinMaxRow(previous.min, previous.max)},
				DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: differentialMinMaxRow(current.min, current.max)},
			)
		}
	}
	if len(emitted) == 0 {
		return nil, nil
	}
	return emitted, nil
}

// GroupMinMaxStringDifferentialRows maintains grouped lexicographic MIN and
// MAX values from signed differential updates. Value multiplicities are
// retained so a retraction can restore the next endpoint exactly. Empty
// strings are valid values and remain distinct from an absent group.
//
// The function is batch-scoped, preserves input order and update timestamps,
// does not mutate input rows, and returns no partial output on invalid counts,
// callback errors, or value multiplicity failures.
func GroupMinMaxStringDifferentialRows(rows []DifferentialRow, groupKey DifferentialGroupByKeyFunc, value DifferentialStringValueFunc) ([]DifferentialRow, error) {
	if groupKey == nil {
		return nil, ErrDifferentialGroupByKeyRequired
	}
	if value == nil {
		return nil, ErrDifferentialGroupByValueRequired
	}
	if len(rows) == 0 {
		return nil, nil
	}

	states := make(map[string]differentialGroupStringMinMaxState, len(rows))
	emitted := make([]DifferentialRow, 0, differentialMinMaxOutputCapacity(len(rows)))
	for _, update := range rows {
		if update.Diff == 0 {
			continue
		}
		key := groupKey(update.Row)
		rowValue, err := value(update.Row)
		if err != nil {
			return nil, fmt.Errorf("group %q: differential value: %w", key, err)
		}

		current := states[key]
		nextCount, ok := addDifferentialCounts(current.count, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByCountOverflow)
		}
		if nextCount < 0 {
			return nil, fmt.Errorf("group %q: %w", key, ErrDifferentialGroupByNegativeCount)
		}

		currentValueCount := current.values[rowValue]
		nextValueCount, ok := addDifferentialCounts(currentValueCount, update.Diff)
		if !ok {
			return nil, fmt.Errorf("group %q value %q: %w", key, rowValue, ErrDifferentialGroupByValueCountOverflow)
		}
		if nextValueCount < 0 {
			return nil, fmt.Errorf("group %q value %q: %w", key, rowValue, ErrDifferentialGroupByValueMultiplicity)
		}

		previous := current
		if current.values == nil {
			current.values = make(map[string]int64)
		}
		if nextValueCount == 0 {
			delete(current.values, rowValue)
		} else {
			current.values[rowValue] = nextValueCount
		}

		if nextCount == 0 {
			delete(states, key)
		} else {
			current.count = nextCount
			current.min, current.max = differentialNextStringMinMax(current, previous, rowValue, update.Diff, nextValueCount)
			states[key] = current
		}

		switch {
		case previous.count == 0:
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: differentialStringMinMaxRow(current.min, current.max)})
		case nextCount == 0:
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: differentialStringMinMaxRow(previous.min, previous.max)})
		case previous.min != current.min || previous.max != current.max:
			emitted = append(emitted,
				DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: differentialStringMinMaxRow(previous.min, previous.max)},
				DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: differentialStringMinMaxRow(current.min, current.max)},
			)
		}
	}
	if len(emitted) == 0 {
		return nil, nil
	}
	return emitted, nil
}

func differentialNextMinMax(current, previous differentialGroupMinMaxState, rowValue, diff int64, nextValueCount int64) (int64, int64) {
	if previous.count == 0 {
		return rowValue, rowValue
	}
	minValue, maxValue := previous.min, previous.max
	if diff > 0 {
		if rowValue < minValue {
			minValue = rowValue
		}
		if rowValue > maxValue {
			maxValue = rowValue
		}
		return minValue, maxValue
	}
	if nextValueCount > 0 || (rowValue != previous.min && rowValue != previous.max) {
		return minValue, maxValue
	}
	return differentialScanMinMax(current.values)
}

func differentialScanMinMax(values map[int64]int64) (int64, int64) {
	first := true
	var minValue, maxValue int64
	for value, count := range values {
		if count <= 0 {
			continue
		}
		if first || value < minValue {
			minValue = value
		}
		if first || value > maxValue {
			maxValue = value
		}
		first = false
	}
	return minValue, maxValue
}

func differentialNextStringMinMax(current, previous differentialGroupStringMinMaxState, rowValue string, diff int64, nextValueCount int64) (string, string) {
	if previous.count == 0 {
		return rowValue, rowValue
	}
	minValue, maxValue := previous.min, previous.max
	if diff > 0 {
		if rowValue < minValue {
			minValue = rowValue
		}
		if rowValue > maxValue {
			maxValue = rowValue
		}
		return minValue, maxValue
	}
	if nextValueCount > 0 || (rowValue != previous.min && rowValue != previous.max) {
		return minValue, maxValue
	}
	return differentialScanStringMinMax(current.values)
}

func differentialScanStringMinMax(values map[string]int64) (string, string) {
	first := true
	var minValue, maxValue string
	for value, count := range values {
		if count <= 0 {
			continue
		}
		if first || value < minValue {
			minValue = value
		}
		if first || value > maxValue {
			maxValue = value
		}
		first = false
	}
	return minValue, maxValue
}

func differentialMinMaxRow(minValue, maxValue int64) Row {
	return Row{"min": minValue, "max": maxValue}
}

func differentialStringMinMaxRow(minValue, maxValue string) Row {
	return Row{"min": minValue, "max": maxValue}
}

func differentialMinMaxOutputCapacity(length int) int {
	if length > int(^uint(0)>>1)/2 {
		return length
	}
	return length * 2
}
