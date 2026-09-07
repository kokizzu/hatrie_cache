package hatSql

import (
	"errors"
	"fmt"
	"math"
)

var (
	// ErrTypedTableMergeJoinVisitor reports a missing output callback.
	ErrTypedTableMergeJoinVisitor = errors.New("typed table merge join visitor is nil")
	// ErrTypedTableMergeJoinField reports an invalid join-field position.
	ErrTypedTableMergeJoinField = errors.New("typed table merge join field is invalid")
	// ErrTypedTableMergeJoinTypeMismatch reports inconsistent key kinds.
	ErrTypedTableMergeJoinTypeMismatch = errors.New("typed table merge join key kinds differ")
	// ErrTypedTableMergeJoinUnsorted reports an input that is not ordered by its
	// non-NULL, non-NaN join values.
	ErrTypedTableMergeJoinUnsorted = errors.New("typed table merge join input is unsorted")
)

// TypedTableMergeJoinInput is one row for MergeSortedTypedTableJoin. Each
// input must be ordered by the selected join-field value. Key is copied to
// the emitted TypedTableJoinRow and is otherwise opaque to the merge join.
type TypedTableMergeJoinInput struct {
	Key    string
	Values []TypedTableValue
}

// MergeSortedTypedTableJoin emits exact inner-join matches from two inputs
// ordered by their selected join fields. It uses a merge cursor rather than a
// hash index and invokes visit as each match is found, so result storage stays
// with the caller. NULL and NaN keys do not match, as in SQL equality.
//
// The inputs and their Values slices are never modified. Emitted values are
// cloned before visit returns so a callback can retain or mutate them.
func MergeSortedTypedTableJoin(left, right []TypedTableMergeJoinInput, leftField, rightField int, visit func(TypedTableJoinRow) error) error {
	if visit == nil {
		return ErrTypedTableMergeJoinVisitor
	}
	if leftField < 0 || rightField < 0 {
		return fmt.Errorf("%w: left=%d right=%d", ErrTypedTableMergeJoinField, leftField, rightField)
	}
	leftKind, leftKnown, err := validateTypedTableMergeJoinInput(left, leftField)
	if err != nil {
		return err
	}
	rightKind, rightKnown, err := validateTypedTableMergeJoinInput(right, rightField)
	if err != nil {
		return err
	}
	if leftKnown && rightKnown && leftKind != rightKind {
		return fmt.Errorf("%w: left=%d right=%d", ErrTypedTableMergeJoinTypeMismatch, leftKind, rightKind)
	}

	for leftIndex, rightIndex := 0, 0; leftIndex < len(left) && rightIndex < len(right); {
		leftValue, leftValid := typedTableMergeJoinValue(left[leftIndex].Values, leftField)
		if !leftValid {
			leftIndex++
			continue
		}
		rightValue, rightValid := typedTableMergeJoinValue(right[rightIndex].Values, rightField)
		if !rightValid {
			rightIndex++
			continue
		}

		switch compareTypedTableMergeJoinValues(leftValue, rightValue) {
		case -1:
			leftIndex++
			continue
		case 1:
			rightIndex++
			continue
		}

		leftEnd := typedTableMergeJoinEqualRunEnd(left, leftIndex, leftField, leftValue)
		rightEnd := typedTableMergeJoinEqualRunEnd(right, rightIndex, rightField, rightValue)
		for currentLeft := leftIndex; currentLeft < leftEnd; currentLeft++ {
			currentLeftValue, currentLeftValid := typedTableMergeJoinValue(left[currentLeft].Values, leftField)
			if !currentLeftValid || compareTypedTableMergeJoinValues(currentLeftValue, leftValue) != 0 {
				continue
			}
			for currentRight := rightIndex; currentRight < rightEnd; currentRight++ {
				currentRightValue, currentRightValid := typedTableMergeJoinValue(right[currentRight].Values, rightField)
				if !currentRightValid || compareTypedTableMergeJoinValues(currentRightValue, rightValue) != 0 {
					continue
				}
				if err := visit(TypedTableJoinRow{
					LeftKey:  left[currentLeft].Key,
					RightKey: right[currentRight].Key,
					Left:     cloneTypedTableValues(left[currentLeft].Values),
					Right:    cloneTypedTableValues(right[currentRight].Values),
				}); err != nil {
					return err
				}
			}
		}
		leftIndex, rightIndex = leftEnd, rightEnd
	}
	return nil
}

func validateTypedTableMergeJoinInput(rows []TypedTableMergeJoinInput, field int) (TypedTableKind, bool, error) {
	var kind TypedTableKind
	var known bool
	var previous TypedTableValue
	var previousKnown bool
	for rowIndex, row := range rows {
		if field >= len(row.Values) {
			return TypedTableNull, false, fmt.Errorf("%w: row=%d field=%d values=%d", ErrTypedTableMergeJoinField, rowIndex, field, len(row.Values))
		}
		value, valid := typedTableMergeJoinValue(row.Values, field)
		if !valid {
			continue
		}
		if !known {
			kind, known = value.Kind, true
		} else if value.Kind != kind {
			return TypedTableNull, false, fmt.Errorf("%w: row=%d got=%d want=%d", ErrTypedTableMergeJoinTypeMismatch, rowIndex, value.Kind, kind)
		}
		if previousKnown && compareTypedTableMergeJoinValues(previous, value) > 0 {
			return TypedTableNull, false, fmt.Errorf("%w: row=%d", ErrTypedTableMergeJoinUnsorted, rowIndex)
		}
		previous, previousKnown = value, true
	}
	return kind, known, nil
}

func typedTableMergeJoinValue(values []TypedTableValue, field int) (TypedTableValue, bool) {
	if field < 0 || field >= len(values) || !values[field].Valid || values[field].Kind == TypedTableNull {
		return TypedTableValue{}, false
	}
	value := values[field]
	if value.Kind == TypedTableFloat64 && math.IsNaN(value.Float64) {
		return TypedTableValue{}, false
	}
	return value, true
}

func compareTypedTableMergeJoinValues(left, right TypedTableValue) int {
	switch left.Kind {
	case TypedTableString:
		if left.String < right.String {
			return -1
		}
		if left.String > right.String {
			return 1
		}
	case TypedTableInt64:
		if left.Int64 < right.Int64 {
			return -1
		}
		if left.Int64 > right.Int64 {
			return 1
		}
	case TypedTableFloat64:
		if left.Float64 < right.Float64 {
			return -1
		}
		if left.Float64 > right.Float64 {
			return 1
		}
	case TypedTableBool:
		if !left.Bool && right.Bool {
			return -1
		}
		if left.Bool && !right.Bool {
			return 1
		}
	}
	return 0
}

func typedTableMergeJoinEqualRunEnd(rows []TypedTableMergeJoinInput, start, field int, key TypedTableValue) int {
	index := start
	for index < len(rows) {
		value, valid := typedTableMergeJoinValue(rows[index].Values, field)
		if valid && compareTypedTableMergeJoinValues(value, key) != 0 {
			break
		}
		index++
	}
	return index
}
