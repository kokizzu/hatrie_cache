package hatSql

import (
	"errors"
	"fmt"
	"sort"
)

var (
	// ErrTypedTableSortedArrangementBound reports a malformed range bound.
	ErrTypedTableSortedArrangementBound = errors.New("typed table sorted arrangement bound is invalid")
)

// TypedTableSortedArrangementBound describes one inclusive or exclusive edge
// of a sorted arrangement range. Values must contain one entry for every
// configured order field, in the same order as the arrangement definition.
// An invalid value represents NULL and follows the arrangement's NullsFirst
// setting. A nil bound leaves that side unbounded.
type TypedTableSortedArrangementBound struct {
	Values    []TypedTableValue
	Inclusive bool
}

// RowsRange returns an independent snapshot of rows between lower and upper.
// The ordered key vector is binary-searched, so the method avoids scanning or
// copying rows outside the requested range. Non-positive limits return an
// empty snapshot after validating the supplied bounds.
func (arrangement *TypedTableSortedArrangement) RowsRange(lower, upper *TypedTableSortedArrangementBound, limit int) ([]TypedTableMergeJoinInput, error) {
	if arrangement == nil {
		return nil, ErrTypedTableSortedArrangementNil
	}
	arrangement.mu.RLock()
	defer arrangement.mu.RUnlock()
	if err := arrangement.validateRangeBound(lower); err != nil {
		return nil, err
	}
	if err := arrangement.validateRangeBound(upper); err != nil {
		return nil, err
	}
	if limit <= 0 || len(arrangement.order) == 0 {
		return []TypedTableMergeJoinInput{}, nil
	}
	start := 0
	if lower != nil {
		start = sort.Search(len(arrangement.order), func(index int) bool {
			comparison := arrangement.compareRowToBound(arrangement.entries[arrangement.order[index]].Values, lower.Values)
			if lower.Inclusive {
				return comparison >= 0
			}
			return comparison > 0
		})
	}
	end := len(arrangement.order)
	if upper != nil {
		end = sort.Search(len(arrangement.order), func(index int) bool {
			comparison := arrangement.compareRowToBound(arrangement.entries[arrangement.order[index]].Values, upper.Values)
			if upper.Inclusive {
				return comparison > 0
			}
			return comparison >= 0
		})
	}
	if start >= end {
		return []TypedTableMergeJoinInput{}, nil
	}
	if limit < end-start {
		end = start + limit
	}
	return arrangement.rowsPageLocked(start, end-start), nil
}

func (arrangement *TypedTableSortedArrangement) validateRangeBound(bound *TypedTableSortedArrangementBound) error {
	if bound == nil {
		return nil
	}
	if len(bound.Values) != len(arrangement.orderFields) {
		return fmt.Errorf("%w: got %d values, want %d", ErrTypedTableSortedArrangementBound, len(bound.Values), len(arrangement.orderFields))
	}
	for index, orderField := range arrangement.orderFields {
		value := bound.Values[index]
		if value.Valid && value.Kind != orderField.kind {
			return fmt.Errorf("%w: value %d has kind %d, want %d", ErrTypedTableSortedArrangementBound, index, value.Kind, orderField.kind)
		}
	}
	return nil
}

func (arrangement *TypedTableSortedArrangement) compareOrderedValues(left, right []TypedTableValue) int {
	for _, orderField := range arrangement.orderFields {
		leftValue, leftValid := typedTableSortedArrangementValue(left, orderField.index)
		rightValue, rightValid := typedTableSortedArrangementValue(right, orderField.index)
		if leftValid != rightValid {
			if orderField.nullsFirst == leftValid {
				return 1
			}
			return -1
		}
		if leftValid {
			comparison := compareTypedTableMergeJoinValues(leftValue, rightValue)
			if orderField.descending {
				comparison = -comparison
			}
			if comparison != 0 {
				return comparison
			}
		}
	}
	return 0
}

func (arrangement *TypedTableSortedArrangement) compareRowToBound(row, bound []TypedTableValue) int {
	for orderIndex, orderField := range arrangement.orderFields {
		leftValue, leftValid := typedTableSortedArrangementValue(row, orderField.index)
		rightValue, rightValid := typedTableSortedArrangementValue(bound, orderIndex)
		if leftValid != rightValid {
			if orderField.nullsFirst == leftValid {
				return 1
			}
			return -1
		}
		if leftValid {
			comparison := compareTypedTableMergeJoinValues(leftValue, rightValue)
			if orderField.descending {
				comparison = -comparison
			}
			if comparison != 0 {
				return comparison
			}
		}
	}
	return 0
}
