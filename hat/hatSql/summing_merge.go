package hatSql

import (
	"errors"
	"fmt"
	"math"
)

var (
	ErrSQLSummingMergeKeyRequired     = errors.New("hatSql: summing merge key callback is required")
	ErrSQLSummingMergeColumnsRequired = errors.New("hatSql: summing merge columns are required")
	ErrSQLSummingMergeOverflow        = errors.New("hatSql: summing merge numeric overflow")
	ErrSQLSummingMergeTypeMismatch    = errors.New("hatSql: summing merge numeric type mismatch")
	ErrSQLSummingMergeNonNumeric      = errors.New("hatSql: summing merge value is not numeric")
)

// SumSQLRows implements an explicit SummingMergeTree-style merge. Rows with
// the same key retain the first row's non-summed fields while the named
// numeric columns are added. Output keys keep first-seen order and output maps
// are shallow copies of the selected input rows.
func SumSQLRows(rows []SQLRow, key SQLReplacingMergeKeyFunc, sumColumns []string) ([]SQLRow, error) {
	if key == nil {
		return nil, ErrSQLSummingMergeKeyRequired
	}
	if err := validateSQLSummingColumns(sumColumns); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}

	positions := make(map[string]int, len(rows))
	merged := make([]SQLRow, 0, len(rows))
	for _, row := range rows {
		rowKey := key(row)
		position, exists := positions[rowKey]
		if !exists {
			positions[rowKey] = len(merged)
			merged = append(merged, cloneSQLSummingMergeRow(row))
			for _, column := range sumColumns {
				incoming, present := row[column]
				if !present || incoming == nil {
					continue
				}
				if err := validateSQLSummingValue(incoming); err != nil {
					return nil, fmt.Errorf("column %q: %w", column, err)
				}
			}
			continue
		}

		for _, column := range sumColumns {
			incoming, present := row[column]
			if !present || incoming == nil {
				continue
			}
			if err := validateSQLSummingValue(incoming); err != nil {
				return nil, fmt.Errorf("column %q: %w", column, err)
			}

			current, currentPresent := merged[position][column]
			if !currentPresent || current == nil {
				if merged[position] == nil {
					merged[position] = make(SQLRow)
				}
				merged[position][column] = incoming
				continue
			}
			sum, err := addSQLSummingValues(current, incoming)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", column, err)
			}
			merged[position][column] = sum
		}
	}
	return merged, nil
}

func validateSQLSummingColumns(columns []string) error {
	if len(columns) == 0 {
		return ErrSQLSummingMergeColumnsRequired
	}
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if column == "" {
			return fmt.Errorf("%w: empty column name", ErrSQLSummingMergeColumnsRequired)
		}
		if _, exists := seen[column]; exists {
			return fmt.Errorf("%w: duplicate column %q", ErrSQLSummingMergeColumnsRequired, column)
		}
		seen[column] = struct{}{}
	}
	return nil
}

func validateSQLSummingValue(value interface{}) error {
	switch value.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return nil
	default:
		return fmt.Errorf("%w: %T", ErrSQLSummingMergeNonNumeric, value)
	}
}

func addSQLSummingValues(left, right interface{}) (interface{}, error) {
	switch leftValue := left.(type) {
	case int:
		rightValue, ok := right.(int)
		if !ok {
			return nil, fmt.Errorf("%w: %T and %T", ErrSQLSummingMergeTypeMismatch, left, right)
		}
		maxInt := int(^uint(0) >> 1)
		minInt := -maxInt - 1
		if (rightValue > 0 && leftValue > maxInt-rightValue) || (rightValue < 0 && leftValue < minInt-rightValue) {
			return nil, ErrSQLSummingMergeOverflow
		}
		return leftValue + rightValue, nil
	case int8:
		rightValue, ok := right.(int8)
		if !ok {
			return nil, fmt.Errorf("%w: %T and %T", ErrSQLSummingMergeTypeMismatch, left, right)
		}
		sum := int16(leftValue) + int16(rightValue)
		if sum < math.MinInt8 || sum > math.MaxInt8 {
			return nil, ErrSQLSummingMergeOverflow
		}
		return int8(sum), nil
	case int16:
		rightValue, ok := right.(int16)
		if !ok {
			return nil, fmt.Errorf("%w: %T and %T", ErrSQLSummingMergeTypeMismatch, left, right)
		}
		sum := int32(leftValue) + int32(rightValue)
		if sum < math.MinInt16 || sum > math.MaxInt16 {
			return nil, ErrSQLSummingMergeOverflow
		}
		return int16(sum), nil
	case int32:
		rightValue, ok := right.(int32)
		if !ok {
			return nil, fmt.Errorf("%w: %T and %T", ErrSQLSummingMergeTypeMismatch, left, right)
		}
		sum := int64(leftValue) + int64(rightValue)
		if sum < math.MinInt32 || sum > math.MaxInt32 {
			return nil, ErrSQLSummingMergeOverflow
		}
		return int32(sum), nil
	case int64:
		rightValue, ok := right.(int64)
		if !ok {
			return nil, fmt.Errorf("%w: %T and %T", ErrSQLSummingMergeTypeMismatch, left, right)
		}
		if (rightValue > 0 && leftValue > math.MaxInt64-rightValue) || (rightValue < 0 && leftValue < math.MinInt64-rightValue) {
			return nil, ErrSQLSummingMergeOverflow
		}
		return leftValue + rightValue, nil
	case uint:
		rightValue, ok := right.(uint)
		if !ok {
			return nil, fmt.Errorf("%w: %T and %T", ErrSQLSummingMergeTypeMismatch, left, right)
		}
		if rightValue > ^uint(0)-leftValue {
			return nil, ErrSQLSummingMergeOverflow
		}
		return leftValue + rightValue, nil
	case uint8:
		rightValue, ok := right.(uint8)
		if !ok {
			return nil, fmt.Errorf("%w: %T and %T", ErrSQLSummingMergeTypeMismatch, left, right)
		}
		sum := uint16(leftValue) + uint16(rightValue)
		if sum > math.MaxUint8 {
			return nil, ErrSQLSummingMergeOverflow
		}
		return uint8(sum), nil
	case uint16:
		rightValue, ok := right.(uint16)
		if !ok {
			return nil, fmt.Errorf("%w: %T and %T", ErrSQLSummingMergeTypeMismatch, left, right)
		}
		sum := uint32(leftValue) + uint32(rightValue)
		if sum > math.MaxUint16 {
			return nil, ErrSQLSummingMergeOverflow
		}
		return uint16(sum), nil
	case uint32:
		rightValue, ok := right.(uint32)
		if !ok {
			return nil, fmt.Errorf("%w: %T and %T", ErrSQLSummingMergeTypeMismatch, left, right)
		}
		sum := uint64(leftValue) + uint64(rightValue)
		if sum > math.MaxUint32 {
			return nil, ErrSQLSummingMergeOverflow
		}
		return uint32(sum), nil
	case uint64:
		rightValue, ok := right.(uint64)
		if !ok {
			return nil, fmt.Errorf("%w: %T and %T", ErrSQLSummingMergeTypeMismatch, left, right)
		}
		if rightValue > math.MaxUint64-leftValue {
			return nil, ErrSQLSummingMergeOverflow
		}
		return leftValue + rightValue, nil
	case float32:
		rightValue, ok := right.(float32)
		if !ok {
			return nil, fmt.Errorf("%w: %T and %T", ErrSQLSummingMergeTypeMismatch, left, right)
		}
		sum := leftValue + rightValue
		if math.IsInf(float64(sum), 0) && !math.IsInf(float64(leftValue), 0) && !math.IsInf(float64(rightValue), 0) {
			return nil, ErrSQLSummingMergeOverflow
		}
		return sum, nil
	case float64:
		rightValue, ok := right.(float64)
		if !ok {
			return nil, fmt.Errorf("%w: %T and %T", ErrSQLSummingMergeTypeMismatch, left, right)
		}
		sum := leftValue + rightValue
		if math.IsInf(sum, 0) && !math.IsInf(leftValue, 0) && !math.IsInf(rightValue, 0) {
			return nil, ErrSQLSummingMergeOverflow
		}
		return sum, nil
	default:
		return nil, fmt.Errorf("%w: %T", ErrSQLSummingMergeNonNumeric, left)
	}
}

func cloneSQLSummingMergeRow(row SQLRow) SQLRow {
	if row == nil {
		return nil
	}
	clone := make(SQLRow, len(row))
	for key, value := range row {
		clone[key] = value
	}
	return clone
}
