package hatSql

import "fmt"

// SQLRowBinaryStatsPredicateOperator is the subset of SQL predicates that can
// be answered conservatively from a column's min/max and NULL counts.
type SQLRowBinaryStatsPredicateOperator uint8

const (
	SQLRowBinaryStatsEqual SQLRowBinaryStatsPredicateOperator = iota + 1
	SQLRowBinaryStatsNotEqual
	SQLRowBinaryStatsLess
	SQLRowBinaryStatsLessEqual
	SQLRowBinaryStatsGreater
	SQLRowBinaryStatsGreaterEqual
	SQLRowBinaryStatsBetween
	SQLRowBinaryStatsIsNull
	SQLRowBinaryStatsIsNotNull
)

// SQLRowBinaryStatsPredicate describes one conservative block-pruning check.
// Value is used by all operators except IS NULL and IS NOT NULL; UpperValue is
// required by BETWEEN.
type SQLRowBinaryStatsPredicate struct {
	Column     string
	Operator   SQLRowBinaryStatsPredicateOperator
	Value      interface{}
	UpperValue interface{}
}

// CanSkipSQLRowBinaryStats reports whether a valid RowBinary block cannot
// produce a row matching predicate. A false result never claims that a block
// contains a match; it only means the available statistics are insufficient
// to prove that it can be skipped.
func CanSkipSQLRowBinaryStats(columns []SQLRowBinaryColumn, stats []SQLRowBinaryColumnStats, predicate SQLRowBinaryStatsPredicate) (bool, error) {
	if len(columns) == 0 || len(stats) != len(columns) {
		return false, fmt.Errorf("RowBinary stats schema and statistics lengths differ")
	}
	columnIndex := -1
	for index, column := range columns {
		if column.Name == predicate.Column {
			columnIndex = index
			break
		}
	}
	if columnIndex < 0 {
		return false, fmt.Errorf("RowBinary stats predicate column %q was not found", predicate.Column)
	}
	column := columns[columnIndex]
	columnStats := stats[columnIndex]
	if columnStats.Name != column.Name {
		return false, fmt.Errorf("RowBinary stats for column %q are out of order", column.Name)
	}
	switch predicate.Operator {
	case SQLRowBinaryStatsIsNull:
		return columnStats.NullCount == 0, nil
	case SQLRowBinaryStatsIsNotNull:
		return columnStats.ValueCount == 0, nil
	case SQLRowBinaryStatsEqual, SQLRowBinaryStatsNotEqual, SQLRowBinaryStatsLess, SQLRowBinaryStatsLessEqual, SQLRowBinaryStatsGreater, SQLRowBinaryStatsGreaterEqual, SQLRowBinaryStatsBetween:
	default:
		return false, fmt.Errorf("RowBinary stats predicate operator %d is unsupported", predicate.Operator)
	}
	if predicate.Value == nil {
		return false, fmt.Errorf("RowBinary stats predicate value is required")
	}
	value, orderable, err := normalizeSQLRowBinaryStatsValue(column.Type, predicate.Value, -1, column.Name)
	if err != nil {
		return false, err
	}
	if !orderable {
		return false, fmt.Errorf("RowBinary stats column %q does not support ordered pruning", column.Name)
	}
	if predicate.Operator == SQLRowBinaryStatsBetween {
		if predicate.UpperValue == nil {
			return false, fmt.Errorf("RowBinary stats BETWEEN upper value is required")
		}
		upper, upperOrderable, upperErr := normalizeSQLRowBinaryStatsValue(column.Type, predicate.UpperValue, -1, column.Name)
		if upperErr != nil {
			return false, upperErr
		}
		if !upperOrderable || compareSQLRowBinaryStatsValues(column.Type, value, upper) > 0 {
			return false, fmt.Errorf("RowBinary stats BETWEEN bounds are invalid")
		}
		if !columnStats.HasMinMax || columnStats.ValueCount == 0 {
			return true, nil
		}
		return compareSQLRowBinaryStatsValues(column.Type, columnStats.Max, value) < 0 || compareSQLRowBinaryStatsValues(column.Type, columnStats.Min, upper) > 0, nil
	}
	if !columnStats.HasMinMax || columnStats.ValueCount == 0 {
		return true, nil
	}
	minimum := compareSQLRowBinaryStatsValues(column.Type, columnStats.Min, value)
	maximum := compareSQLRowBinaryStatsValues(column.Type, columnStats.Max, value)
	switch predicate.Operator {
	case SQLRowBinaryStatsEqual:
		return minimum > 0 || maximum < 0, nil
	case SQLRowBinaryStatsNotEqual:
		return minimum == 0 && maximum == 0, nil
	case SQLRowBinaryStatsLess:
		return minimum >= 0, nil
	case SQLRowBinaryStatsLessEqual:
		return minimum > 0, nil
	case SQLRowBinaryStatsGreater:
		return maximum <= 0, nil
	case SQLRowBinaryStatsGreaterEqual:
		return maximum < 0, nil
	default:
		return false, fmt.Errorf("RowBinary stats predicate operator %d is unsupported", predicate.Operator)
	}
}
