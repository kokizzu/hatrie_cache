package hatSql

import (
	"math"
	"time"
)

type sqlRuntimeJoinBoundKind uint8

const (
	sqlRuntimeJoinBoundInvalid sqlRuntimeJoinBoundKind = iota
	sqlRuntimeJoinBoundNumber
	sqlRuntimeJoinBoundString
	sqlRuntimeJoinBoundBool
	sqlRuntimeJoinBoundTime
)

// sqlRuntimeJoinBounds derives a sound inclusive range from the materialized
// join side. Unsupported values or mixed types disable the optimization so a
// resolver can never turn an advisory filter into a semantic filter.
func sqlRuntimeJoinBounds(rows []sqlExecRow, qualifier, field string) (SQLRuntimeJoinBounds, bool) {
	if field == "" {
		return SQLRuntimeJoinBounds{}, false
	}
	var kind sqlRuntimeJoinBoundKind
	var minimum, maximum interface{}
	var minimumNumber, maximumNumber float64
	for _, row := range rows {
		value := sqlField(row, qualifier, field)
		if value == nil {
			continue
		}
		valueKind, number, ok := sqlRuntimeJoinBoundValue(value)
		if !ok {
			return SQLRuntimeJoinBounds{}, false
		}
		if kind == sqlRuntimeJoinBoundInvalid {
			kind = valueKind
			minimum, maximum = value, value
			minimumNumber, maximumNumber = number, number
			continue
		}
		if kind != valueKind {
			return SQLRuntimeJoinBounds{}, false
		}
		switch kind {
		case sqlRuntimeJoinBoundNumber:
			if number < minimumNumber {
				minimum, minimumNumber = value, number
			}
			if number > maximumNumber {
				maximum, maximumNumber = value, number
			}
		case sqlRuntimeJoinBoundString:
			text := value.(string)
			if text < minimum.(string) {
				minimum = text
			}
			if text > maximum.(string) {
				maximum = text
			}
		case sqlRuntimeJoinBoundBool:
			boolean := value.(bool)
			if !boolean {
				minimum = false
			}
			if boolean {
				maximum = true
			}
		case sqlRuntimeJoinBoundTime:
			moment := value.(time.Time)
			if moment.Before(minimum.(time.Time)) {
				minimum = moment
			}
			if moment.After(maximum.(time.Time)) {
				maximum = moment
			}
		}
	}
	if kind == sqlRuntimeJoinBoundInvalid {
		return SQLRuntimeJoinBounds{}, false
	}
	return SQLRuntimeJoinBounds{Field: field, Min: minimum, Max: maximum}, true
}

func sqlRuntimeJoinBoundValue(value interface{}) (sqlRuntimeJoinBoundKind, float64, bool) {
	if number, ok := sqlNumber(value); ok {
		if math.IsNaN(number) {
			return sqlRuntimeJoinBoundInvalid, 0, false
		}
		return sqlRuntimeJoinBoundNumber, number, true
	}
	switch value.(type) {
	case string:
		return sqlRuntimeJoinBoundString, 0, true
	case bool:
		return sqlRuntimeJoinBoundBool, 0, true
	case time.Time:
		return sqlRuntimeJoinBoundTime, 0, true
	default:
		return sqlRuntimeJoinBoundInvalid, 0, false
	}
}
