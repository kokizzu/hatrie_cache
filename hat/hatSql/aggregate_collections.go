package hatSql

import (
	"fmt"
	"reflect"
)

// sqlAggregateCollectionValues evaluates one aggregate argument in source-row
// order. NULL values are retained; callers choose whether to preserve or
// de-duplicate them.
func sqlAggregateCollectionValues(expr sqlExpr, group []sqlExecRow) ([]interface{}, error) {
	limit, err := sqlAggregateCollectionLimit(expr)
	if err != nil {
		return nil, err
	}
	capacity := len(group)
	if limit >= 0 && capacity > limit {
		capacity = limit
	}
	values := make([]interface{}, 0, capacity)
	if limit == 0 {
		return values, nil
	}
	for _, row := range group {
		if expr.filter != nil {
			matched := evalSQLExpr(*expr.filter, []sqlExecRow{row}, row)
			if err := sqlExpressionError(matched); err != nil {
				return nil, err
			}
			if !sqlTruthy(matched) {
				continue
			}
		}
		value := evalSQLExpr(expr.args[0], []sqlExecRow{row}, row)
		if err := sqlExpressionError(value); err != nil {
			return nil, err
		}
		values = append(values, value)
		if limit >= 0 && len(values) == limit {
			break
		}
	}
	return values, nil
}

func sqlAggregateCollectionLimit(expr sqlExpr) (int, error) {
	if expr.name != "GROUP_ARRAY" || len(expr.args) == 1 {
		if len(expr.args) != 1 {
			return 0, fmt.Errorf("%s expects exactly one argument", expr.name)
		}
		return -1, nil
	}
	if len(expr.args) != 2 {
		return 0, fmt.Errorf("%s expects one value and an optional limit", expr.name)
	}
	limitExpr := expr.args[1]
	if limitExpr.kind != "literal" {
		return 0, fmt.Errorf("%s limit must be a non-negative integer literal", expr.name)
	}
	maxInt := int(^uint(0) >> 1)
	switch value := limitExpr.value.(type) {
	case int:
		if value < 0 {
			return 0, fmt.Errorf("%s limit must be non-negative", expr.name)
		}
		return value, nil
	case int64:
		if value < 0 {
			return 0, fmt.Errorf("%s limit must be non-negative", expr.name)
		}
		if value > int64(maxInt) {
			return 0, fmt.Errorf("%s limit is too large", expr.name)
		}
		return int(value), nil
	case uint:
		if value > uint(maxInt) {
			return 0, fmt.Errorf("%s limit is too large", expr.name)
		}
		return int(value), nil
	case uint64:
		if value > uint64(maxInt) {
			return 0, fmt.Errorf("%s limit is too large", expr.name)
		}
		return int(value), nil
	default:
		return 0, fmt.Errorf("%s limit must be a non-negative integer literal", expr.name)
	}
}

func sqlAggregateUniqueCollection(values []interface{}) []interface{} {
	unique := make([]interface{}, 0, len(values))
	for _, value := range values {
		seen := false
		for _, existing := range unique {
			if reflect.DeepEqual(existing, value) {
				seen = true
				break
			}
		}
		if !seen {
			unique = append(unique, value)
		}
	}
	return unique
}

func sqlAggregateMapValues(expr sqlExpr, group []sqlExecRow) (map[interface{}]interface{}, error) {
	if len(expr.args) != 2 {
		return nil, fmt.Errorf("%s expects exactly two arguments", expr.name)
	}
	rows, err := sqlAggregateFilterRows(expr, group)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	values := make(map[interface{}]interface{}, len(rows))
	for _, row := range rows {
		key := evalSQLExpr(expr.args[0], []sqlExecRow{row}, row)
		if err := sqlExpressionError(key); err != nil {
			return nil, err
		}
		if key == nil {
			return nil, fmt.Errorf("%s does not allow NULL keys", expr.name)
		}
		if !reflect.TypeOf(key).Comparable() {
			return nil, fmt.Errorf("%s key type %T is not comparable", expr.name, key)
		}
		value := evalSQLExpr(expr.args[1], []sqlExecRow{row}, row)
		if err := sqlExpressionError(value); err != nil {
			return nil, err
		}
		// Duplicate keys use the last source-row value, matching map assignment
		// semantics and making the result deterministic for ordered input.
		values[key] = value
	}
	return values, nil
}
