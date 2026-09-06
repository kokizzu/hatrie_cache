package hatSql

import (
	"fmt"
	"reflect"
)

// sqlAggregateCollectionValues evaluates one aggregate argument in source-row
// order. NULL values are retained; callers choose whether to preserve or
// de-duplicate them.
func sqlAggregateCollectionValues(expr sqlExpr, group []sqlExecRow) ([]interface{}, error) {
	if len(expr.args) != 1 {
		return nil, fmt.Errorf("%s expects exactly one argument", expr.name)
	}
	rows, err := sqlAggregateFilterRows(expr, group)
	if err != nil {
		return nil, err
	}
	values := make([]interface{}, 0, len(rows))
	for _, row := range rows {
		value := evalSQLExpr(expr.args[0], []sqlExecRow{row}, row)
		if err := sqlExpressionError(value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
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
