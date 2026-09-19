package hatSql

import (
	"fmt"
	"strings"

	"hatrie_cache/hat/hatDataStructure"
)

func sqlApproximateDistinctStateSpec(name string) (merge, ok bool) {
	switch strings.ToUpper(name) {
	case "APPROX_COUNT_DISTINCT_STATE":
		return false, true
	case "APPROX_COUNT_DISTINCT_MERGE":
		return true, true
	default:
		return false, false
	}
}

func sqlApproximateDistinctStatePrecision(expr sqlExpr) (uint8, error) {
	if len(expr.args) < 1 || len(expr.args) > 2 || expr.args[0].kind == "star" {
		return 0, fmt.Errorf("%s expects one value expression and an optional precision", expr.name)
	}
	precision := hatDataStructure.DefaultHyperLogLogPrecision
	if len(expr.args) == 2 {
		value, err := sqlApproximateIntegerArgument(expr.args[1], expr.name+" precision")
		if err != nil {
			return 0, err
		}
		if value < int64(hatDataStructure.MinHyperLogLogPrecision) || value > int64(hatDataStructure.MaxHyperLogLogPrecision) {
			return 0, fmt.Errorf("%s precision must be between %d and %d", expr.name, hatDataStructure.MinHyperLogLogPrecision, hatDataStructure.MaxHyperLogLogPrecision)
		}
		precision = uint8(value)
	}
	return precision, nil
}

func evalSQLApproximateDistinctState(expr sqlExpr, group []sqlExecRow) interface{} {
	merge, ok := sqlApproximateDistinctStateSpec(expr.name)
	if !ok {
		return sqlApproximateAggregateError(expr, fmt.Sprintf("unsupported approximate distinct state %q", expr.name))
	}
	if merge {
		if len(expr.args) != 1 || expr.args[0].kind == "star" {
			return sqlApproximateAggregateError(expr, fmt.Sprintf("%s expects exactly one state argument", expr.name))
		}
		rows, err := sqlAggregateFilterRows(expr, group)
		if err != nil {
			return sqlEvaluationFailure(err)
		}
		var merged hatDataStructure.HyperLogLog
		for _, row := range rows {
			value := evalSQLExpr(expr.args[0], []sqlExecRow{row}, row)
			if err := sqlExpressionError(value); err != nil {
				return sqlEvaluationFailure(err)
			}
			if value == nil {
				continue
			}
			serialized, ok := value.([]byte)
			if !ok {
				return sqlApproximateAggregateError(expr, fmt.Sprintf("%s expects serialized HAG1 state, got %s", expr.name, sqlLiteralTypeName(value)))
			}
			other, err := hatDataStructure.NewHyperLogLogFromAggregateState(serialized)
			if err != nil {
				return sqlApproximateAggregateError(expr, err.Error())
			}
			if err := merged.Merge(other); err != nil {
				return sqlApproximateAggregateError(expr, err.Error())
			}
		}
		return merged.Count()
	}

	precision, err := sqlApproximateDistinctStatePrecision(expr)
	if err != nil {
		return sqlApproximateAggregateError(expr, err.Error())
	}
	sketch, err := hatDataStructure.NewHyperLogLog(precision)
	if err != nil {
		return sqlApproximateAggregateError(expr, err.Error())
	}
	rows, err := sqlAggregateFilterRows(expr, group)
	if err != nil {
		return sqlEvaluationFailure(err)
	}
	for _, row := range rows {
		value := evalSQLExpr(expr.args[0], nil, row)
		if err := sqlExpressionError(value); err != nil {
			return sqlEvaluationFailure(err)
		}
		if value == nil {
			continue
		}
		encoded, err := sqlApproximateValueKey(value)
		if err != nil {
			return sqlApproximateAggregateError(expr, err.Error())
		}
		sketch.AddJSONString(encoded)
	}
	wire, err := sketch.MarshalAggregateState()
	if err != nil {
		return sqlApproximateAggregateError(expr, err.Error())
	}
	return wire
}
