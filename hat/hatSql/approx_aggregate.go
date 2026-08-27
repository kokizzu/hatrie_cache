package hatSql

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"hatrie_cache/hat/hatDataStructure"
)

const (
	defaultSQLApproxTopKCapacity = 10
	maxSQLApproxTopKCapacity     = 65536
)

// SQLApproxTopKItem is one bounded Space-Saving heavy-hitter estimate.
// Estimate is never lower than the actual count; Error bounds its overcount.
type SQLApproxTopKItem struct {
	Value    interface{} `json:"value"`
	Estimate uint64      `json:"estimate"`
	Error    uint64      `json:"error"`
}

type sqlApproxTopKEntry struct {
	SQLApproxTopKItem
	key string
}

func evalSQLApproximateAggregate(expr sqlExpr, group []sqlExecRow) interface{} {
	switch expr.name {
	case "APPROX_COUNT_DISTINCT":
		return evalSQLApproxCountDistinct(expr, group)
	case "APPROX_PERCENTILE":
		return evalSQLApproxPercentile(expr, group)
	case "APPROX_TOP_K":
		return evalSQLApproxTopK(expr, group)
	default:
		return sqlEvalError{err: fmt.Errorf("unsupported approximate aggregate %q", expr.name), token: expr.token}
	}
}

func evalSQLApproxCountDistinct(expr sqlExpr, group []sqlExecRow) interface{} {
	if len(expr.args) < 1 || len(expr.args) > 2 {
		return sqlApproximateAggregateError(expr, "APPROX_COUNT_DISTINCT expects one value expression and an optional precision")
	}
	precision := hatDataStructure.DefaultHyperLogLogPrecision
	if len(expr.args) == 2 {
		value, err := sqlApproximateIntegerArgument(expr.args[1], "APPROX_COUNT_DISTINCT precision")
		if err != nil {
			return sqlApproximateAggregateError(expr, err.Error())
		}
		if value > math.MaxUint8 {
			return sqlApproximateAggregateError(expr, "APPROX_COUNT_DISTINCT precision is out of range")
		}
		precision = uint8(value)
	}
	sketch, err := hatDataStructure.NewHyperLogLog(precision)
	if err != nil {
		return sqlApproximateAggregateError(expr, err.Error())
	}
	for _, row := range group {
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
	return sketch.Count()
}

func evalSQLApproxPercentile(expr sqlExpr, group []sqlExecRow) interface{} {
	if len(expr.args) < 2 || len(expr.args) > 3 {
		return sqlApproximateAggregateError(expr, "APPROX_PERCENTILE expects a value expression, quantile, and optional epsilon")
	}
	quantile, err := sqlApproximateNumberArgument(expr.args[1], "APPROX_PERCENTILE quantile")
	if err != nil || quantile < 0 || quantile > 1 {
		if err == nil {
			err = fmt.Errorf("APPROX_PERCENTILE quantile must be between 0 and 1")
		}
		return sqlApproximateAggregateError(expr, err.Error())
	}
	epsilon := hatDataStructure.DefaultQuantileSketchEpsilon
	if len(expr.args) == 3 {
		epsilon, err = sqlApproximateNumberArgument(expr.args[2], "APPROX_PERCENTILE epsilon")
		if err != nil {
			return sqlApproximateAggregateError(expr, err.Error())
		}
	}
	sketch, err := hatDataStructure.NewQuantileSketch(epsilon)
	if err != nil {
		return sqlApproximateAggregateError(expr, err.Error())
	}
	for _, row := range group {
		value := evalSQLExpr(expr.args[0], nil, row)
		if err := sqlExpressionError(value); err != nil {
			return sqlEvaluationFailure(err)
		}
		if number, ok := sqlNumber(value); ok && !math.IsNaN(number) && !math.IsInf(number, 0) {
			sketch.Add(number)
		}
	}
	estimate, ok := sketch.Estimate(quantile)
	if !ok {
		return nil
	}
	return estimate.Value
}

func evalSQLApproxTopK(expr sqlExpr, group []sqlExecRow) interface{} {
	if len(expr.args) < 1 || len(expr.args) > 2 {
		return sqlApproximateAggregateError(expr, "APPROX_TOP_K expects one value expression and an optional capacity")
	}
	capacity := defaultSQLApproxTopKCapacity
	if len(expr.args) == 2 {
		value, err := sqlApproximateIntegerArgument(expr.args[1], "APPROX_TOP_K capacity")
		if err != nil || value <= 0 || value > maxSQLApproxTopKCapacity {
			if err == nil {
				err = fmt.Errorf("APPROX_TOP_K capacity must be between 1 and %d", maxSQLApproxTopKCapacity)
			}
			return sqlApproximateAggregateError(expr, err.Error())
		}
		capacity = int(value)
	}
	entries := make([]sqlApproxTopKEntry, 0, capacity)
	positions := make(map[string]int, capacity)
	for _, row := range group {
		value := evalSQLExpr(expr.args[0], nil, row)
		if err := sqlExpressionError(value); err != nil {
			return sqlEvaluationFailure(err)
		}
		if value == nil {
			continue
		}
		key, err := sqlApproximateValueKey(value)
		if err != nil {
			return sqlApproximateAggregateError(expr, err.Error())
		}
		if index, ok := positions[key]; ok {
			entries[index].Estimate++
			continue
		}
		if len(entries) < capacity {
			positions[key] = len(entries)
			entries = append(entries, sqlApproxTopKEntry{SQLApproxTopKItem: SQLApproxTopKItem{Value: value, Estimate: 1}, key: key})
			continue
		}
		minimum := 0
		for index := 1; index < len(entries); index++ {
			if entries[index].Estimate < entries[minimum].Estimate || entries[index].Estimate == entries[minimum].Estimate && entries[index].key > entries[minimum].key {
				minimum = index
			}
		}
		delete(positions, entries[minimum].key)
		replaced := entries[minimum].Estimate
		entries[minimum] = sqlApproxTopKEntry{SQLApproxTopKItem: SQLApproxTopKItem{Value: value, Estimate: replaced + 1, Error: replaced}, key: key}
		positions[key] = minimum
	}
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].Estimate != entries[right].Estimate {
			return entries[left].Estimate > entries[right].Estimate
		}
		return entries[left].key < entries[right].key
	})
	out := make([]SQLApproxTopKItem, len(entries))
	for index := range entries {
		out[index] = entries[index].SQLApproxTopKItem
	}
	return out
}

func sqlApproximateNumberArgument(expr sqlExpr, name string) (float64, error) {
	value := evalSQLExpr(expr, nil, sqlExecRow{})
	if err := sqlExpressionError(value); err != nil {
		return 0, err
	}
	number, ok := sqlNumber(value)
	if !ok || math.IsNaN(number) || math.IsInf(number, 0) {
		return 0, fmt.Errorf("%s must be a finite numeric literal", name)
	}
	return number, nil
}

func sqlApproximateIntegerArgument(expr sqlExpr, name string) (int64, error) {
	number, err := sqlApproximateNumberArgument(expr, name)
	if err != nil {
		return 0, err
	}
	if math.Trunc(number) != number || number < math.MinInt64 || number > math.MaxInt64 {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return int64(number), nil
}

func sqlApproximateValueKey(value interface{}) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode approximate aggregate value: %w", err)
	}
	return string(encoded), nil
}

func sqlApproximateAggregateError(expr sqlExpr, message string) sqlEvalError {
	return sqlEvalError{err: fmt.Errorf("%s", message), token: expr.token}
}
