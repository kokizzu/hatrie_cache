package hatSql

import (
	"fmt"
	"math"
	"strings"

	"hatrie_cache/hat/hatDataStructure"
)

func sqlApproximateTDigestStateSpec(name string) (merge, ok bool) {
	switch strings.ToUpper(name) {
	case "APPROX_TDIGEST_PERCENTILE_STATE":
		return false, true
	case "APPROX_TDIGEST_PERCENTILE_MERGE":
		return true, true
	default:
		return false, false
	}
}

func sqlApproximateTDigestStateCompression(expr sqlExpr) (uint32, error) {
	if len(expr.args) < 1 || len(expr.args) > 2 || expr.args[0].kind == "star" {
		return 0, fmt.Errorf("%s expects one value expression and an optional compression", expr.name)
	}
	compression := hatDataStructure.DefaultTDigestCompression
	if len(expr.args) == 2 {
		value, err := sqlApproximateIntegerArgument(expr.args[1], expr.name+" compression")
		if err != nil {
			return 0, err
		}
		if value < int64(hatDataStructure.MinTDigestCompression) || value > int64(hatDataStructure.MaxTDigestCompression) {
			return 0, fmt.Errorf("%s compression must be between %d and %d", expr.name, hatDataStructure.MinTDigestCompression, hatDataStructure.MaxTDigestCompression)
		}
		compression = uint32(value)
	}
	return compression, nil
}

func sqlApproximateTDigestMergeQuantile(expr sqlExpr) (float64, error) {
	if len(expr.args) != 2 || expr.args[0].kind == "star" {
		return 0, fmt.Errorf("%s expects a serialized state and a quantile", expr.name)
	}
	quantile, err := sqlApproximateNumberArgument(expr.args[1], expr.name+" quantile")
	if err != nil {
		return 0, err
	}
	if quantile < 0 || quantile > 1 {
		return 0, fmt.Errorf("%s quantile must be between 0 and 1", expr.name)
	}
	return quantile, nil
}

func evalSQLApproximateTDigestState(expr sqlExpr, group []sqlExecRow) interface{} {
	merge, ok := sqlApproximateTDigestStateSpec(expr.name)
	if !ok {
		return sqlApproximateAggregateError(expr, fmt.Sprintf("unsupported t-digest state %q", expr.name))
	}
	rows, err := sqlAggregateFilterRows(expr, group)
	if err != nil {
		return sqlEvaluationFailure(err)
	}
	if merge {
		quantile, err := sqlApproximateTDigestMergeQuantile(expr)
		if err != nil {
			return sqlApproximateAggregateError(expr, err.Error())
		}
		var merged *hatDataStructure.TDigest
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
			other, err := hatDataStructure.NewTDigestFromAggregateState(serialized)
			if err != nil {
				return sqlApproximateAggregateError(expr, err.Error())
			}
			if merged == nil {
				merged = &other
				continue
			}
			if err := merged.Merge(other); err != nil {
				return sqlApproximateAggregateError(expr, err.Error())
			}
		}
		if merged == nil {
			return nil
		}
		estimate, ok := merged.Estimate(quantile)
		if !ok {
			return nil
		}
		return estimate.Value
	}

	compression, err := sqlApproximateTDigestStateCompression(expr)
	if err != nil {
		return sqlApproximateAggregateError(expr, err.Error())
	}
	digest, err := hatDataStructure.NewTDigest(compression)
	if err != nil {
		return sqlApproximateAggregateError(expr, err.Error())
	}
	for _, row := range rows {
		value := evalSQLExpr(expr.args[0], nil, row)
		if err := sqlExpressionError(value); err != nil {
			return sqlEvaluationFailure(err)
		}
		if number, ok := sqlNumber(value); ok && !math.IsNaN(number) && !math.IsInf(number, 0) {
			digest.Add(number)
		}
	}
	wire, err := digest.MarshalAggregateState()
	if err != nil {
		return sqlApproximateAggregateError(expr, err.Error())
	}
	return wire
}
