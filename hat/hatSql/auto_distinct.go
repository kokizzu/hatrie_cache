package hatSql

import (
	"fmt"

	"hatrie_cache/hat/hatDataStructure"
)

const (
	defaultSQLAutoDistinctExactLimit = 1024
	maxSQLAutoDistinctExactLimit     = 65536
)

type sqlAutoDistinctConfig struct {
	exactLimit int
	precision  uint8
}

func sqlAutoDistinctConfigFromExpr(expr sqlExpr) (sqlAutoDistinctConfig, error) {
	if len(expr.args) < 1 || len(expr.args) > 3 {
		return sqlAutoDistinctConfig{}, fmt.Errorf("AUTO_COUNT_DISTINCT expects a value expression, an optional exact limit, and an optional precision")
	}
	config := sqlAutoDistinctConfig{
		exactLimit: defaultSQLAutoDistinctExactLimit,
		precision:  hatDataStructure.DefaultHyperLogLogPrecision,
	}
	if len(expr.args) >= 2 {
		value, err := sqlApproximateIntegerArgument(expr.args[1], "AUTO_COUNT_DISTINCT exact limit")
		if err != nil {
			return sqlAutoDistinctConfig{}, err
		}
		if value < 0 || value > maxSQLAutoDistinctExactLimit {
			return sqlAutoDistinctConfig{}, fmt.Errorf("AUTO_COUNT_DISTINCT exact limit must be between 0 and %d", maxSQLAutoDistinctExactLimit)
		}
		config.exactLimit = int(value)
	}
	if len(expr.args) == 3 {
		value, err := sqlApproximateIntegerArgument(expr.args[2], "AUTO_COUNT_DISTINCT precision")
		if err != nil {
			return sqlAutoDistinctConfig{}, err
		}
		if value < int64(hatDataStructure.MinHyperLogLogPrecision) || value > int64(hatDataStructure.MaxHyperLogLogPrecision) {
			return sqlAutoDistinctConfig{}, fmt.Errorf("AUTO_COUNT_DISTINCT precision must be between %d and %d", hatDataStructure.MinHyperLogLogPrecision, hatDataStructure.MaxHyperLogLogPrecision)
		}
		config.precision = uint8(value)
	}
	return config, nil
}

type sqlAutoDistinctState struct {
	exactLimit int
	precision  uint8
	exact      map[string]struct{}
	hll        *hatDataStructure.HyperLogLog
}

func newSQLAutoDistinctState(config sqlAutoDistinctConfig) (*sqlAutoDistinctState, error) {
	state := &sqlAutoDistinctState{
		exactLimit: config.exactLimit,
		precision:  config.precision,
	}
	if config.exactLimit == 0 {
		sketch, err := hatDataStructure.NewHyperLogLog(config.precision)
		if err != nil {
			return nil, err
		}
		state.hll = &sketch
	}
	return state, nil
}

func (state *sqlAutoDistinctState) addValue(value interface{}) error {
	if value == nil {
		return nil
	}
	encoded, err := sqlApproximateValueKey(value)
	if err != nil {
		return err
	}
	if state.hll != nil {
		state.hll.AddJSONString(encoded)
		return nil
	}
	if _, exists := state.exact[encoded]; exists {
		return nil
	}
	if len(state.exact) < state.exactLimit {
		if state.exact == nil {
			state.exact = make(map[string]struct{})
		}
		state.exact[encoded] = struct{}{}
		return nil
	}
	return state.promote(encoded)
}

func (state *sqlAutoDistinctState) promote(next string) error {
	sketch, err := hatDataStructure.NewHyperLogLog(state.precision)
	if err != nil {
		return err
	}
	for encoded := range state.exact {
		sketch.AddJSONString(encoded)
	}
	sketch.AddJSONString(next)
	state.hll = &sketch
	state.exact = nil
	return nil
}

func (state *sqlAutoDistinctState) count() uint64 {
	if state.hll != nil {
		return state.hll.Count()
	}
	return uint64(len(state.exact))
}

func evalSQLAutoCountDistinct(expr sqlExpr, group []sqlExecRow) interface{} {
	config, err := sqlAutoDistinctConfigFromExpr(expr)
	if err != nil {
		return sqlApproximateAggregateError(expr, err.Error())
	}
	state, err := newSQLAutoDistinctState(config)
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
		if err := state.addValue(value); err != nil {
			return sqlApproximateAggregateError(expr, err.Error())
		}
	}
	return state.count()
}
