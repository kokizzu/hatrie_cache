package hatSql

import (
	"fmt"
	"strings"
	"time"
)

const (
	sqlGroupingSetsProjectionLiteral = iota
	sqlGroupingSetsProjectionDimension
	sqlGroupingSetsProjectionGrouping
	sqlGroupingSetsProjectionAggregate
)

type sqlGroupingSetsOnePassProjection struct {
	kind      int
	dimension int
	literal   interface{}
	aggregate sqlOrderedAggregate
}

type sqlGroupingSetsOnePassState struct {
	dimensions []interface{}
	aggregates []sqlOrderedAggregate
	rows       int
}

type sqlGroupingSetsOnePassBucket struct {
	indexes map[string]int
	states  []sqlGroupingSetsOnePassState
}

func sqlGroupingSetsOnePassDimensionIndex(dimensions []sqlExpr, expression sqlExpr) int {
	for index, dimension := range dimensions {
		if sqlSameGroupingExpr(dimension, expression) {
			return index
		}
	}
	return -1
}

func sqlGroupingSetsOnePassAggregate(expression sqlExpr) (sqlOrderedAggregate, bool) {
	if expression.kind != "func" || expression.filter != nil || expression.window != nil {
		return sqlOrderedAggregate{}, false
	}
	aggregate := sqlOrderedAggregate{name: strings.ToUpper(expression.name)}
	switch aggregate.name {
	case "COUNT":
		if len(expression.args) == 0 || len(expression.args) == 1 && expression.args[0].kind == "star" {
			return aggregate, true
		}
		if len(expression.args) != 1 || expression.args[0].kind != "field" {
			return sqlOrderedAggregate{}, false
		}
		aggregate.field = expression.args[0]
	case "SUM", "AVG", "MIN", "MAX":
		if len(expression.args) != 1 || expression.args[0].kind != "field" {
			return sqlOrderedAggregate{}, false
		}
		aggregate.field = expression.args[0]
	default:
		return sqlOrderedAggregate{}, false
	}
	return aggregate, true
}

func sqlGroupingSetsOnePassProjections(query *sqlQuery) ([]sqlGroupingSetsOnePassProjection, bool) {
	if query == nil || len(query.groupingDimensions) == 0 || len(query.groupingDimensions) > maxSQLCubeDimensions {
		return nil, false
	}
	projections := make([]sqlGroupingSetsOnePassProjection, len(query.selects))
	for index, item := range query.selects {
		expression := item.expr
		switch expression.kind {
		case "literal":
			projections[index] = sqlGroupingSetsOnePassProjection{kind: sqlGroupingSetsProjectionLiteral, literal: expression.value}
		case "field":
			dimension := sqlGroupingSetsOnePassDimensionIndex(query.groupingDimensions, expression)
			if dimension < 0 {
				return nil, false
			}
			projections[index] = sqlGroupingSetsOnePassProjection{kind: sqlGroupingSetsProjectionDimension, dimension: dimension}
		case "func":
			if strings.EqualFold(expression.name, "GROUPING") {
				if len(expression.args) != 1 {
					return nil, false
				}
				dimension := sqlGroupingSetsOnePassDimensionIndex(query.groupingDimensions, expression.args[0])
				if dimension < 0 {
					return nil, false
				}
				projections[index] = sqlGroupingSetsOnePassProjection{kind: sqlGroupingSetsProjectionGrouping, dimension: dimension}
				continue
			}
			aggregate, ok := sqlGroupingSetsOnePassAggregate(expression)
			if !ok {
				return nil, false
			}
			projections[index] = sqlGroupingSetsOnePassProjection{kind: sqlGroupingSetsProjectionAggregate, aggregate: aggregate}
		default:
			return nil, false
		}
	}
	return projections, true
}

func sqlGroupingSetsOnePassEligible(query *sqlQuery) bool {
	if query == nil || len(query.groupingSets) < 2 || query.explain || query.from == nil || len(query.ctes) != 0 || len(query.joins) != 0 || len(query.unions) != 0 || len(query.orderBy) != 0 || query.limitBy != nil || query.limitWithTies || query.limit >= 0 || query.offset != 0 || query.distinct || query.having.kind != "" || query.qualify.kind != "" || query.sample != nil || query.prewhere.kind != "" || !sqlQueryHasAggregate(query) {
		return false
	}
	if sqlExprHasAggregate(query.where) || sqlExprHasSubqueryExpression(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	_, ok := sqlGroupingSetsOnePassProjections(query)
	return ok
}

func sqlGroupingSetsOnePassKey(groupingSet, dimensions []sqlExpr, row sqlExecRow) (string, []interface{}, error) {
	parts := make([]string, len(groupingSet))
	values := make([]interface{}, len(dimensions))
	one := [1]sqlExecRow{row}
	for index, expression := range groupingSet {
		value := evalSQLExpr(expression, one[:], row)
		if err := sqlExpressionError(value); err != nil {
			return "", nil, err
		}
		parts[index] = sqlCollationValueKey(expression.collation, value)
		dimension := sqlGroupingSetsOnePassDimensionIndex(dimensions, expression)
		if dimension >= 0 {
			values[dimension] = value
		}
	}
	return strings.Join(parts, "\x00"), values, nil
}

func sqlGroupingSetsOnePassContains(groupingSet []sqlExpr, dimensions []sqlExpr, dimension int) bool {
	return dimension >= 0 && dimension < len(dimensions) && sqlGroupingSetContains(groupingSet, dimensions[dimension])
}

func executeSQLGroupingSetsOnePass(q *sqlQuery, rows []sqlExecRow, control *sqlExecutionControl, metrics *sqlExecutionMetrics) (SQLQueryResult, bool, error) {
	projections, ok := sqlGroupingSetsOnePassProjections(q)
	if !sqlGroupingSetsOnePassEligible(q) || !ok {
		return SQLQueryResult{}, false, nil
	}
	started := time.Now()
	buckets := make([]sqlGroupingSetsOnePassBucket, len(q.groupingSets))
	for index := range buckets {
		buckets[index].indexes = make(map[string]int)
	}
	for _, row := range rows {
		if control != nil {
			if err := control.check(); err != nil {
				return SQLQueryResult{}, true, err
			}
		}
		for groupingSetIndex, groupingSet := range q.groupingSets {
			key, dimensions, err := sqlGroupingSetsOnePassKey(groupingSet, q.groupingDimensions, row)
			if err != nil {
				return SQLQueryResult{}, true, err
			}
			bucket := &buckets[groupingSetIndex]
			stateIndex, exists := bucket.indexes[key]
			if !exists {
				if control != nil && control.options.MaxGroupKeys > 0 && len(bucket.states) >= control.options.MaxGroupKeys {
					return SQLQueryResult{}, true, fmt.Errorf("SQL group key limit exceeded: query produced more than %d groups", control.options.MaxGroupKeys)
				}
				stateIndex = len(bucket.states)
				bucket.indexes[key] = stateIndex
				state := sqlGroupingSetsOnePassState{dimensions: dimensions, aggregates: make([]sqlOrderedAggregate, len(projections))}
				for projectionIndex, projection := range projections {
					if projection.kind == sqlGroupingSetsProjectionAggregate {
						state.aggregates[projectionIndex] = projection.aggregate
					}
				}
				bucket.states = append(bucket.states, state)
			}
			state := &bucket.states[stateIndex]
			state.rows++
			if control != nil && control.options.MaxGroupRowsPerKey > 0 && state.rows > control.options.MaxGroupRowsPerKey {
				return SQLQueryResult{}, true, fmt.Errorf("SQL group skew limit exceeded: group has %d rows, maximum %d", state.rows, control.options.MaxGroupRowsPerKey)
			}
			for projectionIndex, projection := range projections {
				if projection.kind != sqlGroupingSetsProjectionAggregate {
					continue
				}
				if err := state.aggregates[projectionIndex].add(row); err != nil {
					return SQLQueryResult{}, true, err
				}
			}
		}
	}
	for index := range q.groupingSets {
		if len(buckets[index].states) != 0 || len(rows) != 0 {
			continue
		}
		state := sqlGroupingSetsOnePassState{aggregates: make([]sqlOrderedAggregate, len(projections))}
		for projectionIndex, projection := range projections {
			if projection.kind == sqlGroupingSetsProjectionAggregate {
				state.aggregates[projectionIndex] = projection.aggregate
			}
		}
		buckets[index].states = append(buckets[index].states, state)
		buckets[index].indexes[""] = 0
	}
	columns := sqlColumns(q.selects)
	result := SQLQueryResult{Columns: columns, Rows: make([]SQLRow, 0)}
	resultBytes := 0
	for groupingSetIndex, groupingSet := range q.groupingSets {
		for _, state := range buckets[groupingSetIndex].states {
			row := make(SQLRow, len(columns))
			for projectionIndex, projection := range projections {
				switch projection.kind {
				case sqlGroupingSetsProjectionLiteral:
					row[columns[projectionIndex]] = projection.literal
				case sqlGroupingSetsProjectionDimension:
					if sqlGroupingSetsOnePassContains(groupingSet, q.groupingDimensions, projection.dimension) {
						row[columns[projectionIndex]] = state.dimensions[projection.dimension]
					} else {
						row[columns[projectionIndex]] = nil
					}
				case sqlGroupingSetsProjectionGrouping:
					if sqlGroupingSetsOnePassContains(groupingSet, q.groupingDimensions, projection.dimension) {
						row[columns[projectionIndex]] = int64(0)
					} else {
						row[columns[projectionIndex]] = int64(1)
					}
				case sqlGroupingSetsProjectionAggregate:
					row[columns[projectionIndex]] = state.aggregates[projectionIndex].value()
				}
			}
			result.Rows = append(result.Rows, row)
			resultBytes += sqlRowBytes(row)
			if control != nil && control.options.MaxResultBytes > 0 && resultBytes > control.options.MaxResultBytes {
				return SQLQueryResult{}, true, fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
	}
	if metrics != nil {
		metrics.record("GROUPING SETS ONE PASS", fmt.Sprintf("sets=%d dimensions=%d", len(q.groupingSets), len(q.groupingDimensions)), len(rows), len(result.Rows), started)
		metrics.record("PROJECT", sqlExplainSelects(q.selects), len(result.Rows), len(result.Rows), started)
	}
	return result, true, nil
}
