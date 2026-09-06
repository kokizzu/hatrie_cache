package hatSql

import (
	"fmt"
	"time"
)

// sqlHashGroupAggregatePlan accepts only grouped projections whose state can be
// updated one row at a time. Other grouped queries retain the materialized
// executor because representative rows, HAVING, windows, and custom functions
// can require the full group.
func sqlHashGroupAggregatePlan(query *sqlQuery) bool {
	if query == nil || query.explain || query.from == nil || len(query.groupBy) != 1 || len(query.orderBy) != 0 || query.limitBy != nil {
		return false
	}
	_, ok := sqlOrderedGroupProjections(query)
	return ok
}

func sqlHashGroupAggregateStreamable(query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl) bool {
	if sqlQueryHasWithFill(query) || !sqlHashGroupAggregatePlan(query) || control == nil || control.options.MaxGroupBytes > 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || query.sample != nil || len(query.from.fieldTypes) != 0 || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	if _, ok := resolver.(SQLColumnarSourceResolver); ok {
		return false
	}
	if query.from.kind == "VALUES" {
		return true
	}
	if query.from.kind != "CACHE" {
		return false
	}
	_, ok := resolver.(SQLStreamSourceResolver)
	return ok
}

type sqlHashGroupAggregateState struct {
	value      interface{}
	rows       int
	aggregates []sqlOrderedAggregate
}

func (aggregate *sqlOrderedAggregate) addValue(value interface{}) error {
	if aggregate.name == "COUNT" && aggregate.field.kind == "" {
		aggregate.count++
		return nil
	}
	if aggregate.name == "COUNT" {
		if value != nil {
			aggregate.count++
		}
		return nil
	}
	number, ok := sqlNumber(value)
	if !ok {
		return nil
	}
	if !aggregate.seen {
		aggregate.seen = true
		aggregate.sum = number
		aggregate.min = number
		aggregate.max = number
		aggregate.count = 1
		return nil
	}
	aggregate.count++
	switch aggregate.name {
	case "SUM", "AVG":
		aggregate.sum += number
	case "MIN":
		if number < aggregate.min {
			aggregate.min = number
		}
	case "MAX":
		if number > aggregate.max {
			aggregate.max = number
		}
	}
	return nil
}

func mergeSQLOrderedAggregate(destination *sqlOrderedAggregate, source sqlOrderedAggregate) {
	if destination == nil || source.name == "" {
		return
	}
	if destination.name == "COUNT" {
		destination.count += source.count
		return
	}
	if !source.seen {
		return
	}
	if !destination.seen {
		destination.seen = true
		destination.sum = source.sum
		destination.min = source.min
		destination.max = source.max
		destination.count = source.count
		return
	}
	destination.count += source.count
	switch destination.name {
	case "SUM", "AVG":
		destination.sum += source.sum
	case "MIN":
		if source.min < destination.min {
			destination.min = source.min
		}
	case "MAX":
		if source.max > destination.max {
			destination.max = source.max
		}
	}
}

func executeSQLHashGroupAggregateRows(q *sqlQuery, stream func(func(sqlExecRow) error) error, control *sqlExecutionControl, metrics *sqlExecutionMetrics, visit func([]string, SQLRow) error) (SQLQueryResult, bool, error) {
	if control != nil && control.options.MaxGroupBytes > 0 {
		return SQLQueryResult{}, false, nil
	}
	projections, ok := sqlOrderedGroupProjections(q)
	if !sqlHashGroupAggregatePlan(q) || !ok {
		return SQLQueryResult{}, false, nil
	}
	started := time.Now()
	states := make([]sqlHashGroupAggregateState, 0)
	indexes := make(map[string]int)
	inputRows := 0
	var one [1]sqlExecRow
	err := stream(func(row sqlExecRow) error {
		if control != nil {
			if err := control.check(); err != nil {
				return err
			}
		}
		one[0] = row
		value := evalSQLExpr(q.groupBy[0], one[:], row)
		if err := sqlExpressionError(value); err != nil {
			return err
		}
		key := sqlCollationValueKey(q.groupBy[0].collation, value)
		index, exists := indexes[key]
		if !exists {
			index = len(states)
			indexes[key] = index
			state := sqlHashGroupAggregateState{value: value, aggregates: make([]sqlOrderedAggregate, len(projections))}
			for projectionIndex, projection := range projections {
				if projection.aggregate != nil {
					state.aggregates[projectionIndex] = *projection.aggregate
				}
			}
			states = append(states, state)
		}
		state := &states[index]
		state.rows++
		if control != nil && control.options.MaxGroupRowsPerKey > 0 && state.rows > control.options.MaxGroupRowsPerKey {
			return fmt.Errorf("SQL group skew limit exceeded: group has %d rows, maximum %d", state.rows, control.options.MaxGroupRowsPerKey)
		}
		for projectionIndex, projection := range projections {
			if projection.aggregate == nil {
				continue
			}
			aggregate := &state.aggregates[projectionIndex]
			value := interface{}(nil)
			if aggregate.field.kind != "" {
				value = evalSQLExpr(aggregate.field, one[:], row)
				if err := sqlExpressionError(value); err != nil {
					return err
				}
			}
			if err := aggregate.addValue(value); err != nil {
				return err
			}
		}
		inputRows++
		return nil
	})
	if err != nil {
		return SQLQueryResult{}, true, err
	}
	columns := sqlColumns(q.selects)
	result := SQLQueryResult{Columns: columns, Rows: make([]SQLRow, 0, len(states))}
	emitted := 0
	position := 0
	resultBytes := 0
	for _, state := range states {
		if position < q.offset {
			position++
			continue
		}
		if q.limit >= 0 && emitted >= q.limit {
			break
		}
		row := make(SQLRow, len(columns))
		for projectionIndex, projection := range projections {
			if projection.group {
				row[columns[projectionIndex]] = state.value
				continue
			}
			row[columns[projectionIndex]] = state.aggregates[projectionIndex].value()
		}
		resultBytes += sqlRowBytes(row)
		if control != nil && control.options.MaxResultBytes > 0 && resultBytes > control.options.MaxResultBytes {
			return SQLQueryResult{}, true, fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
		}
		if visit != nil {
			if err := visit(columns, row); err != nil {
				return SQLQueryResult{}, true, err
			}
		} else {
			result.Rows = append(result.Rows, row)
		}
		emitted++
		position++
	}
	if metrics != nil {
		metrics.record("HASH AGGREGATE", sqlExplainExpressions(q.groupBy), inputRows, len(states), started)
		metrics.record("PROJECT", sqlExplainSelects(q.selects), len(states), emitted, started)
		if q.limit >= 0 || q.offset > 0 {
			metrics.record("LIMIT", fmt.Sprintf("limit=%d offset=%d", q.limit, q.offset), len(states), emitted, started)
		}
	}
	return result, true, nil
}

func executeSQLHashGroupAggregateStream(q *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics, visit func([]string, SQLRow) error) (SQLQueryResult, bool, error) {
	if sqlQueryHasWithFill(q) {
		return SQLQueryResult{}, false, nil
	}
	if !sqlHashGroupAggregateStreamable(q, resolver, control) {
		return SQLQueryResult{}, false, nil
	}
	inputRows := 0
	result, handled, err := executeSQLHashGroupAggregateRows(q, func(consume func(sqlExecRow) error) error {
		return streamSQLSourceRows(control.ctx, *q.from, resolver, func(sourceRow SQLRow) error {
			if err := control.check(); err != nil {
				return err
			}
			inputRows++
			if inputRows > control.maxRows {
				return fmt.Errorf("SQL source %q exceeds the %d row limit", q.from.alias, control.maxRows)
			}
			row := sqlExecRow{sources: map[string]SQLRow{q.from.alias: sourceRow}, order: []string{q.from.alias}}
			if q.where.kind != "" {
				value := evalSQLExpr(q.where, []sqlExecRow{row}, row)
				if err := sqlExpressionError(value); err != nil {
					return err
				}
				if !sqlTruthy(value) {
					return nil
				}
			}
			return consume(row)
		})
	}, control, metrics, visit)
	return result, handled, err
}
