package hatSql

import (
	"fmt"
	"sync"
	"time"
)

const sqlColumnarVectorGroupBlockRows = 1024
const sqlColumnarVectorTwoLevelMinimumRows = 16 * sqlColumnarVectorGroupBlockRows
const sqlColumnarVectorTwoLevelMaxWorkers = 16

func sqlColumnarVectorGroupAggregatePlan(q *sqlQuery, outer *sqlExecRow) (string, []sqlOrderedGroupProjection, []string, bool) {
	if q == nil || outer != nil || q.explain || q.from == nil || q.from.kind != "CACHE" || len(q.from.fieldTypes) != 0 || len(q.ctes) != 0 || len(q.joins) != 0 || len(q.unions) != 0 || len(q.groupBy) != 1 || len(q.groupingSets) != 0 || q.having.kind != "" || q.distinct || len(q.orderBy) != 0 || q.limitBy != nil || q.sample != nil || sqlQueryHasWindow(q) || sqlQueryHasSubqueryExpression(q) || q.where.window != nil || sqlExprHasAggregate(q.where) || sqlExprHasWindow(q.where) {
		return "", nil, nil, false
	}
	if q.groupBy[0].kind != "field" {
		return "", nil, nil, false
	}
	groupField := ""
	if !sqlColumnarAggregateField(q.groupBy[0], q.from.alias, &groupField) {
		return "", nil, nil, false
	}
	if q.where.kind != "" && !sqlColumnarPredicateFields(q.where, q.from.alias, nil) {
		return "", nil, nil, false
	}
	for _, item := range q.selects {
		if item.expr.filter != nil || item.expr.window != nil {
			return "", nil, nil, false
		}
	}
	projections, ok := sqlOrderedGroupProjections(q)
	if !ok {
		return "", nil, nil, false
	}
	seen := map[string]bool{}
	fields := make([]string, 0, len(q.selects)+1)
	addField := func(field string) {
		if field != "" && !seen[field] {
			seen[field] = true
			fields = append(fields, field)
		}
	}
	addField(groupField)
	for _, projection := range projections {
		if projection.aggregate == nil || projection.aggregate.field.kind == "" {
			continue
		}
		field := ""
		if !sqlColumnarAggregateField(projection.aggregate.field, q.from.alias, &field) {
			return "", nil, nil, false
		}
		addField(field)
	}
	return groupField, projections, fields, true
}

func executeSQLColumnarVectorGroupAggregate(q *sqlQuery, columnar SQLColumnarSourceResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics, outer *sqlExecRow) (SQLQueryResult, bool, error) {
	if control != nil && control.options.MaxGroupBytes > 0 {
		return SQLQueryResult{}, false, nil
	}
	groupField, projections, fields, ok := sqlColumnarVectorGroupAggregatePlan(q, outer)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	batch, _, available, err := resolveSQLColumnarSource(columnar, q.from.kind, q.from.key, fields)
	if err != nil || !available {
		return SQLQueryResult{}, available, err
	}
	functions, _ := columnar.(SQLFunctionResolver)
	return executeSQLColumnarVectorGroupAggregateBatch(q, batch, functions, groupField, projections, fields, control, metrics, nil)
}

func executeSQLColumnarVectorGroupAggregateRows(q *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics, visit func([]string, SQLRow) error) (SQLQueryResult, bool, error) {
	columnar, ok := resolver.(SQLColumnarSourceResolver)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	if control != nil && control.options.MaxGroupBytes > 0 {
		return SQLQueryResult{}, false, nil
	}
	groupField, projections, fields, ok := sqlColumnarVectorGroupAggregatePlan(q, nil)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	batch, _, available, err := resolveSQLColumnarSource(columnar, q.from.kind, q.from.key, fields)
	if err != nil || !available {
		return SQLQueryResult{}, available, err
	}
	functions, _ := resolver.(SQLFunctionResolver)
	return executeSQLColumnarVectorGroupAggregateBatch(q, batch, functions, groupField, projections, fields, control, metrics, visit)
}

func executeSQLColumnarVectorGroupAggregateBatchFromQuery(q *sqlQuery, batch ColumnarBatch, functions SQLFunctionResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics, visit func([]string, SQLRow) error) (SQLQueryResult, bool, error) {
	groupField, projections, fields, ok := sqlColumnarVectorGroupAggregatePlan(q, nil)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	return executeSQLColumnarVectorGroupAggregateBatch(q, batch, functions, groupField, projections, fields, control, metrics, visit)
}

// executeSQLColumnarVectorGroupAggregateBatch evaluates one columnar source in
// fixed-size blocks. The selection slice keeps rejected rows out of the hash
// table and lets the aggregate loop operate on aligned source columns without
// constructing a sqlExecRow or source map for every input row.
func executeSQLColumnarVectorGroupAggregateBatch(q *sqlQuery, batch ColumnarBatch, functions SQLFunctionResolver, groupField string, projections []sqlOrderedGroupProjection, fields []string, control *sqlExecutionControl, metrics *sqlExecutionMetrics, visit func([]string, SQLRow) error) (SQLQueryResult, bool, error) {
	if control != nil && control.options.MaxGroupBytes > 0 {
		return SQLQueryResult{}, false, nil
	}
	if batch.Rows < 0 {
		return SQLQueryResult{}, true, fmt.Errorf("SQL columnar source %q returned a negative row count", q.from.key)
	}
	if control != nil && batch.Rows > control.maxRows {
		return SQLQueryResult{}, true, fmt.Errorf("SQL source %q exceeds the %d row limit", q.from.alias, control.maxRows)
	}
	for _, field := range fields {
		if batch.FieldRows(field) != batch.Rows {
			return SQLQueryResult{}, true, fmt.Errorf("SQL columnar source %q returned %d values for field %q, want %d", q.from.key, batch.FieldRows(field), field, batch.Rows)
		}
	}

	started := time.Now()
	match := sqlColumnarQueryRowsMatcher(q, batch, functions)
	twoLevelWorkers := sqlColumnarVectorTwoLevelWorkers(batch.Rows, functions, control, projections)
	var states []sqlHashGroupAggregateState
	var matched int
	var err error
	if twoLevelWorkers > 1 {
		states, matched, err = executeSQLColumnarTwoLevelGroupStates(q, batch, projections, groupField, match, control, twoLevelWorkers)
	} else {
		states, matched, err = executeSQLColumnarSingleLevelGroupStates(q, batch, projections, groupField, match, control)
	}
	if err != nil {
		return SQLQueryResult{}, true, err
	}

	columns := sqlColumns(q.selects)
	result := SQLQueryResult{Columns: columns, Rows: make([]SQLRow, 0, len(states))}
	emitted, position, resultBytes := 0, 0, 0
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
		node := "COLUMNAR VECTOR GROUP AGGREGATE"
		detail := sqlExplainExpressions(q.groupBy)
		if twoLevelWorkers > 1 {
			node = "COLUMNAR TWO-LEVEL GROUP AGGREGATE"
			detail = fmt.Sprintf("%s workers=%d matched=%d", detail, twoLevelWorkers, matched)
		}
		metrics.record(node, detail, batch.Rows, len(states), started)
		metrics.record("PROJECT", sqlExplainSelects(q.selects), len(states), emitted, started)
		if q.limit >= 0 || q.offset > 0 {
			metrics.record("LIMIT", fmt.Sprintf("limit=%d offset=%d", q.limit, q.offset), len(states), emitted, started)
		}
	}
	return result, true, nil
}

func sqlColumnarVectorTwoLevelWorkers(rows int, functions SQLFunctionResolver, control *sqlExecutionControl, projections []sqlOrderedGroupProjection) int {
	if rows < sqlColumnarVectorTwoLevelMinimumRows || functions != nil || control == nil || control.options.Workers < 2 || !sqlColumnarTwoLevelAggregatesSupported(projections) {
		return 1
	}
	workers := control.options.Workers
	blocks := (rows + sqlColumnarVectorGroupBlockRows - 1) / sqlColumnarVectorGroupBlockRows
	if workers > blocks {
		workers = blocks
	}
	if workers > sqlColumnarVectorTwoLevelMaxWorkers {
		workers = sqlColumnarVectorTwoLevelMaxWorkers
	}
	return workers
}

func sqlColumnarTwoLevelAggregatesSupported(projections []sqlOrderedGroupProjection) bool {
	aggregateCount := 0
	for _, projection := range projections {
		if projection.aggregate == nil {
			continue
		}
		aggregateCount++
		switch projection.aggregate.name {
		case "COUNT", "MIN", "MAX":
		default:
			return false
		}
	}
	return aggregateCount >= 2
}

func executeSQLColumnarSingleLevelGroupStates(q *sqlQuery, batch ColumnarBatch, projections []sqlOrderedGroupProjection, groupField string, match func(int) (bool, error), control *sqlExecutionControl) ([]sqlHashGroupAggregateState, int, error) {
	states := make([]sqlHashGroupAggregateState, 0)
	indexes := make(map[string]int)
	selectionCapacity := sqlColumnarVectorGroupBlockRows
	if batch.Rows < selectionCapacity {
		selectionCapacity = batch.Rows
	}
	selection := make([]int, 0, selectionCapacity)
	matched := 0
	for blockStart := 0; blockStart < batch.Rows; blockStart += sqlColumnarVectorGroupBlockRows {
		blockEnd := blockStart + sqlColumnarVectorGroupBlockRows
		if blockEnd > batch.Rows {
			blockEnd = batch.Rows
		}
		selection = selection[:0]
		for rowIndex := blockStart; rowIndex < blockEnd; rowIndex++ {
			if control != nil {
				if err := control.check(); err != nil {
					return nil, matched, err
				}
			}
			matches, err := match(rowIndex)
			if err != nil {
				return nil, matched, err
			}
			if matches {
				selection = append(selection, rowIndex)
			}
		}
		for _, rowIndex := range selection {
			groupValue, _ := batch.Value(groupField, rowIndex)
			key := sqlCollationValueKey(q.groupBy[0].collation, groupValue)
			stateIndex, exists := indexes[key]
			if !exists {
				stateIndex = len(states)
				indexes[key] = stateIndex
				state := sqlHashGroupAggregateState{value: groupValue, aggregates: make([]sqlOrderedAggregate, len(projections))}
				for projectionIndex, projection := range projections {
					if projection.aggregate != nil {
						state.aggregates[projectionIndex] = *projection.aggregate
					}
				}
				states = append(states, state)
			}
			state := &states[stateIndex]
			state.rows++
			if control != nil && control.options.MaxGroupRowsPerKey > 0 && state.rows > control.options.MaxGroupRowsPerKey {
				return nil, matched, fmt.Errorf("SQL group skew limit exceeded: group has %d rows, maximum %d", state.rows, control.options.MaxGroupRowsPerKey)
			}
			for projectionIndex, projection := range projections {
				if projection.aggregate == nil {
					continue
				}
				aggregate := &state.aggregates[projectionIndex]
				value := interface{}(nil)
				if aggregate.field.kind != "" {
					value, _ = batch.Value(aggregate.field.name, rowIndex)
				}
				if err := aggregate.addValue(value); err != nil {
					return nil, matched, err
				}
			}
			matched++
		}
	}
	return states, matched, nil
}

type sqlColumnarTwoLevelWorkerState struct {
	states  []sqlHashGroupAggregateState
	keys    []string
	indexes map[string]int
	matched int
	err     error
}

func executeSQLColumnarTwoLevelGroupStates(q *sqlQuery, batch ColumnarBatch, projections []sqlOrderedGroupProjection, groupField string, match func(int) (bool, error), control *sqlExecutionControl, workers int) ([]sqlHashGroupAggregateState, int, error) {
	local := make([]sqlColumnarTwoLevelWorkerState, workers)
	var wait sync.WaitGroup
	wait.Add(workers)
	for workerIndex := 0; workerIndex < workers; workerIndex++ {
		start := batch.Rows * workerIndex / workers
		end := batch.Rows * (workerIndex + 1) / workers
		go func(index, start, end int) {
			defer wait.Done()
			worker := &local[index]
			worker.indexes = make(map[string]int)
			for rowIndex := start; rowIndex < end; rowIndex++ {
				if control != nil {
					if err := control.check(); err != nil {
						worker.err = err
						return
					}
				}
				matches, err := match(rowIndex)
				if err != nil {
					worker.err = err
					return
				}
				if !matches {
					continue
				}
				groupValue, _ := batch.Value(groupField, rowIndex)
				key := sqlCollationValueKey(q.groupBy[0].collation, groupValue)
				stateIndex, exists := worker.indexes[key]
				if !exists {
					stateIndex = len(worker.states)
					worker.indexes[key] = stateIndex
					state := sqlHashGroupAggregateState{value: groupValue, aggregates: make([]sqlOrderedAggregate, len(projections))}
					for projectionIndex, projection := range projections {
						if projection.aggregate != nil {
							state.aggregates[projectionIndex] = *projection.aggregate
						}
					}
					worker.states = append(worker.states, state)
					worker.keys = append(worker.keys, key)
				}
				state := &worker.states[stateIndex]
				state.rows++
				for projectionIndex, projection := range projections {
					if projection.aggregate == nil {
						continue
					}
					aggregate := &state.aggregates[projectionIndex]
					value := interface{}(nil)
					if aggregate.field.kind != "" {
						value, _ = batch.Value(aggregate.field.name, rowIndex)
					}
					if err := aggregate.addValue(value); err != nil {
						worker.err = err
						return
					}
				}
				worker.matched++
			}
		}(workerIndex, start, end)
	}
	wait.Wait()

	states := make([]sqlHashGroupAggregateState, 0)
	indexes := make(map[string]int)
	matched := 0
	for workerIndex := range local {
		worker := &local[workerIndex]
		if worker.err != nil {
			return nil, matched, worker.err
		}
		matched += worker.matched
		for stateIndex, state := range worker.states {
			key := worker.keys[stateIndex]
			globalIndex, exists := indexes[key]
			if !exists {
				indexes[key] = len(states)
				states = append(states, state)
				continue
			}
			merged := &states[globalIndex]
			merged.rows += state.rows
			for projectionIndex := range merged.aggregates {
				mergeSQLOrderedAggregate(&merged.aggregates[projectionIndex], state.aggregates[projectionIndex])
			}
		}
	}
	if control != nil && control.options.MaxGroupRowsPerKey > 0 {
		for _, state := range states {
			if state.rows > control.options.MaxGroupRowsPerKey {
				return nil, matched, fmt.Errorf("SQL group skew limit exceeded: group has %d rows, maximum %d", state.rows, control.options.MaxGroupRowsPerKey)
			}
		}
	}
	return states, matched, nil
}
