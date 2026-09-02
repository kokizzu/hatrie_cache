package hatSql

import (
	"fmt"
	"strings"
	"time"
)

func sqlPrewhereStreamable(query *sqlQuery, resolver SQLSourceResolver) bool {
	if query == nil || resolver == nil || query.from == nil || query.where.kind == "" || query.limit == 0 || query.sample != nil || query.explain || query.from.kind != "CACHE" || len(query.from.fieldTypes) != 0 || len(query.joins) != 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || len(query.orderBy) != 0 || sqlQueryHasSubqueryExpression(query) {
		return false
	}
	if _, ok := resolver.(SQLStreamSourceResolver); !ok {
		return false
	}
	functions, _ := resolver.(SQLFunctionResolver)
	for _, selectItem := range query.selects {
		if selectItem.expr.kind == "star" || selectItem.expr.window != nil || sqlExprHasAggregate(selectItem.expr) || sqlExprHasCustomFunction(selectItem.expr, functions) {
			return false
		}
	}
	if sqlExprHasCustomFunction(query.where, functions) {
		return false
	}
	return true
}

// executeSQLPrewhereScan filters a stream before allocating projected result
// rows. It is limited to single-source queries with no ordering or relational
// state, so streamed LIMIT behavior and source order are direct and exact.
func executeSQLPrewhereScan(query *sqlQuery, resolver SQLSourceResolver, ctes map[string][]SQLRow, metrics *sqlExecutionMetrics, control *sqlExecutionControl, outer *sqlExecRow) (SQLQueryResult, bool, error) {
	if !sqlPrewhereStreamable(query, resolver) {
		return SQLQueryResult{}, false, nil
	}
	started := time.Now()
	columns := sqlColumns(query.selects)
	result := SQLQueryResult{Columns: columns, Rows: make([]SQLRow, 0)}
	inputRows, matched, resultBytes := 0, 0, 0
	var materializeStarted time.Time
	execRows := make([]sqlExecRow, 1)
	environment := &sqlEvalEnvironment{resolver: resolver, ctes: ctes, metrics: metrics, control: control}
	visit := func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if control != nil && inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		execRows[0] = newSQLSingleSourceExecRow(query.from.alias, sourceRow)
		sqlAttachSQLExecutionEnvironment(execRows, outer, environment)
		execRow := execRows[0]
		value := evalSQLExpr(query.where, execRows, execRow)
		if err := sqlExpressionError(value); err != nil {
			return err
		}
		if !sqlTruthy(value) {
			return nil
		}
		matched++
		emit := matched > query.offset && (query.limit < 0 || len(result.Rows) < query.limit)
		var row SQLRow
		if emit {
			if materializeStarted.IsZero() {
				materializeStarted = time.Now()
			}
			row = make(SQLRow, len(columns))
		}
		for index, selectItem := range query.selects {
			value := evalSQLExpr(selectItem.expr, execRows, execRow)
			if err := sqlExpressionError(value); err != nil {
				return err
			}
			if emit {
				row[columns[index]] = value
			}
		}
		if !emit {
			return nil
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		result.Rows = append(result.Rows, row)
		return nil
	}

	err := streamSQLSourceRows(control.ctx, *query.from, resolver, visit)
	if err != nil {
		return SQLQueryResult{}, true, err
	}
	if metrics != nil {
		metrics.record("PREWHERE SCAN", sqlExplainExpression(query.where), inputRows, matched, started)
		if !materializeStarted.IsZero() {
			metrics.record("LATE MATERIALIZATION", strings.Join(columns, ","), matched, len(result.Rows), materializeStarted)
		}
		if query.limit >= 0 || query.offset > 0 {
			metrics.record("LIMIT", fmt.Sprintf("limit=%d offset=%d", query.limit, query.offset), matched, len(result.Rows), started)
		}
	}
	return result, true, nil
}
