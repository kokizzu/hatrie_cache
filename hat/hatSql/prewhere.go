package hatSql

import (
	"fmt"
	"strings"
	"time"
)

func sqlPrewhereStreamable(query *sqlQuery, resolver SQLSourceResolver) bool {
	if query == nil || resolver == nil || query.from == nil || query.prewhere.kind == "" && query.where.kind == "" || query.limit == 0 || query.sample != nil || query.explain || query.from.kind != "CACHE" || len(query.from.fieldTypes) != 0 || len(query.joins) != 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || len(query.orderBy) != 0 || sqlQueryHasSubqueryExpression(query) {
		return false
	}
	if _, ok := resolver.(SQLStreamSourceResolver); !ok {
		return false
	}
	switch resolver.(type) {
	case SQLColumnarSourceResolver, SQLIndexedSourceResolver, SQLOrderedSourceResolver, SQLOrderedStreamSourceResolver:
		return false
	}
	functions, _ := resolver.(SQLFunctionResolver)
	for _, selectItem := range query.selects {
		if selectItem.expr.kind == "star" || selectItem.expr.window != nil || sqlExprHasAggregate(selectItem.expr) || sqlExprHasCustomFunction(selectItem.expr, functions) {
			return false
		}
	}
	if sqlExprHasCustomFunction(query.prewhere, functions) || sqlExprHasCustomFunction(query.where, functions) {
		return false
	}
	return true
}

func sqlCombinedWhere(query *sqlQuery) sqlExpr {
	if query == nil || query.prewhere.kind == "" {
		if query == nil {
			return sqlExpr{}
		}
		return query.where
	}
	if query.where.kind == "" {
		return query.prewhere
	}
	left := query.prewhere
	right := query.where
	return sqlExpr{kind: "binary", op: "AND", left: &left, right: &right, token: query.prewhere.token}
}

func sqlQueryWithCombinedPrewhere(query *sqlQuery) *sqlQuery {
	if query == nil || query.prewhere.kind == "" {
		return query
	}
	clone := *query
	clone.where = sqlCombinedWhere(query)
	clone.prewhere = sqlExpr{}
	return &clone
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
		if query.prewhere.kind != "" {
			value := evalSQLExpr(query.prewhere, execRows, execRow)
			if err := sqlExpressionError(value); err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		if query.where.kind != "" {
			value := evalSQLExpr(query.where, execRows, execRow)
			if err := sqlExpressionError(value); err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
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

	err := streamSQLSourceRowsWithPartitionPredicates(control.ctx, *query.from, resolver, sqlQueryPartitionPredicates(query), visit)
	if err != nil {
		return SQLQueryResult{}, true, err
	}
	if metrics != nil {
		detail := sqlExplainExpression(query.where)
		if query.prewhere.kind != "" {
			detail = sqlExplainExpression(query.prewhere)
			if query.where.kind != "" {
				detail += " THEN " + sqlExplainExpression(query.where)
			}
		}
		metrics.record("PREWHERE SCAN", detail, inputRows, matched, started)
		if !materializeStarted.IsZero() {
			metrics.record("LATE MATERIALIZATION", strings.Join(columns, ","), matched, len(result.Rows), materializeStarted)
		}
		if query.limit >= 0 || query.offset > 0 {
			metrics.record("LIMIT", fmt.Sprintf("limit=%d offset=%d", query.limit, query.offset), matched, len(result.Rows), started)
		}
	}
	return result, true, nil
}
