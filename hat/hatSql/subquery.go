package hatSql

import "fmt"

type sqlEvalEnvironment struct {
	resolver SQLSourceResolver
	ctes     map[string][]SQLRow
	metrics  *sqlExecutionMetrics
	control  *sqlExecutionControl
}

func evalSQLExists(expr sqlExpr, row sqlExecRow) interface{} {
	result, err := executeSQLExpressionSubquery(expr, row)
	if err != nil {
		return sqlEvalError{err: err, token: expr.token}
	}
	return len(result.Rows) != 0
}

func evalSQLScalarSubquery(expr sqlExpr, row sqlExecRow) interface{} {
	result, err := executeSQLExpressionSubquery(expr, row)
	if err != nil {
		return sqlEvalError{err: err, token: expr.token}
	}
	if len(result.Rows) == 0 {
		return nil
	}
	if len(result.Rows) != 1 || len(result.Columns) != 1 {
		return sqlEvalError{err: fmt.Errorf("scalar subquery must return at most one row and exactly one column"), token: expr.token}
	}
	return result.Rows[0][result.Columns[0]]
}

func executeSQLExpressionSubquery(expr sqlExpr, row sqlExecRow) (SQLQueryResult, error) {
	if expr.query == nil {
		return SQLQueryResult{}, fmt.Errorf("SQL subquery is missing a query")
	}
	if row.environment == nil {
		return SQLQueryResult{}, fmt.Errorf("SQL subquery requires an execution environment")
	}
	outer := row
	ctes := make(map[string][]SQLRow, len(row.environment.ctes))
	for name, rows := range row.environment.ctes {
		ctes[name] = rows
	}
	return executeSQLQueryWithMetricsOuter(expr.query, row.environment.resolver, ctes, row.environment.metrics, row.environment.control, &outer)
}

func sqlAttachSQLExecutionEnvironment(rows []sqlExecRow, outer *sqlExecRow, environment *sqlEvalEnvironment) {
	for index := range rows {
		rows[index].outer = outer
		rows[index].environment = environment
	}
}

func sqlQueryHasSubqueryExpression(query *sqlQuery) bool {
	if query == nil {
		return false
	}
	for _, item := range query.selects {
		if sqlExprHasSubqueryExpression(item.expr) {
			return true
		}
	}
	if sqlExprHasSubqueryExpression(query.where) || sqlExprHasSubqueryExpression(query.having) {
		return true
	}
	for _, expression := range query.groupBy {
		if sqlExprHasSubqueryExpression(expression) {
			return true
		}
	}
	for _, order := range query.orderBy {
		if sqlExprHasSubqueryExpression(order.expr) {
			return true
		}
	}
	for _, join := range query.joins {
		if sqlExprHasSubqueryExpression(join.on) {
			return true
		}
	}
	return false
}

func sqlExprHasSubqueryExpression(expr sqlExpr) bool {
	if expr.kind == "exists" || expr.kind == "subquery" {
		return true
	}
	if expr.left != nil && sqlExprHasSubqueryExpression(*expr.left) {
		return true
	}
	if expr.right != nil && sqlExprHasSubqueryExpression(*expr.right) {
		return true
	}
	for _, argument := range expr.args {
		if sqlExprHasSubqueryExpression(argument) {
			return true
		}
	}
	for _, branch := range expr.cases {
		if sqlExprHasSubqueryExpression(branch.when) || sqlExprHasSubqueryExpression(branch.then) {
			return true
		}
	}
	return false
}
