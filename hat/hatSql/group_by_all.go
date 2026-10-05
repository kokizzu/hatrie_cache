package hatSql

import "fmt"

func sqlExpandGroupByAll(query *sqlQuery) error {
	if query == nil || !query.groupByAll {
		return nil
	}
	query.groupByAll = false
	var expressions []sqlExpr
	for _, item := range query.selects {
		for _, expression := range sqlGroupByAllExpressions(item.expr) {
			if expression.kind == "star" {
				return fmt.Errorf("GROUP BY ALL does not support SELECT *")
			}
			if !sqlGroupingSetContains(expressions, expression) {
				expressions = append(expressions, expression)
			}
		}
	}
	query.groupBy = expressions
	return nil
}

func sqlGroupByAllExpressions(expression sqlExpr) []sqlExpr {
	if expression.kind == "" || expression.window != nil {
		return nil
	}
	if !sqlExprHasAggregate(expression) && !sqlExprHasWindow(expression) {
		return []sqlExpr{cloneSQLExpr(expression)}
	}
	if sqlExprIsDirectAggregate(expression) {
		if expression.filter == nil {
			return nil
		}
		return sqlGroupByAllExpressions(*expression.filter)
	}
	var out []sqlExpr
	appendExpressions := func(values []sqlExpr) {
		out = append(out, values...)
	}
	for _, argument := range expression.args {
		appendExpressions(sqlGroupByAllExpressions(argument))
	}
	for _, branch := range expression.cases {
		appendExpressions(sqlGroupByAllExpressions(branch.when))
		appendExpressions(sqlGroupByAllExpressions(branch.then))
	}
	if expression.left != nil {
		appendExpressions(sqlGroupByAllExpressions(*expression.left))
	}
	if expression.right != nil {
		appendExpressions(sqlGroupByAllExpressions(*expression.right))
	}
	if expression.filter != nil {
		appendExpressions(sqlGroupByAllExpressions(*expression.filter))
	}
	return out
}

func sqlExprIsDirectAggregate(expression sqlExpr) bool {
	return expression.kind == "func" && sqlExprHasAggregate(sqlExpr{kind: "func", name: expression.name})
}
