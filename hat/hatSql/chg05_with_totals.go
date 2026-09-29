package hatSql

import "fmt"

func sqlWithTotalsResult(q *sqlQuery, rows []sqlExecRow, columns []string) ([]SQLRow, error) {
	if q == nil || !q.withTotals {
		return nil, nil
	}
	total := make(SQLRow, len(columns))
	for index, item := range q.selects {
		if item.expr.kind == "star" {
			return nil, fmt.Errorf("SQL WITH TOTALS requires explicit SELECT expressions")
		}
		isGroupExpression := false
		for _, group := range q.groupBy {
			if sqlSameField(item.expr, group) || sqlExpressionsStructurallyEqual(item.expr, group) {
				isGroupExpression = true
				break
			}
		}
		var value interface{}
		if !isGroupExpression {
			value = evalSQLExpr(item.expr, rows, sqlExecRow{})
		}
		if err := sqlExpressionError(value); err != nil {
			return nil, err
		}
		total[columns[index]] = value
	}
	return []SQLRow{total}, nil
}
