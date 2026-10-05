package hatSql

import "fmt"

func sqlExpandOrderByAll(query *sqlQuery) error {
	if query == nil || !query.orderByAll {
		return nil
	}
	query.orderByAll = false
	if len(query.selects) == 0 {
		return fmt.Errorf("ORDER BY ALL requires a SELECT list")
	}
	orders := make([]sqlOrder, 0, len(query.selects))
	for _, item := range query.selects {
		if item.expr.kind == "star" {
			return fmt.Errorf("ORDER BY ALL does not support SELECT *")
		}
		orders = append(orders, sqlOrder{
			expr:       cloneSQLExpr(item.expr),
			desc:       query.orderByAllDesc,
			nullsFirst: query.orderByAllNullsFirst,
			nullsLast:  query.orderByAllNullsLast,
		})
	}
	query.orderByAllDesc = false
	query.orderByAllNullsFirst = false
	query.orderByAllNullsLast = false
	query.orderBy = orders
	return nil
}
