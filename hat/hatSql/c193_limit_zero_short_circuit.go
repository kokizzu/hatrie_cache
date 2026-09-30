package hatSql

// sqlC193LimitZeroResult avoids reading a direct cache source when the query
// cannot produce a row. The scope is intentionally narrow: dynamic star
// expansion and multi-source plans still use the established executor because
// they may need source metadata or branch-specific validation.
func sqlC193LimitZeroResult(query *sqlQuery) (SQLQueryResult, bool) {
	if query == nil || query.limit != 0 || query.limitWithTies || query.withTotals || query.explain || query.sample != nil || query.from == nil {
		return SQLQueryResult{}, false
	}
	if query.from.kind != "CACHE" && query.from.kind != "KEYS" {
		return SQLQueryResult{}, false
	}
	if query.from.final || query.from.lateral || query.from.query != nil || len(query.ctes) != 0 || len(query.joins) != 0 || len(query.unions) != 0 {
		return SQLQueryResult{}, false
	}
	for _, item := range query.selects {
		if item.expr.kind == "star" {
			return SQLQueryResult{}, false
		}
	}
	return SQLQueryResult{Columns: sqlColumns(query.selects), Rows: []SQLRow{}}, true
}
