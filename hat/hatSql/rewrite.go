package hatSql

// rewriteSQLQuery simplifies execution-local query trees after parameter
// binding. Parsed templates stay immutable in the prepared-query cache.
func rewriteSQLQuery(query *sqlQuery) {
	if query == nil {
		return
	}
	for index := range query.ctes {
		rewriteSQLQuery(query.ctes[index].query)
	}
	if query.from != nil {
		rewriteSQLSource(query.from)
	}
	for index := range query.joins {
		rewriteSQLSource(&query.joins[index].source)
		query.joins[index].on = rewriteSQLExpr(query.joins[index].on)
	}
	for index := range query.unions {
		rewriteSQLQuery(query.unions[index].query)
	}
	for index := range query.selects {
		query.selects[index].expr = rewriteSQLExpr(query.selects[index].expr)
	}
	query.where = rewriteSQLExpr(query.where)
	query.groupBy = rewriteSQLExprs(query.groupBy)
	query.having = rewriteSQLExpr(query.having)
	for index := range query.orderBy {
		query.orderBy[index].expr = rewriteSQLExpr(query.orderBy[index].expr)
	}
	sqlPruneDeadDerivedProjections(query)
}

func rewriteSQLSource(source *sqlSource) {
	if source != nil && source.kind == "SUBQUERY" {
		rewriteSQLQuery(source.query)
	}
}

func rewriteSQLExprs(expressions []sqlExpr) []sqlExpr {
	for index := range expressions {
		expressions[index] = rewriteSQLExpr(expressions[index])
	}
	return expressions
}

func rewriteSQLExpr(expr sqlExpr) sqlExpr {
	if expr.query != nil {
		rewriteSQLQuery(expr.query)
	}
	if expr.left != nil {
		value := rewriteSQLExpr(*expr.left)
		expr.left = &value
	}
	if expr.right != nil {
		value := rewriteSQLExpr(*expr.right)
		expr.right = &value
	}
	if expr.filter != nil {
		filter := rewriteSQLExpr(*expr.filter)
		expr.filter = &filter
	}
	expr.args = rewriteSQLExprs(expr.args)
	for index := range expr.cases {
		expr.cases[index].when = rewriteSQLExpr(expr.cases[index].when)
		expr.cases[index].then = rewriteSQLExpr(expr.cases[index].then)
	}
	if expr.window != nil {
		expr.window.partition = rewriteSQLExprs(expr.window.partition)
		for index := range expr.window.order {
			expr.window.order[index].expr = rewriteSQLExpr(expr.window.order[index].expr)
		}
	}
	if expr.kind == "binary" && expr.left != nil && expr.right != nil {
		if left, ok := sqlBooleanLiteral(*expr.left); ok {
			switch expr.op {
			case "AND":
				if !left {
					return sqlLiteralExpr(false, expr.token)
				}
				return *expr.right
			case "OR":
				if left {
					return sqlLiteralExpr(true, expr.token)
				}
				return *expr.right
			}
		}
		if right, ok := sqlBooleanLiteral(*expr.right); ok {
			switch expr.op {
			case "AND":
				if !right {
					return sqlLiteralExpr(false, expr.token)
				}
				return *expr.left
			case "OR":
				if right {
					return sqlLiteralExpr(true, expr.token)
				}
				return *expr.left
			}
		}
	}
	if sqlRewriteFoldable(expr) {
		value := evalSQLExpr(expr, nil, sqlExecRow{})
		if sqlExpressionError(value) == nil {
			return sqlLiteralExpr(value, expr.token)
		}
	}
	return expr
}

func sqlLiteralExpr(value interface{}, token sqlToken) sqlExpr {
	return sqlExpr{kind: "literal", value: value, token: token}
}

func sqlBooleanLiteral(expr sqlExpr) (bool, bool) {
	value, ok := expr.value.(bool)
	return value, ok && expr.kind == "literal"
}

func sqlRewriteFoldable(expr sqlExpr) bool {
	switch expr.kind {
	case "literal":
		return true
	case "unary":
		return expr.left != nil && sqlRewriteFoldable(*expr.left)
	case "binary":
		if expr.left == nil || expr.right == nil {
			return false
		}
		switch expr.op {
		case "AND", "OR", "=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "LIKE":
			return sqlRewriteFoldable(*expr.left) && sqlRewriteFoldable(*expr.right)
		}
	}
	return false
}

func sqlPruneDeadDerivedProjections(query *sqlQuery) {
	if query == nil {
		return
	}
	if query.from != nil {
		sqlPruneDerivedProjection(query, query.from)
	}
	for index := range query.joins {
		sqlPruneDerivedProjection(query, &query.joins[index].source)
	}
}

func sqlPruneDerivedProjection(query *sqlQuery, source *sqlSource) {
	if source == nil || source.kind != "SUBQUERY" || source.query == nil {
		return
	}
	references, wildcard := sqlAliasReferences(query, source.alias)
	if wildcard || len(references) == 0 || len(source.query.selects) < 2 {
		return
	}
	columns := sqlColumns(source.query.selects)
	kept := make([]sqlSelectItem, 0, len(source.query.selects))
	for index, item := range source.query.selects {
		if references[columns[index]] {
			kept = append(kept, item)
		}
	}
	if len(kept) > 0 {
		source.query.selects = kept
	}
}

func sqlAliasReferences(query *sqlQuery, alias string) (map[string]bool, bool) {
	references := make(map[string]bool)
	wildcard := false
	visit := func(expr sqlExpr) {}
	visit = func(expr sqlExpr) {
		if expr.kind == "star" && (expr.qualifier == "" || expr.qualifier == alias) {
			wildcard = true
		}
		if expr.kind == "field" && expr.qualifier == alias {
			references[expr.name] = true
		}
		if expr.left != nil {
			visit(*expr.left)
		}
		if expr.right != nil {
			visit(*expr.right)
		}
		for _, argument := range expr.args {
			visit(argument)
		}
		for _, branch := range expr.cases {
			visit(branch.when)
			visit(branch.then)
		}
	}
	for _, item := range query.selects {
		visit(item.expr)
	}
	visit(query.where)
	visit(query.having)
	for _, expression := range query.groupBy {
		visit(expression)
	}
	for _, order := range query.orderBy {
		visit(order.expr)
	}
	for _, join := range query.joins {
		visit(join.on)
	}
	return references, wildcard
}
