package hatSql

import "reflect"

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
		if (expr.op == "AND" || expr.op == "OR") && sqlRewriteCommonSubexpressionSafe(*expr.left) && sqlRewriteCommonSubexpressionSafe(*expr.right) && sqlExpressionsStructurallyEqual(*expr.left, *expr.right) {
			return *expr.left
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

// sqlRewriteCommonSubexpressionSafe limits idempotent predicate elimination to
// expressions whose repeated evaluation cannot invoke user code or depend on
// query state. Keeping this at rewrite time avoids a cache lookup on ordinary
// expressions that do not contain a duplicate subtree.
func sqlRewriteCommonSubexpressionSafe(expr sqlExpr) bool {
	if expr.query != nil || expr.filter != nil || expr.window != nil {
		return false
	}
	switch expr.kind {
	case "literal", "field":
		return true
	case "cast":
		return len(expr.args) == 1 && sqlRewriteCommonSubexpressionSafe(expr.args[0])
	case "unary":
		return expr.left != nil && sqlRewriteCommonSubexpressionSafe(*expr.left)
	case "func":
		if !sqlRewriteFoldableFunction(expr.name) {
			return false
		}
		for _, argument := range expr.args {
			if !sqlRewriteCommonSubexpressionSafe(argument) {
				return false
			}
		}
		return true
	case "case":
		if expr.left != nil && !sqlRewriteCommonSubexpressionSafe(*expr.left) {
			return false
		}
		for _, branch := range expr.cases {
			if !sqlRewriteCommonSubexpressionSafe(branch.when) || !sqlRewriteCommonSubexpressionSafe(branch.then) {
				return false
			}
		}
		return expr.right == nil || sqlRewriteCommonSubexpressionSafe(*expr.right)
	case "in":
		if expr.left == nil || !sqlRewriteCommonSubexpressionSafe(*expr.left) {
			return false
		}
		for _, argument := range expr.args {
			if !sqlRewriteCommonSubexpressionSafe(argument) {
				return false
			}
		}
		return true
	case "between":
		return expr.left != nil && len(expr.args) == 2 && sqlRewriteCommonSubexpressionSafe(*expr.left) && sqlRewriteCommonSubexpressionSafe(expr.args[0]) && sqlRewriteCommonSubexpressionSafe(expr.args[1])
	case "binary":
		if expr.left == nil {
			return false
		}
		if expr.op == "IS NULL" || expr.op == "IS NOT NULL" {
			return sqlRewriteCommonSubexpressionSafe(*expr.left)
		}
		if expr.right == nil {
			return false
		}
		switch expr.op {
		case "AND", "OR", "=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "LIKE", "REGEXP", "NOT REGEXP":
			return sqlRewriteCommonSubexpressionSafe(*expr.left) && sqlRewriteCommonSubexpressionSafe(*expr.right)
		}
	}
	return false
}

func sqlExpressionsStructurallyEqual(left, right sqlExpr) bool {
	if left.kind != right.kind || left.name != right.name || left.qualifier != right.qualifier || left.op != right.op || left.windowName != right.windowName || left.collation != right.collation || !sqlExpressionValuesStructurallyEqual(left.value, right.value) {
		return false
	}
	if !sqlExpressionPointersStructurallyEqual(left.left, right.left) || !sqlExpressionPointersStructurallyEqual(left.right, right.right) || !sqlExpressionPointersStructurallyEqual(left.filter, right.filter) {
		return false
	}
	if len(left.args) != len(right.args) || len(left.cases) != len(right.cases) {
		return false
	}
	for index := range left.args {
		if !sqlExpressionsStructurallyEqual(left.args[index], right.args[index]) {
			return false
		}
	}
	for index := range left.cases {
		if !sqlExpressionsStructurallyEqual(left.cases[index].when, right.cases[index].when) || !sqlExpressionsStructurallyEqual(left.cases[index].then, right.cases[index].then) {
			return false
		}
	}
	return left.query == nil && right.query == nil && left.window == nil && right.window == nil
}

func sqlExpressionValuesStructurallyEqual(left, right interface{}) bool {
	leftToken, leftIsToken := left.(sqlToken)
	rightToken, rightIsToken := right.(sqlToken)
	if leftIsToken || rightIsToken {
		return leftIsToken && rightIsToken && leftToken.kind == rightToken.kind && leftToken.text == rightToken.text
	}
	return reflect.DeepEqual(left, right)
}

func sqlExpressionPointersStructurallyEqual(left, right *sqlExpr) bool {
	if left == nil || right == nil {
		return left == right
	}
	return sqlExpressionsStructurallyEqual(*left, *right)
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
	case "cast":
		return len(expr.args) == 1 && sqlRewriteFoldable(expr.args[0])
	case "func":
		if !sqlRewriteFoldableFunction(expr.name) {
			return false
		}
		for _, argument := range expr.args {
			if !sqlRewriteFoldable(argument) {
				return false
			}
		}
		return true
	case "case":
		if expr.left != nil && !sqlRewriteFoldable(*expr.left) {
			return false
		}
		for _, branch := range expr.cases {
			if !sqlRewriteFoldable(branch.when) || !sqlRewriteFoldable(branch.then) {
				return false
			}
		}
		return expr.right == nil || sqlRewriteFoldable(*expr.right)
	case "in":
		if expr.left == nil || !sqlRewriteFoldable(*expr.left) {
			return false
		}
		for _, argument := range expr.args {
			if !sqlRewriteFoldable(argument) {
				return false
			}
		}
		return true
	case "between":
		if expr.left == nil || len(expr.args) != 2 || !sqlRewriteFoldable(*expr.left) {
			return false
		}
		return sqlRewriteFoldable(expr.args[0]) && sqlRewriteFoldable(expr.args[1])
	case "unary":
		return expr.left != nil && sqlRewriteFoldable(*expr.left)
	case "binary":
		if expr.left == nil || expr.right == nil {
			return (expr.op == "IS NULL" || expr.op == "IS NOT NULL") && expr.left != nil && sqlRewriteFoldable(*expr.left)
		}
		switch expr.op {
		case "AND", "OR", "=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "LIKE", "REGEXP", "NOT REGEXP", "IS NULL", "IS NOT NULL":
			return sqlRewriteFoldable(*expr.left) && sqlRewriteFoldable(*expr.right)
		}
	}
	return false
}

func sqlRewriteFoldableFunction(name string) bool {
	switch name {
	case "LOWER", "COALESCE", "NULLIF", "PARSE_TIMESTAMP", "TIMESTAMP_ADD", "TIMESTAMP_DIFF", "REGEXP_LIKE", "REGEXP_EXTRACT", "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS", "CONTAINS":
		return true
	default:
		return false
	}
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
