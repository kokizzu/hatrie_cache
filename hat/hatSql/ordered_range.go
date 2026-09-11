package hatSql

// sqlOrderedRangePredicate returns one safe literal range conjunct for the
// field that defines a single-field ordered scan. A conjunct from AND is safe
// to use as a candidate bound because the complete predicate is still checked
// after the source returns rows; OR and negated expressions are excluded.
func sqlOrderedRangePredicate(query *sqlQuery) (operator string, value interface{}, ok bool) {
	if query == nil || query.from == nil || len(query.orderBy) != 1 || query.orderBy[0].expr.kind != "field" || query.orderBy[0].expr.qualifier != query.from.alias || query.orderBy[0].expr.name == "" {
		return "", nil, false
	}
	field := query.orderBy[0].expr.name
	var find func(sqlExpr) (string, interface{}, bool)
	find = func(expression sqlExpr) (string, interface{}, bool) {
		if expression.kind == "binary" && expression.op == "AND" && expression.left != nil && expression.right != nil {
			if operator, value, ok := find(*expression.left); ok {
				return operator, value, true
			}
			return find(*expression.right)
		}
		if expression.kind != "binary" || expression.left == nil || expression.right == nil || !sqlOrderedRangeOperator(expression.op) {
			return "", nil, false
		}
		left, right := *expression.left, *expression.right
		if left.kind == "field" && left.qualifier == query.from.alias && left.name == field && right.kind == "literal" && right.value != nil {
			return expression.op, right.value, true
		}
		if right.kind == "field" && right.qualifier == query.from.alias && right.name == field && left.kind == "literal" && left.value != nil {
			return sqlReverseComparison(expression.op), left.value, true
		}
		return "", nil, false
	}
	return find(query.where)
}

func sqlOrderedRangeOperator(operator string) bool {
	switch operator {
	case "=", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}
