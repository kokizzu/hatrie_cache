package hatSql

// sqlBetweenProgram holds literal bounds that are safe to reuse for every
// input row. Dynamic bounds continue through the normal expression evaluator.
type sqlBetweenProgram struct {
	lower interface{}
	upper interface{}
}

func prepareSQLBetweenExpr(expr *sqlExpr) {
	if expr == nil {
		return
	}
	expr.betweenProgram = nil
	if expr.kind != "between" || len(expr.args) != 2 {
		return
	}
	lower := expr.args[0]
	upper := expr.args[1]
	if lower.kind != "literal" || upper.kind != "literal" {
		return
	}
	if sqlExpressionError(lower.value) != nil || sqlExpressionError(upper.value) != nil {
		return
	}
	expr.betweenProgram = &sqlBetweenProgram{lower: lower.value, upper: upper.value}
}

func (program *sqlBetweenProgram) evaluate(op string, left interface{}, collation SQLCollation) interface{} {
	return sqlBetweenValueWithCollation(op, left, program.lower, program.upper, collation)
}
