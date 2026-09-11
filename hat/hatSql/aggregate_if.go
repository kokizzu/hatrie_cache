package hatSql

import "fmt"

func sqlAggregateIfBase(name string) (string, bool) {
	switch name {
	case "COUNTIF", "COUNT_IF":
		return "COUNT", true
	case "SUMIF", "SUM_IF":
		return "SUM", true
	case "AVGIF", "AVG_IF":
		return "AVG", true
	case "MINIF", "MIN_IF":
		return "MIN", true
	case "MAXIF", "MAX_IF":
		return "MAX", true
	case "ARGMAXIF", "ARGMAX_IF":
		return "ARGMAX", true
	case "ARGMINIF", "ARGMIN_IF":
		return "ARGMIN", true
	default:
		return "", false
	}
}

func normalizeSQLAggregateIf(expr sqlExpr) (sqlExpr, bool, error) {
	base, combinator := sqlAggregateIfBase(expr.name)
	if !combinator {
		return expr, false, nil
	}
	expected := 2
	if base == "COUNT" {
		expected = 1
	} else if base == "ARGMAX" || base == "ARGMIN" {
		expected = 3
	}
	if len(expr.args) != expected {
		return sqlExpr{}, true, fmt.Errorf("%s expects %d arguments", expr.name, expected)
	}
	condition := expr.args[len(expr.args)-1]
	if condition.kind == "star" {
		return sqlExpr{}, true, fmt.Errorf("%s condition cannot be *", expr.name)
	}
	normalized := expr
	normalized.name = base
	normalized.args = append([]sqlExpr(nil), expr.args[:len(expr.args)-1]...)
	normalized.filter = &condition
	if expr.filter != nil {
		existing := *expr.filter
		normalized.filter = &sqlExpr{
			kind:  "binary",
			op:    "AND",
			left:  &existing,
			right: normalized.filter,
		}
	}
	return normalized, true, nil
}
