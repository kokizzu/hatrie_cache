package hatSql

import (
	"fmt"
	"strings"
)

// normalizeSQLAggregateStateIf composes the existing State/Merge aggregate
// contracts with a trailing If condition. It deliberately normalizes only
// names whose base already has strict state arity validation.
func normalizeSQLAggregateStateIf(expr sqlExpr) (sqlExpr, bool, error) {
	name := strings.ToUpper(expr.name)
	if !strings.HasSuffix(name, "_IF") {
		return expr, false, nil
	}
	baseName := strings.TrimSuffix(name, "_IF")
	normalized := expr
	normalized.name = baseName

	if kind, merge, ok := sqlAggregateStateSpec(baseName); ok {
		if len(expr.args) == 0 {
			return sqlExpr{}, true, sqlAggregateStateArityError(normalized, merge, kind)
		}
		base := normalized
		base.args = append([]sqlExpr(nil), expr.args[:len(expr.args)-1]...)
		if _, err := sqlAggregateStateArity(base, merge, kind); err != nil {
			return sqlExpr{}, true, err
		}
		withCondition, err := sqlAggregateStateIfWithCondition(normalized, base.args, expr.args[len(expr.args)-1])
		if err != nil {
			return sqlExpr{}, true, err
		}
		return withCondition, true, nil
	}
	if _, merge, ok := sqlArgExtremeStateSpec(baseName); ok {
		if len(expr.args) == 0 {
			return sqlExpr{}, true, sqlArgExtremeStateArityError(normalized, merge)
		}
		base := normalized
		base.args = append([]sqlExpr(nil), expr.args[:len(expr.args)-1]...)
		if _, _, err := sqlArgExtremeStateArity(base, merge); err != nil {
			return sqlExpr{}, true, err
		}
		withCondition, err := sqlAggregateStateIfWithCondition(normalized, base.args, expr.args[len(expr.args)-1])
		if err != nil {
			return sqlExpr{}, true, err
		}
		return withCondition, true, nil
	}
	return expr, false, nil
}

func sqlAggregateStateIfWithCondition(expr sqlExpr, args []sqlExpr, condition sqlExpr) (sqlExpr, error) {
	expr.args = args
	if condition.kind == "star" {
		return sqlExpr{}, fmt.Errorf("%s condition cannot be *", expr.name)
	}
	if expr.filter == nil {
		expr.filter = &condition
		return expr, nil
	}
	existing := *expr.filter
	expr.filter = &sqlExpr{kind: "binary", op: "AND", left: &existing, right: &condition}
	return expr, nil
}

func sqlAggregateStateArityError(expr sqlExpr, merge bool, kind sqlAggregateStateKind) error {
	_, err := sqlAggregateStateArity(expr, merge, kind)
	return err
}

func sqlArgExtremeStateArityError(expr sqlExpr, merge bool) error {
	_, _, err := sqlArgExtremeStateArity(expr, merge)
	return err
}
