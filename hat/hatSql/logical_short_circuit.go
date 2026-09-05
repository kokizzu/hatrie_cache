package hatSql

func sqlExprBatchShortCircuitSafe(expr sqlExpr) bool {
	switch expr.kind {
	case "literal", "field":
		return true
	case "binary":
		if expr.left == nil {
			return false
		}
		if expr.op == "IS NULL" || expr.op == "IS NOT NULL" {
			return sqlExprBatchShortCircuitSafe(*expr.left)
		}
		if expr.right == nil {
			return false
		}
		switch expr.op {
		case "AND", "OR", "LIKE", "=", "!=", "<>", "<", "<=", ">", ">=":
			return sqlExprBatchShortCircuitSafe(*expr.left) && sqlExprBatchShortCircuitSafe(*expr.right)
		default:
			return false
		}
	default:
		return false
	}
}

func evalSQLLogicalExprBatch(expr sqlExpr, rows []sqlExecRow, functions SQLFunctionResolver) ([]interface{}, error) {
	left, err := evalSQLExprBatch(*expr.left, rows, functions)
	if err != nil {
		return nil, err
	}
	out := make([]interface{}, len(rows))
	needed := 0
	for index, value := range left {
		if err := sqlExpressionError(value); err != nil {
			out[index] = sqlEvaluationFailure(err)
			continue
		}
		if expr.op == "AND" {
			if value != nil && !sqlTruthy(value) {
				out[index] = false
				continue
			}
		} else if value != nil && sqlTruthy(value) {
			out[index] = true
			continue
		}
		needed++
	}
	if needed == 0 {
		return out, nil
	}

	if needed == len(rows) {
		right, err := evalSQLExprBatch(*expr.right, rows, functions)
		if err != nil {
			return nil, err
		}
		for index, value := range right {
			if err := sqlExpressionError(value); err != nil {
				out[index] = sqlEvaluationFailure(err)
				continue
			}
			out[index] = sqlBinaryValueWithCollation(expr.op, left[index], value, expr.collation)
		}
		return out, nil
	}

	indexes := make([]int, 0, needed)
	selectedRows := make([]sqlExecRow, 0, needed)
	for index, value := range left {
		if expr.op == "AND" && value != nil && !sqlTruthy(value) || expr.op == "OR" && value != nil && sqlTruthy(value) {
			continue
		}
		indexes = append(indexes, index)
		selectedRows = append(selectedRows, rows[index])
	}
	right, err := evalSQLExprBatch(*expr.right, selectedRows, functions)
	if err != nil {
		return nil, err
	}
	for rightIndex, index := range indexes {
		value := right[rightIndex]
		if err := sqlExpressionError(value); err != nil {
			out[index] = sqlEvaluationFailure(err)
			continue
		}
		out[index] = sqlBinaryValueWithCollation(expr.op, left[index], value, expr.collation)
	}
	return out, nil
}
