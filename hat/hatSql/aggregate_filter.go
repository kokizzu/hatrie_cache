package hatSql

func sqlAggregateFilterRows(expr sqlExpr, group []sqlExecRow) ([]sqlExecRow, error) {
	if expr.filter == nil {
		return group, nil
	}
	filtered := make([]sqlExecRow, 0, len(group))
	for _, row := range group {
		value := evalSQLExpr(*expr.filter, []sqlExecRow{row}, row)
		if err := sqlExpressionError(value); err != nil {
			return nil, err
		}
		if sqlTruthy(value) {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}
