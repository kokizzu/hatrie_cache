package hatSql

// sqlColumnarBooleanPredicateKernel evaluates a direct comparison against a
// validated bit-packed boolean column without materializing interface values.
type sqlColumnarBooleanPredicateKernel struct {
	bits     []byte
	validity []byte
	rows     int
	operator string
	value    bool
}

func newSQLColumnarBooleanPredicateKernel(column ColumnarBoolColumn, operator string, value bool) (sqlColumnarBooleanPredicateKernel, bool) {
	if column.Rows < 0 || column.RowCount() != column.Rows || !sqlColumnarBooleanOperator(operator) {
		return sqlColumnarBooleanPredicateKernel{}, false
	}
	return sqlColumnarBooleanPredicateKernel{
		bits:     column.Bits,
		validity: column.Validity,
		rows:     column.Rows,
		operator: operator,
		value:    value,
	}, true
}

func (kernel sqlColumnarBooleanPredicateKernel) matches(row int) bool {
	if row < 0 || row >= kernel.rows {
		return false
	}
	byteIndex := row >> 3
	mask := byte(1 << uint(row&7))
	if kernel.validity != nil && kernel.validity[byteIndex]&mask == 0 {
		return false
	}
	actual := kernel.bits[byteIndex]&mask != 0
	matched := actual == kernel.value
	if kernel.operator == "!=" || kernel.operator == "<>" {
		return !matched
	}
	return matched
}

func sqlColumnarBooleanOperator(operator string) bool {
	return operator == "=" || operator == "!=" || operator == "<>"
}

func sqlColumnarBooleanPredicate(expr sqlExpr, alias string) (field, operator string, value bool, ok bool) {
	if expr.kind != "binary" || expr.left == nil || expr.right == nil {
		return "", "", false, false
	}
	if expr.left.kind == "field" && (expr.left.qualifier == "" || expr.left.qualifier == alias) && expr.right.kind == "literal" {
		value, literalOK := expr.right.value.(bool)
		return expr.left.name, expr.op, value, literalOK && sqlColumnarBooleanOperator(expr.op)
	}
	if expr.right.kind == "field" && (expr.right.qualifier == "" || expr.right.qualifier == alias) && expr.left.kind == "literal" {
		value, literalOK := expr.left.value.(bool)
		return expr.right.name, expr.op, value, literalOK && sqlColumnarBooleanOperator(expr.op)
	}
	return "", "", false, false
}

func sqlColumnarBooleanKernelForQuery(expr sqlExpr, alias string, batch ColumnarBatch) (sqlColumnarBooleanPredicateKernel, bool) {
	field, operator, value, ok := sqlColumnarBooleanPredicate(expr, alias)
	if !ok {
		return sqlColumnarBooleanPredicateKernel{}, false
	}
	column, ok := batch.BoolColumns[field]
	if !ok {
		return sqlColumnarBooleanPredicateKernel{}, false
	}
	return newSQLColumnarBooleanPredicateKernel(column, operator, value)
}
