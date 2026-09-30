package hatSql

// sqlColumnarNullPredicateKernel evaluates IS NULL and IS NOT NULL directly
// from a packed column's validity bitmap without decoding its values.
type sqlColumnarNullPredicateKernel struct {
	validity []byte
	rows     int
	isNull   bool
}

func newSQLColumnarNullPredicateKernel(validity []byte, rows int, isNull bool) (sqlColumnarNullPredicateKernel, bool) {
	bitmapBytes := columnarPackedBitmapBytes(rows)
	if rows < 0 || (validity != nil && len(validity) != bitmapBytes) || !columnarBitmapHasNoTrailingBits(validity, rows) {
		return sqlColumnarNullPredicateKernel{}, false
	}
	return sqlColumnarNullPredicateKernel{validity: validity, rows: rows, isNull: isNull}, true
}

func (kernel sqlColumnarNullPredicateKernel) matches(row int) bool {
	if row < 0 || row >= kernel.rows {
		return false
	}
	valid := true
	if kernel.validity != nil {
		valid = kernel.validity[row>>3]&(1<<uint(row&7)) != 0
	}
	return valid == !kernel.isNull
}

func sqlColumnarNullPredicate(expr sqlExpr, alias string) (field string, isNull bool, ok bool) {
	if expr.kind != "binary" || expr.left == nil || expr.right != nil || expr.left.kind != "field" {
		return "", false, false
	}
	if expr.left.qualifier != "" && expr.left.qualifier != alias {
		return "", false, false
	}
	switch expr.op {
	case "IS NULL":
		return expr.left.name, true, true
	case "IS NOT NULL":
		return expr.left.name, false, true
	default:
		return "", false, false
	}
}

func sqlColumnarNullBitmapForField(batch ColumnarBatch, field string) ([]byte, int, bool) {
	if column, ok := batch.NumericColumns[field]; ok {
		if column.Rows != batch.Rows || column.RowCount() != batch.Rows {
			return nil, 0, false
		}
		return column.Validity, batch.Rows, true
	}
	if column, ok := batch.BoolColumns[field]; ok {
		if column.Rows != batch.Rows || column.RowCount() != batch.Rows {
			return nil, 0, false
		}
		return column.Validity, batch.Rows, true
	}
	if column, ok := batch.PackedColumns[field]; ok {
		if column.Rows != batch.Rows || column.RowCount() != batch.Rows {
			return nil, 0, false
		}
		return column.Validity, batch.Rows, true
	}
	return nil, 0, false
}

func sqlColumnarNullKernelForQuery(expr sqlExpr, alias string, batch ColumnarBatch) (sqlColumnarNullPredicateKernel, bool) {
	field, isNull, ok := sqlColumnarNullPredicate(expr, alias)
	if !ok {
		return sqlColumnarNullPredicateKernel{}, false
	}
	validity, rows, ok := sqlColumnarNullBitmapForField(batch, field)
	if !ok {
		return sqlColumnarNullPredicateKernel{}, false
	}
	return newSQLColumnarNullPredicateKernel(validity, rows, isNull)
}
