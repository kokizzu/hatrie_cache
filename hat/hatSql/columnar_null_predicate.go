package hatSql

// sqlColumnarNullPredicate recognizes a direct field NULL check. Compound
// expressions stay on the general evaluator so SQL UNKNOWN semantics remain
// authoritative outside this narrow bitmap-safe shape.
func sqlColumnarNullPredicate(expr sqlExpr, alias string) (field string, notNull bool, ok bool) {
	if expr.kind != "binary" || expr.left == nil || expr.right != nil || (expr.op != "IS NULL" && expr.op != "IS NOT NULL") {
		return "", false, false
	}
	if expr.left.kind != "field" || expr.left.qualifier != "" && expr.left.qualifier != alias {
		return "", false, false
	}
	return expr.left.name, expr.op == "IS NOT NULL", true
}

type sqlColumnarNullPredicateKernel struct {
	validity []byte
	plain    []interface{}
	rows     int
	notNull  bool
}

func (kernel sqlColumnarNullPredicateKernel) matches(row int) bool {
	if row < 0 || row >= kernel.rows {
		return false
	}
	valid := true
	if kernel.plain != nil {
		valid = kernel.plain[row] != nil
	} else if kernel.validity != nil {
		valid = kernel.validity[row>>3]&(byte(1)<<uint(row&7)) != 0
	}
	return valid == kernel.notNull
}

func sqlColumnarNullPredicateKernelForBatch(expr sqlExpr, alias string, batch ColumnarBatch) (sqlColumnarNullPredicateKernel, bool) {
	field, notNull, ok := sqlColumnarNullPredicate(expr, alias)
	if !ok || batch.Rows < 0 {
		return sqlColumnarNullPredicateKernel{}, false
	}
	if column, exists := batch.PackedColumns[field]; exists {
		if column.Rows != batch.Rows || column.RowCount() != batch.Rows {
			return sqlColumnarNullPredicateKernel{}, false
		}
		return sqlColumnarNullPredicateKernel{validity: column.Validity, rows: batch.Rows, notNull: notNull}, true
	}
	if column, exists := batch.BoolColumns[field]; exists {
		if column.Rows != batch.Rows || column.Rows < 0 || len(column.Bits) != columnarPackedBitmapBytes(column.Rows) || (column.Validity != nil && len(column.Validity) != columnarPackedBitmapBytes(column.Rows)) || !columnarBitmapHasNoTrailingBits(column.Bits, column.Rows) || !columnarBitmapHasNoTrailingBits(column.Validity, column.Rows) {
			return sqlColumnarNullPredicateKernel{}, false
		}
		return sqlColumnarNullPredicateKernel{validity: column.Validity, rows: batch.Rows, notNull: notNull}, true
	}
	if column, exists := batch.NumericColumns[field]; exists {
		if column.Rows != batch.Rows || column.Rows < 0 || column.Kind != ColumnarNumericInt64 && column.Kind != ColumnarNumericFloat64 || len(column.Data) != columnarFixedWidthBytes(column.Rows, 8) || (column.Validity != nil && len(column.Validity) != columnarPackedBitmapBytes(column.Rows)) || !columnarBitmapHasNoTrailingBits(column.Validity, column.Rows) {
			return sqlColumnarNullPredicateKernel{}, false
		}
		return sqlColumnarNullPredicateKernel{validity: column.Validity, rows: batch.Rows, notNull: notNull}, true
	}
	if _, exists := batch.Dictionaries[field]; exists {
		return sqlColumnarNullPredicateKernel{}, false
	}
	if _, exists := batch.ListColumns[field]; exists {
		return sqlColumnarNullPredicateKernel{}, false
	}
	if _, exists := batch.NestedColumns[field]; exists {
		return sqlColumnarNullPredicateKernel{}, false
	}
	if _, exists := batch.MapColumns[field]; exists {
		return sqlColumnarNullPredicateKernel{}, false
	}
	if batch.fieldOffsets != nil || batch.decompressedBlockCache != nil {
		return sqlColumnarNullPredicateKernel{}, false
	}
	values, exists := batch.Columns[field]
	if !exists || len(values) != batch.Rows {
		return sqlColumnarNullPredicateKernel{}, false
	}
	return sqlColumnarNullPredicateKernel{plain: values, rows: batch.Rows, notNull: notNull}, true
}
