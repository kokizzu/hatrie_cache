package hatSql

import "math/bits"

func sqlColumnarBitmapNonNullCount(validity []byte, rows int) int64 {
	if rows <= 0 {
		return 0
	}
	if validity == nil {
		return int64(rows)
	}
	var count int64
	for _, value := range validity {
		count += int64(bits.OnesCount8(value))
	}
	return count
}

// sqlColumnarNonNullCount returns a NULL-aware count without decoding values
// when the column's physical layout already carries validated validity data.
// Unknown layouts deliberately fall back to the general aggregate path.
func sqlColumnarNonNullCount(batch ColumnarBatch, field string) (int64, bool) {
	if batch.Rows < 0 {
		return 0, false
	}
	if column, ok := batch.NumericColumns[field]; ok {
		if column.Rows != batch.Rows || column.RowCount() != batch.Rows {
			return 0, false
		}
		return sqlColumnarBitmapNonNullCount(column.Validity, batch.Rows), true
	}
	if column, ok := batch.BoolColumns[field]; ok {
		if column.Rows != batch.Rows || column.RowCount() != batch.Rows {
			return 0, false
		}
		return sqlColumnarBitmapNonNullCount(column.Validity, batch.Rows), true
	}
	if column, ok := batch.PackedColumns[field]; ok {
		if column.Rows != batch.Rows || column.RowCount() != batch.Rows {
			return 0, false
		}
		return int64(len(column.Values)), true
	}
	return 0, false
}

func sqlColumnarPrepareCountMetadata(aggregates []sqlColumnarNumericAggregate, batch ColumnarBatch, where sqlExpr) bool {
	if where.kind != "" || len(aggregates) == 0 {
		return false
	}
	allCounts := true
	for index := range aggregates {
		aggregate := &aggregates[index]
		if aggregate.name != "COUNT" {
			allCounts = false
			continue
		}
		count := int64(batch.Rows)
		if aggregate.field != "" {
			var ok bool
			count, ok = sqlColumnarNonNullCount(batch, aggregate.field)
			if !ok {
				allCounts = false
				continue
			}
		}
		aggregate.count = count
		aggregate.countMetadata = true
	}
	return allCounts
}
