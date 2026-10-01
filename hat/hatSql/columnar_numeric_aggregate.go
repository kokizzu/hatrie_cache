package hatSql

import (
	"encoding/binary"
	"math"
)

func sqlColumnarPackedNumericValue(column ColumnarNumericColumn, row int) (float64, bool) {
	if row < 0 || row >= column.Rows || column.Validity != nil && column.Validity[row>>3]&(1<<uint(row&7)) == 0 {
		return 0, false
	}
	bitsValue := binary.LittleEndian.Uint64(column.Data[row<<3:])
	if column.Kind == ColumnarNumericInt64 {
		return float64(int64(bitsValue)), true
	}
	if column.Kind == ColumnarNumericFloat64 {
		return math.Float64frombits(bitsValue), true
	}
	return 0, false
}

func (aggregate *sqlColumnarNumericAggregate) addPackedNumeric(column ColumnarNumericColumn, row int) {
	if aggregate.countMetadata {
		return
	}
	number, ok := sqlColumnarPackedNumericValue(column, row)
	if aggregate.name == "COUNT" {
		if ok {
			aggregate.count++
		}
		return
	}
	if ok {
		aggregate.addNumber(number)
	}
}

func sqlColumnarPrepareNumericAggregateColumn(aggregate *sqlColumnarNumericAggregate, batch ColumnarBatch) {
	if aggregate == nil || aggregate.field == "" {
		return
	}
	column, ok := batch.NumericColumns[aggregate.field]
	if !ok || column.Rows != batch.Rows || column.RowCount() != batch.Rows {
		return
	}
	aggregate.numericColumn = column
	aggregate.numericPacked = true
}

func sqlColumnarPrepareNumericAggregateColumns(aggregates []sqlColumnarNumericAggregate, batch ColumnarBatch) {
	for index := range aggregates {
		sqlColumnarPrepareNumericAggregateColumn(&aggregates[index], batch)
	}
}
