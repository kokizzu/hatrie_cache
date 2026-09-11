package hatSql

import (
	"encoding/binary"
	"math"
)

// sqlColumnarNumericPredicateKernel evaluates one validated packed numeric
// column without boxing each value through ColumnarBatch.Value.
type sqlColumnarNumericPredicateKernel struct {
	kind     ColumnarNumericKind
	data     []byte
	validity []byte
	rows     int
	operator string
	value    float64
}

func newSQLColumnarNumericPredicateKernel(column ColumnarNumericColumn, operator string, value float64) (sqlColumnarNumericPredicateKernel, bool) {
	if column.Rows < 0 || column.Kind != ColumnarNumericInt64 && column.Kind != ColumnarNumericFloat64 || column.RowCount() != column.Rows || !sqlColumnarNumericOperator(operator) {
		return sqlColumnarNumericPredicateKernel{}, false
	}
	return sqlColumnarNumericPredicateKernel{
		kind:     column.Kind,
		data:     column.Data,
		validity: column.Validity,
		rows:     column.Rows,
		operator: operator,
		value:    value,
	}, true
}

func (kernel sqlColumnarNumericPredicateKernel) matches(row int) bool {
	if row < 0 || row >= kernel.rows {
		return false
	}
	if kernel.validity != nil && kernel.validity[row>>3]&(1<<uint(row&7)) == 0 {
		return false
	}
	bitsValue := binary.LittleEndian.Uint64(kernel.data[row<<3:])
	number := math.Float64frombits(bitsValue)
	if kernel.kind == ColumnarNumericInt64 {
		number = float64(int64(bitsValue))
	}
	return sqlColumnarNumericMatches(number, kernel.operator, kernel.value)
}

func sqlColumnarNumericPredicateKernels(batch ColumnarBatch, predicates []sqlColumnarNumericFilter) ([]sqlColumnarNumericPredicateKernel, bool) {
	if len(predicates) == 0 || len(batch.NumericColumns) == 0 {
		return nil, false
	}
	kernels := make([]sqlColumnarNumericPredicateKernel, len(predicates))
	for index, predicate := range predicates {
		column, ok := batch.NumericColumns[predicate.field]
		if !ok || column.Rows != batch.Rows {
			return nil, false
		}
		kernel, ok := newSQLColumnarNumericPredicateKernel(column, predicate.operator, predicate.value)
		if !ok {
			return nil, false
		}
		kernels[index] = kernel
	}
	return kernels, true
}
