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
	number, ok := sqlColumnarNumericValueAt(kernel.kind, kernel.data, kernel.validity, kernel.rows, row)
	if !ok {
		return false
	}
	return sqlColumnarNumericMatches(number, kernel.operator, kernel.value)
}

type sqlColumnarNumericNotBetweenKernel struct {
	kind     ColumnarNumericKind
	data     []byte
	validity []byte
	rows     int
	lower    float64
	upper    float64
}

func newSQLColumnarNumericNotBetweenKernel(column ColumnarNumericColumn, lower, upper float64) (sqlColumnarNumericNotBetweenKernel, bool) {
	if column.Rows < 0 || column.Kind != ColumnarNumericInt64 && column.Kind != ColumnarNumericFloat64 || column.RowCount() != column.Rows {
		return sqlColumnarNumericNotBetweenKernel{}, false
	}
	return sqlColumnarNumericNotBetweenKernel{
		kind:     column.Kind,
		data:     column.Data,
		validity: column.Validity,
		rows:     column.Rows,
		lower:    lower,
		upper:    upper,
	}, true
}

func (kernel sqlColumnarNumericNotBetweenKernel) matches(row int) bool {
	number, ok := sqlColumnarNumericValueAt(kernel.kind, kernel.data, kernel.validity, kernel.rows, row)
	return ok && sqlColumnarNumericNotBetweenMatches(number, kernel.lower, kernel.upper)
}

func sqlColumnarNumericValueAt(kind ColumnarNumericKind, data, validity []byte, rows, row int) (float64, bool) {
	if row < 0 || row >= rows || len(data) < 8 || row > (len(data)-8)/8 {
		return 0, false
	}
	if validity != nil {
		validityByte := row >> 3
		if validityByte >= len(validity) || validity[validityByte]&(1<<uint(row&7)) == 0 {
			return 0, false
		}
	}
	bitsValue := binary.LittleEndian.Uint64(data[row<<3:])
	if kind == ColumnarNumericInt64 {
		return float64(int64(bitsValue)), true
	}
	return math.Float64frombits(bitsValue), true
}

func sqlColumnarNumericNotBetweenMatches(number, lower, upper float64) bool {
	return number < lower || number > upper || math.IsNaN(number) || math.IsNaN(lower) || math.IsNaN(upper)
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
