package hatSql

import (
	"encoding/binary"
	"math"
	"sort"
)

// sqlColumnarNumericINPredicate recognizes a direct field IN list containing
// only non-NULL numeric literals. Lists with NULL, dynamic expressions, or
// non-numeric values retain the general evaluator's SQL three-valued logic.
func sqlColumnarNumericINPredicate(expr sqlExpr, alias string) (field string, values []interface{}, ok bool) {
	if expr.kind != "in" || expr.op != "IN" || expr.left == nil || expr.left.kind != "field" || (expr.left.qualifier != "" && expr.left.qualifier != alias) || len(expr.args) == 0 {
		return "", nil, false
	}
	values = make([]interface{}, len(expr.args))
	for index, argument := range expr.args {
		if argument.kind != "literal" || argument.value == nil {
			return "", nil, false
		}
		if _, numeric := sqlNumber(argument.value); !numeric {
			return "", nil, false
		}
		values[index] = argument.value
	}
	return expr.left.name, values, true
}

type sqlColumnarNumericINPredicateKernel struct {
	kind        ColumnarNumericKind
	data        []byte
	validity    []byte
	rows        int
	intValues   []int64
	floatValues []float64
}

func newSQLColumnarNumericINPredicateKernel(column ColumnarNumericColumn, values []interface{}) (sqlColumnarNumericINPredicateKernel, bool) {
	if column.Rows < 0 || column.RowCount() != column.Rows || len(values) == 0 {
		return sqlColumnarNumericINPredicateKernel{}, false
	}
	kernel := sqlColumnarNumericINPredicateKernel{
		kind:     column.Kind,
		data:     column.Data,
		validity: column.Validity,
		rows:     column.Rows,
	}
	switch column.Kind {
	case ColumnarNumericInt64:
		kernel.intValues = make([]int64, 0, len(values))
		for _, value := range values {
			integer, ok := sqlColumnarNumericINInt64(value)
			if !ok {
				return sqlColumnarNumericINPredicateKernel{}, false
			}
			kernel.intValues = append(kernel.intValues, integer)
		}
		sort.Slice(kernel.intValues, func(left, right int) bool { return kernel.intValues[left] < kernel.intValues[right] })
		kernel.intValues = sqlColumnarNumericINDeduplicateInt64(kernel.intValues)
	case ColumnarNumericFloat64:
		kernel.floatValues = make([]float64, 0, len(values))
		for _, value := range values {
			number, ok := sqlNumber(value)
			if !ok || math.IsNaN(number) {
				return sqlColumnarNumericINPredicateKernel{}, false
			}
			kernel.floatValues = append(kernel.floatValues, number)
		}
		sort.Float64s(kernel.floatValues)
		kernel.floatValues = sqlColumnarNumericINDeduplicateFloat64(kernel.floatValues)
	default:
		return sqlColumnarNumericINPredicateKernel{}, false
	}
	return kernel, true
}

func (kernel sqlColumnarNumericINPredicateKernel) matches(row int) bool {
	if row < 0 || row >= kernel.rows {
		return false
	}
	if kernel.validity != nil && kernel.validity[row>>3]&(byte(1)<<uint(row&7)) == 0 {
		return false
	}
	bitsValue := binary.LittleEndian.Uint64(kernel.data[row<<3:])
	if kernel.kind == ColumnarNumericInt64 {
		value := int64(bitsValue)
		index := sort.Search(len(kernel.intValues), func(index int) bool { return kernel.intValues[index] >= value })
		return index < len(kernel.intValues) && kernel.intValues[index] == value
	}
	value := math.Float64frombits(bitsValue)
	if math.IsNaN(value) {
		return false
	}
	index := sort.Search(len(kernel.floatValues), func(index int) bool { return kernel.floatValues[index] >= value })
	return index < len(kernel.floatValues) && kernel.floatValues[index] == value
}

func sqlColumnarNumericINPredicateKernelForBatch(expr sqlExpr, alias string, batch ColumnarBatch) (sqlColumnarNumericINPredicateKernel, bool) {
	field, values, ok := sqlColumnarNumericINPredicate(expr, alias)
	if !ok || batch.Rows < 0 {
		return sqlColumnarNumericINPredicateKernel{}, false
	}
	column, ok := batch.NumericColumns[field]
	if !ok || column.Rows != batch.Rows {
		return sqlColumnarNumericINPredicateKernel{}, false
	}
	return newSQLColumnarNumericINPredicateKernel(column, values)
}

func sqlColumnarNumericINInt64(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		if uint64(value) > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(value), true
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		if value > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(value), true
	default:
		return 0, false
	}
}

func sqlColumnarNumericINDeduplicateInt64(values []int64) []int64 {
	if len(values) < 2 {
		return values
	}
	write := 1
	for _, value := range values[1:] {
		if value == values[write-1] {
			continue
		}
		values[write] = value
		write++
	}
	return values[:write]
}

func sqlColumnarNumericINDeduplicateFloat64(values []float64) []float64 {
	if len(values) < 2 {
		return values
	}
	write := 1
	for _, value := range values[1:] {
		if value == values[write-1] {
			continue
		}
		values[write] = value
		write++
	}
	return values[:write]
}
