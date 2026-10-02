package hatSql

import (
	"strings"
	"sync"
)

type ColumnarBatch struct {
	Columns map[string][]interface{}
	Rows    int
}

func (batch ColumnarBatch) Value(field string, row int) (interface{}, bool) {
	values, ok := batch.Columns[field]
	if !ok || row < 0 || row >= len(values) {
		return nil, false
	}
	return values[row], true
}

type round14Schema struct {
	SourceName string
	Name       string
}

type TypedTable struct {
	schema   round14Schema
	columnar round14ColumnarCache
	mu       sync.RWMutex
}

type round14ColumnarCacheOptions struct {
	Enabled          bool
	SortedOrderCache bool
	MaxBytes         int
}

type round14ColumnarLayout struct {
	orders         map[string][]uint32
	touched        uint64
	batch          ColumnarBatch
	sourceSequence uint64
	bytes          int
}

type round14ColumnarCache struct {
	mu                sync.Mutex
	options           round14ColumnarCacheOptions
	layouts           map[string]round14ColumnarLayout
	orderObservations map[typedTableColumnarOrderCacheKey]uint8
	tick              uint64
	bytes             int
}

func typedTableColumnarLayoutKey(fields []string) string {
	return strings.Join(fields, "\x00")
}

func sqlNumber(value interface{}) (float64, bool) {
	switch number := value.(type) {
	case int:
		return float64(number), true
	case int8:
		return float64(number), true
	case int16:
		return float64(number), true
	case int32:
		return float64(number), true
	case int64:
		return float64(number), true
	case uint:
		return float64(number), true
	case uint8:
		return float64(number), true
	case uint16:
		return float64(number), true
	case uint32:
		return float64(number), true
	case uint64:
		return float64(number), true
	case float32:
		return float64(number), true
	case float64:
		return number, true
	default:
		return 0, false
	}
}
