package hatSql

import (
	"math"
	"sort"
	"strings"
)

const (
	typedTableColumnarOrderCacheMinReads      = 8
	typedTableColumnarOrderCacheMaxCandidates = 128
)

type typedTableColumnarOrderCacheKey struct {
	layout string
	field  string
}

type typedTableColumnarOrderValue struct {
	text   string
	number float64
	kind   uint8
}

// BorrowSQLColumnarSourceOrder returns an immutable ascending ordinal
// projection for an admitted columnar layout. The order is derived only after
// repeated requests and is charged against the existing columnar cache limit.
// A cold or disabled layout retains the ordinary columnar top-N path.
func (table *TypedTable) BorrowSQLColumnarSourceOrder(name string, key string, fields []string, orderField string) ([]uint32, bool, error) {
	if table == nil || strings.ToUpper(strings.TrimSpace(name)) != table.schema.SourceName || key != table.schema.Name || orderField == "" {
		return nil, false, nil
	}
	cache := &table.columnar
	if !cache.options.Enabled || !cache.options.SortedOrderCache {
		return nil, false, nil
	}
	layoutKey := typedTableColumnarLayoutKey(fields)
	orderKey := typedTableColumnarOrderCacheKey{layout: layoutKey, field: orderField}

	table.mu.RLock()
	cache.mu.Lock()
	layout, found := cache.layouts[layoutKey]
	if !found {
		cache.mu.Unlock()
		table.mu.RUnlock()
		return nil, false, nil
	}
	if order, found := layout.orders[orderField]; found {
		cache.tick++
		layout.touched = cache.tick
		cache.layouts[layoutKey] = layout
		cache.mu.Unlock()
		table.mu.RUnlock()
		return order, true, nil
	}
	if cache.orderObservations == nil {
		cache.orderObservations = make(map[typedTableColumnarOrderCacheKey]uint8)
	}
	reads := cache.orderObservations[orderKey] + 1
	if reads < typedTableColumnarOrderCacheMinReads {
		if len(cache.orderObservations) >= typedTableColumnarOrderCacheMaxCandidates {
			for candidate := range cache.orderObservations {
				delete(cache.orderObservations, candidate)
				break
			}
		}
		cache.orderObservations[orderKey] = reads
		cache.mu.Unlock()
		table.mu.RUnlock()
		return nil, false, nil
	}
	delete(cache.orderObservations, orderKey)
	batch, sourceSequence := layout.batch, layout.sourceSequence
	cache.mu.Unlock()
	table.mu.RUnlock()

	order, bytes, ok := typedTableColumnarOrder(batch, orderField)
	if !ok {
		return nil, false, nil
	}

	table.mu.RLock()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	defer table.mu.RUnlock()
	layout, found = cache.layouts[layoutKey]
	if !found || layout.sourceSequence != sourceSequence {
		return nil, false, nil
	}
	if existing, found := layout.orders[orderField]; found {
		return existing, true, nil
	}
	if bytes > cache.options.MaxBytes || cache.bytes > cache.options.MaxBytes-bytes {
		return nil, false, nil
	}
	if layout.orders == nil {
		layout.orders = make(map[string][]uint32)
	}
	layout.orders[orderField] = order
	layout.bytes += bytes
	cache.layouts[layoutKey] = layout
	cache.bytes += bytes
	return order, true, nil
}

func typedTableColumnarOrder(batch ColumnarBatch, field string) ([]uint32, int, bool) {
	if batch.Rows <= 0 || uint64(batch.Rows) > uint64(^uint32(0)) || field == "" {
		return nil, 0, false
	}
	values := make([]typedTableColumnarOrderValue, batch.Rows)
	order := make([]uint32, batch.Rows)
	var kind uint8
	for row := 0; row < batch.Rows; row++ {
		value, available := batch.Value(field, row)
		if !available || value == nil {
			return nil, 0, false
		}
		if text, ok := value.(string); ok {
			if kind == 2 {
				return nil, 0, false
			}
			kind = 1
			values[row] = typedTableColumnarOrderValue{text: text, kind: 1}
			continue
		}
		number, ok := sqlNumber(value)
		if !ok || math.IsNaN(number) {
			return nil, 0, false
		}
		if kind == 1 {
			return nil, 0, false
		}
		kind = 2
		values[row] = typedTableColumnarOrderValue{number: number, kind: 2}
	}
	for row := range order {
		order[row] = uint32(row)
	}
	sort.Slice(order, func(left, right int) bool {
		leftValue := values[order[left]]
		rightValue := values[order[right]]
		if leftValue.kind == 1 {
			if leftValue.text != rightValue.text {
				return leftValue.text < rightValue.text
			}
		} else if leftValue.number != rightValue.number {
			return leftValue.number < rightValue.number
		}
		return order[left] < order[right]
	})
	return order, len(order) * 4, true
}
