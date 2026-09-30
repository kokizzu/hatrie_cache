package hatSql

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

const (
	typedTableColumnarOrderCacheMinReads      = 8
	typedTableColumnarOrderCacheMaxCandidates = 128
	typedTableColumnarRadixOrderMinRows       = 256
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

// BorrowSQLColumnarSourceOrderFields returns an immutable ascending ordinal
// projection for an admitted composite columnar order. The projection is
// admitted only after repeated compatible requests and shares the existing
// columnar cache byte limit.
func (table *TypedTable) BorrowSQLColumnarSourceOrderFields(name string, key string, fields, orderFields []string) ([]uint32, bool, error) {
	return table.borrowSQLColumnarCompositeOrder(name, key, fields, orderFields, nil)
}

// BorrowSQLColumnarSourceOrderBy returns an immutable ordinal projection for
// an admitted composite order with one direction per field. An empty
// descending slice means all fields are ascending.
func (table *TypedTable) BorrowSQLColumnarSourceOrderBy(name string, key string, fields, orderFields []string, descending []bool) ([]uint32, bool, error) {
	return table.borrowSQLColumnarCompositeOrder(name, key, fields, orderFields, descending)
}

func (table *TypedTable) borrowSQLColumnarCompositeOrder(name string, key string, fields, orderFields []string, descending []bool) ([]uint32, bool, error) {
	if table == nil || strings.ToUpper(strings.TrimSpace(name)) != table.schema.SourceName || key != table.schema.Name {
		return nil, false, nil
	}
	orderKey, ok := typedTableColumnarCompositeOrderKey(orderFields, descending)
	if !ok {
		return nil, false, nil
	}
	cache := &table.columnar
	if !cache.options.Enabled || !cache.options.SortedOrderCache {
		return nil, false, nil
	}
	layoutKey := typedTableColumnarLayoutKey(fields)
	observationKey := typedTableColumnarOrderCacheKey{layout: layoutKey, field: orderKey}

	table.mu.RLock()
	cache.mu.Lock()
	layout, found := cache.layouts[layoutKey]
	if !found {
		cache.mu.Unlock()
		table.mu.RUnlock()
		return nil, false, nil
	}
	if order, found := layout.orders[orderKey]; found {
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
	reads := cache.orderObservations[observationKey] + 1
	if reads < typedTableColumnarOrderCacheMinReads {
		if len(cache.orderObservations) >= typedTableColumnarOrderCacheMaxCandidates {
			for candidate := range cache.orderObservations {
				delete(cache.orderObservations, candidate)
				break
			}
		}
		cache.orderObservations[observationKey] = reads
		cache.mu.Unlock()
		table.mu.RUnlock()
		return nil, false, nil
	}
	delete(cache.orderObservations, observationKey)
	batch, sourceSequence := layout.batch, layout.sourceSequence
	cache.mu.Unlock()
	table.mu.RUnlock()

	order, bytes, ok := typedTableColumnarOrderFields(batch, orderFields, descending)
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
	if existing, found := layout.orders[orderKey]; found {
		return existing, true, nil
	}
	if bytes > cache.options.MaxBytes || cache.bytes > cache.options.MaxBytes-bytes {
		return nil, false, nil
	}
	if layout.orders == nil {
		layout.orders = make(map[string][]uint32)
	}
	layout.orders[orderKey] = order
	layout.bytes += bytes
	cache.layouts[layoutKey] = layout
	cache.bytes += bytes
	return order, true, nil
}

func typedTableColumnarCompositeOrderKey(orderFields []string, descending []bool) (string, bool) {
	if len(orderFields) < 2 || (len(descending) != 0 && len(descending) != len(orderFields)) {
		return "", false
	}
	var builder strings.Builder
	builder.WriteString("\x00C;")
	for index, field := range orderFields {
		if field == "" {
			return "", false
		}
		builder.WriteString(strconv.Itoa(len(field)))
		builder.WriteByte(':')
		builder.WriteString(field)
		if len(descending) > 0 && descending[index] {
			builder.WriteByte('D')
		} else {
			builder.WriteByte('A')
		}
		builder.WriteByte(';')
	}
	return builder.String(), true
}

func typedTableColumnarOrder(batch ColumnarBatch, field string) ([]uint32, int, bool) {
	if batch.Rows <= 0 || uint64(batch.Rows) > uint64(^uint32(0)) || field == "" {
		return nil, 0, false
	}
	if order, bytes, ok := typedTableColumnarInt64Order(batch, field); ok {
		return order, bytes, true
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

func typedTableColumnarInt64Order(batch ColumnarBatch, field string) ([]uint32, int, bool) {
	if batch.Rows < typedTableColumnarRadixOrderMinRows || field == "" {
		return nil, 0, false
	}
	values := make([]int64, batch.Rows)
	for row := 0; row < batch.Rows; row++ {
		value, available := batch.Value(field, row)
		if !available {
			return nil, 0, false
		}
		number, ok := value.(int64)
		if !ok {
			return nil, 0, false
		}
		values[row] = number
	}
	order := make([]uint32, batch.Rows)
	for row := range order {
		order[row] = uint32(row)
	}
	sortTypedTableColumnarInt64Order(order, values, false)
	return order, len(order) * 4, true
}

func typedTableColumnarInt64OrderFields(batch ColumnarBatch, orderFields []string, descending []bool) ([]uint32, int, bool) {
	if batch.Rows < typedTableColumnarRadixOrderMinRows || len(orderFields) < 2 || (len(descending) != 0 && len(descending) != len(orderFields)) {
		return nil, 0, false
	}
	if len(orderFields) > int(^uint(0)>>1)/batch.Rows {
		return nil, 0, false
	}
	for _, field := range orderFields {
		if field == "" {
			return nil, 0, false
		}
		if values, ok := typedTableColumnarPlainValues(batch, field); ok {
			for _, value := range values {
				if _, ok := value.(int64); !ok {
					return nil, 0, false
				}
			}
			continue
		}
		for row := 0; row < batch.Rows; row++ {
			value, available := batch.Value(field, row)
			if !available {
				return nil, 0, false
			}
			if _, ok := value.(int64); !ok {
				return nil, 0, false
			}
		}
	}
	values := make([]int64, batch.Rows*len(orderFields))
	for fieldIndex, field := range orderFields {
		fieldValues := values[fieldIndex*batch.Rows : (fieldIndex+1)*batch.Rows]
		if column, ok := typedTableColumnarPlainValues(batch, field); ok {
			for row, value := range column {
				fieldValues[row] = value.(int64)
			}
			continue
		}
		for row := 0; row < batch.Rows; row++ {
			value, available := batch.Value(field, row)
			if !available {
				return nil, 0, false
			}
			number, ok := value.(int64)
			if !ok {
				return nil, 0, false
			}
			fieldValues[row] = number
		}
	}
	order := make([]uint32, batch.Rows)
	for row := range order {
		order[row] = uint32(row)
	}
	sortTypedTableColumnarInt64OrderFields(order, values, batch.Rows, descending)
	return order, len(order) * 4, true
}

func typedTableColumnarPlainValues(batch ColumnarBatch, field string) ([]interface{}, bool) {
	if batch.decompressedBlockCache != nil || batch.fieldOffsets != nil ||
		batch.Dictionaries != nil || batch.PackedColumns != nil || batch.BoolColumns != nil ||
		batch.NumericColumns != nil || batch.ListColumns != nil || batch.NestedColumns != nil ||
		batch.MapColumns != nil || batch.JSONSubcolumns != nil {
		return nil, false
	}
	values, ok := batch.Columns[field]
	if !ok || len(values) < batch.Rows {
		return nil, false
	}
	return values[:batch.Rows], true
}

func sortTypedTableColumnarInt64Order(order []uint32, values []int64, descending bool) {
	if len(order) < 2 {
		return
	}
	scratch := make([]uint32, len(order))
	sortTypedTableColumnarInt64OrderWithScratch(order, scratch, values, descending)
}

func sortTypedTableColumnarInt64OrderFields(order []uint32, values []int64, rows int, descending []bool) {
	if len(order) < 2 || rows <= 0 || len(values) < rows || len(values)%rows != 0 {
		return
	}
	scratch := make([]uint32, len(order))
	fieldCount := len(values) / rows
	for fieldIndex := fieldCount - 1; fieldIndex >= 0; fieldIndex-- {
		fieldValues := values[fieldIndex*rows : (fieldIndex+1)*rows]
		isDescending := len(descending) > 0 && fieldIndex < len(descending) && descending[fieldIndex]
		sortTypedTableColumnarInt64OrderWithScratch(order, scratch, fieldValues, isDescending)
	}
}

func sortTypedTableColumnarInt64OrderWithScratch(order, scratch []uint32, values []int64, descending bool) {
	if len(order) < 2 {
		return
	}
	source, destination := order, scratch
	for pass := uint(0); pass < 8; pass++ {
		var counts [256]int
		shift := pass * 8
		for _, row := range source {
			key := uint64(values[int(row)]) ^ (uint64(1) << 63)
			if descending {
				key = ^key
			}
			counts[byte(key>>shift)]++
		}
		offset := 0
		for index, count := range counts {
			counts[index] = offset
			offset += count
		}
		for _, row := range source {
			key := uint64(values[int(row)]) ^ (uint64(1) << 63)
			if descending {
				key = ^key
			}
			bucket := byte(key >> shift)
			index := counts[bucket]
			destination[index] = row
			counts[bucket] = index + 1
		}
		source, destination = destination, source
	}
}

func typedTableColumnarOrderFields(batch ColumnarBatch, orderFields []string, descending []bool) ([]uint32, int, bool) {
	if batch.Rows <= 0 || uint64(batch.Rows) > uint64(^uint32(0)) || len(orderFields) == 0 || (len(descending) != 0 && len(descending) != len(orderFields)) {
		return nil, 0, false
	}
	if len(orderFields) > int(^uint(0)>>1)/batch.Rows {
		return nil, 0, false
	}
	if order, bytes, ok := typedTableColumnarInt64OrderFields(batch, orderFields, descending); ok {
		return order, bytes, true
	}
	values := make([]typedTableColumnarOrderValue, batch.Rows*len(orderFields))
	kinds := make([]uint8, len(orderFields))
	order := make([]uint32, batch.Rows)
	for row := 0; row < batch.Rows; row++ {
		for fieldIndex, field := range orderFields {
			value, available := batch.Value(field, row)
			if !available || value == nil {
				return nil, 0, false
			}
			valueIndex := row*len(orderFields) + fieldIndex
			if text, ok := value.(string); ok {
				if kinds[fieldIndex] == 2 {
					return nil, 0, false
				}
				kinds[fieldIndex] = 1
				values[valueIndex] = typedTableColumnarOrderValue{text: text, kind: 1}
				continue
			}
			number, ok := sqlNumber(value)
			if !ok || math.IsNaN(number) {
				return nil, 0, false
			}
			if kinds[fieldIndex] == 1 {
				return nil, 0, false
			}
			kinds[fieldIndex] = 2
			values[valueIndex] = typedTableColumnarOrderValue{number: number, kind: 2}
		}
	}
	for row := range order {
		order[row] = uint32(row)
	}
	sort.Slice(order, func(left, right int) bool {
		leftRow, rightRow := int(order[left]), int(order[right])
		for fieldIndex := range orderFields {
			leftValue := values[leftRow*len(orderFields)+fieldIndex]
			rightValue := values[rightRow*len(orderFields)+fieldIndex]
			if leftValue.kind == 1 {
				if leftValue.text != rightValue.text {
					if len(descending) > 0 && descending[fieldIndex] {
						return leftValue.text > rightValue.text
					}
					return leftValue.text < rightValue.text
				}
			} else if leftValue.number != rightValue.number {
				if len(descending) > 0 && descending[fieldIndex] {
					return leftValue.number > rightValue.number
				}
				return leftValue.number < rightValue.number
			}
		}
		return leftRow < rightRow
	})
	return order, len(order) * 4, true
}
