package hatSql

type columnarFieldOffsetKind uint8

const (
	columnarFieldOffsetDictionary columnarFieldOffsetKind = iota + 1
	columnarFieldOffsetPacked
	columnarFieldOffsetBool
	columnarFieldOffsetNumeric
	columnarFieldOffsetPlain
	columnarFieldOffsetList
	columnarFieldOffsetNested
	columnarFieldOffsetMap
)

type columnarFieldOffset struct {
	kind  columnarFieldOffsetKind
	index uint32
}

type columnarFieldOffsets struct {
	offsets      map[string]columnarFieldOffset
	dictionaries []DictionaryColumn
	packed       []ColumnarPackedColumn
	bools        []ColumnarBoolColumn
	numeric      []ColumnarNumericColumn
	plain        [][]interface{}
	lists        []ColumnarListColumn
	nested       []ColumnarNestedColumn
	maps         []ColumnarMapColumn
}

// PrepareFieldOffsets builds an immutable representation lookup for a mixed
// columnar batch. It is useful for repeated reads of a cached batch and keeps
// the default batch path allocation-free. Call it only after the batch layout
// is complete; the packing methods invalidate it before changing a layout.
func (batch *ColumnarBatch) PrepareFieldOffsets() {
	if batch == nil || batch.fieldOffsets != nil {
		return
	}
	physicalMaps := 0
	if len(batch.Dictionaries) > 0 {
		physicalMaps++
	}
	if len(batch.PackedColumns) > 0 {
		physicalMaps++
	}
	if len(batch.BoolColumns) > 0 {
		physicalMaps++
	}
	if len(batch.NumericColumns) > 0 {
		physicalMaps++
	}
	if len(batch.Columns) > 0 {
		physicalMaps++
	}
	if len(batch.ListColumns) > 0 {
		physicalMaps++
	}
	if len(batch.NestedColumns) > 0 {
		physicalMaps++
	}
	if len(batch.MapColumns) > 0 {
		physicalMaps++
	}
	if physicalMaps <= 1 {
		return
	}

	fieldCount := len(batch.Dictionaries) + len(batch.PackedColumns) + len(batch.BoolColumns) +
		len(batch.NumericColumns) + len(batch.Columns) + len(batch.ListColumns) +
		len(batch.NestedColumns) + len(batch.MapColumns)
	cache := &columnarFieldOffsets{offsets: make(map[string]columnarFieldOffset, fieldCount)}
	add := func(field string, kind columnarFieldOffsetKind, index int) {
		if _, found := cache.offsets[field]; !found {
			cache.offsets[field] = columnarFieldOffset{kind: kind, index: uint32(index)}
		}
	}
	for field, column := range batch.Dictionaries {
		index := len(cache.dictionaries)
		cache.dictionaries = append(cache.dictionaries, column)
		add(field, columnarFieldOffsetDictionary, index)
	}
	for field, column := range batch.PackedColumns {
		index := len(cache.packed)
		cache.packed = append(cache.packed, column)
		add(field, columnarFieldOffsetPacked, index)
	}
	for field, column := range batch.BoolColumns {
		index := len(cache.bools)
		cache.bools = append(cache.bools, column)
		add(field, columnarFieldOffsetBool, index)
	}
	for field, column := range batch.NumericColumns {
		index := len(cache.numeric)
		cache.numeric = append(cache.numeric, column)
		add(field, columnarFieldOffsetNumeric, index)
	}
	for field, column := range batch.Columns {
		index := len(cache.plain)
		cache.plain = append(cache.plain, column)
		add(field, columnarFieldOffsetPlain, index)
	}
	for field, column := range batch.ListColumns {
		index := len(cache.lists)
		cache.lists = append(cache.lists, column)
		add(field, columnarFieldOffsetList, index)
	}
	for field, column := range batch.NestedColumns {
		index := len(cache.nested)
		cache.nested = append(cache.nested, column)
		add(field, columnarFieldOffsetNested, index)
	}
	for field, column := range batch.MapColumns {
		index := len(cache.maps)
		cache.maps = append(cache.maps, column)
		add(field, columnarFieldOffsetMap, index)
	}
	batch.fieldOffsets = cache
}

func (batch ColumnarBatch) valueAtPreparedField(field string, row int) (interface{}, bool) {
	cache := batch.fieldOffsets
	if cache == nil {
		return batch.valueWithoutDecompressedCache(field, row)
	}
	offset, found := cache.offsets[field]
	if !found {
		return nil, false
	}
	switch offset.kind {
	case columnarFieldOffsetDictionary:
		if int(offset.index) >= len(cache.dictionaries) {
			return nil, false
		}
		column := cache.dictionaries[int(offset.index)]
		code, valid := column.CodeAt(row)
		if !valid {
			return nil, false
		}
		value, valid := column.ValueAt(code)
		if !valid {
			return nil, false
		}
		return value, true
	case columnarFieldOffsetPacked:
		if int(offset.index) >= len(cache.packed) {
			return nil, false
		}
		column := cache.packed[int(offset.index)]
		return column.Value(row)
	case columnarFieldOffsetBool:
		if int(offset.index) >= len(cache.bools) {
			return nil, false
		}
		column := cache.bools[int(offset.index)]
		return column.Value(row)
	case columnarFieldOffsetNumeric:
		if int(offset.index) >= len(cache.numeric) {
			return nil, false
		}
		column := cache.numeric[int(offset.index)]
		return column.Value(row)
	case columnarFieldOffsetPlain:
		if int(offset.index) >= len(cache.plain) {
			return nil, false
		}
		values := cache.plain[int(offset.index)]
		if row < 0 || row >= len(values) {
			return nil, false
		}
		return values[row], true
	case columnarFieldOffsetList:
		if int(offset.index) >= len(cache.lists) {
			return nil, false
		}
		column := cache.lists[int(offset.index)]
		return column.Value(row)
	case columnarFieldOffsetNested:
		if int(offset.index) >= len(cache.nested) {
			return nil, false
		}
		column := cache.nested[int(offset.index)]
		return column.Value(row)
	case columnarFieldOffsetMap:
		if int(offset.index) >= len(cache.maps) {
			return nil, false
		}
		column := cache.maps[int(offset.index)]
		return column.Value(row)
	default:
		return nil, false
	}
}
