package hatSql

type typedTableSparsePrimaryMarkLayout struct {
	segments *ColumnarNumericSegments
	bytes    int
	touched  uint64
}

// typedTableSparsePrimaryMarkCache is protected by its parent columnar cache
// mutex. It retains only the configured primary field bounds, not the batch or
// any other columnar sidecars.
type typedTableSparsePrimaryMarkCache struct {
	layouts map[string]typedTableSparsePrimaryMarkLayout
	bytes   int
	tick    uint64
}

func (cache *typedTableColumnarCache) lookupSparsePrimaryMarkLocked(key string) (*ColumnarNumericSegments, bool) {
	if !cache.options.SparsePrimaryMarkCache {
		return nil, false
	}
	mark, found := cache.sparsePrimaryMarks.layouts[key]
	if !found {
		return nil, false
	}
	cache.sparsePrimaryMarks.tick++
	mark.touched = cache.sparsePrimaryMarks.tick
	cache.sparsePrimaryMarks.layouts[key] = mark
	return mark.segments, true
}

func (cache *typedTableColumnarCache) touchSparsePrimaryMarkLocked(key string) {
	if !cache.options.SparsePrimaryMarkCache {
		return
	}
	mark, found := cache.sparsePrimaryMarks.layouts[key]
	if !found {
		return
	}
	cache.sparsePrimaryMarks.tick++
	mark.touched = cache.sparsePrimaryMarks.tick
	cache.sparsePrimaryMarks.layouts[key] = mark
}

func (cache *typedTableColumnarCache) observeSparsePrimaryMarkLocked(key string, segments *ColumnarNumericSegments) {
	if !cache.options.SparsePrimaryMarkCache || segments == nil || segments.SparsePrimaryField == "" {
		return
	}
	field := segments.SparsePrimaryField
	bounds, found := segments.Columns[field]
	if !found || len(bounds) == 0 {
		return
	}
	bytes := typedTableSparsePrimaryMarkBytes(field, len(bounds))
	if bytes > cache.options.SparsePrimaryMarkMaxBytes {
		return
	}
	if mark, found := cache.sparsePrimaryMarks.layouts[key]; found {
		cache.sparsePrimaryMarks.tick++
		mark.touched = cache.sparsePrimaryMarks.tick
		cache.sparsePrimaryMarks.layouts[key] = mark
		return
	}
	for cache.sparsePrimaryMarks.bytes+bytes > cache.options.SparsePrimaryMarkMaxBytes && len(cache.sparsePrimaryMarks.layouts) > 0 {
		cache.evictOldestSparsePrimaryMarkLocked()
	}
	if cache.sparsePrimaryMarks.bytes+bytes > cache.options.SparsePrimaryMarkMaxBytes {
		return
	}
	if cache.sparsePrimaryMarks.layouts == nil {
		cache.sparsePrimaryMarks.layouts = make(map[string]typedTableSparsePrimaryMarkLayout)
	}
	cache.sparsePrimaryMarks.tick++
	cache.sparsePrimaryMarks.layouts[key] = typedTableSparsePrimaryMarkLayout{
		segments: &ColumnarNumericSegments{
			RowsPerSegment:     segments.RowsPerSegment,
			SparsePrimaryField: field,
			Columns:            map[string][]ColumnarNumericSegment{field: bounds},
		},
		bytes:   bytes,
		touched: cache.sparsePrimaryMarks.tick,
	}
	cache.sparsePrimaryMarks.bytes += bytes
}

func (cache *typedTableColumnarCache) evictOldestSparsePrimaryMarkLocked() {
	var oldestKey string
	var oldest typedTableSparsePrimaryMarkLayout
	for key, mark := range cache.sparsePrimaryMarks.layouts {
		if oldestKey == "" || mark.touched < oldest.touched {
			oldestKey, oldest = key, mark
		}
	}
	if oldestKey == "" {
		return
	}
	delete(cache.sparsePrimaryMarks.layouts, oldestKey)
	cache.sparsePrimaryMarks.bytes -= oldest.bytes
}

func (cache *typedTableColumnarCache) clearSparsePrimaryMarksLocked() {
	cache.sparsePrimaryMarks.layouts = nil
	cache.sparsePrimaryMarks.bytes = 0
	cache.sparsePrimaryMarks.tick = 0
}

func typedTableSparsePrimaryMarkBytes(field string, segmentCount int) int {
	return 64 + len(field) + segmentCount*24
}
