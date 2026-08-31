package hatCache

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"hatrie_cache/hat/hatSql"
)

const (
	sqlColumnarLayoutCacheMinReads      = 2
	sqlColumnarLayoutOrderCacheMinReads = 8
	sqlColumnarLayoutCacheMaxEntries    = 32
	sqlColumnarLayoutCacheMaxBytes      = 4 << 20
	sqlColumnarLayoutCacheMaxCandidates = 128
)

// sqlColumnarLayoutCache retains a small number of decoded scalar layouts for
// observed repeated analytical reads. Raw cache values remain authoritative;
// every write drops layouts for its affected keys.
type sqlColumnarLayoutCache struct {
	mu                sync.RWMutex
	entries           map[sqlColumnarLayoutCacheKey]sqlColumnarLayoutCacheEntry
	observations      map[sqlColumnarLayoutCacheKey]uint8
	orderObservations map[sqlColumnarLayoutOrderCacheKey]uint8
	bytes             int
	sequence          uint64
	hits              atomic.Uint64
}

type sqlColumnarLayoutCacheKey struct {
	sourceKey string
	fields    string
}

type sqlColumnarLayoutCacheEntry struct {
	batch    hatSql.ColumnarBatch
	segments *hatSql.ColumnarNumericSegments
	orders   map[string][]uint32
	bytes    int
	sequence uint64
}

type sqlColumnarLayoutOrderCacheKey struct {
	layout sqlColumnarLayoutCacheKey
	field  string
}

type sqlColumnarLayoutCacheStats struct {
	Entries int
	Bytes   int
	Hits    uint64
}

func newSQLColumnarLayoutCacheKey(sourceKey string, fields []string) sqlColumnarLayoutCacheKey {
	ordered := append([]string(nil), fields...)
	sort.Strings(ordered)
	var builder strings.Builder
	for index, field := range ordered {
		if index > 0 {
			builder.WriteByte('|')
		}
		builder.WriteString(strconv.Itoa(len(field)))
		builder.WriteByte(':')
		builder.WriteString(field)
	}
	return sqlColumnarLayoutCacheKey{sourceKey: sourceKey, fields: builder.String()}
}

func (cache *sqlColumnarLayoutCache) lookup(key sqlColumnarLayoutCacheKey) (hatSql.ColumnarBatch, bool) {
	cache.mu.RLock()
	entry, ok := cache.entries[key]
	if ok {
		entry.batch = cloneSQLColumnarBatch(entry.batch)
	}
	cache.mu.RUnlock()
	if ok {
		cache.hits.Add(1)
	}
	return entry.batch, ok
}

func (cache *sqlColumnarLayoutCache) borrow(key sqlColumnarLayoutCacheKey) (hatSql.ColumnarBatch, bool) {
	batch, _, ok := cache.borrowSegments(key)
	return batch, ok
}

func (cache *sqlColumnarLayoutCache) borrowSegments(key sqlColumnarLayoutCacheKey) (hatSql.ColumnarBatch, *hatSql.ColumnarNumericSegments, bool) {
	cache.mu.RLock()
	entry, ok := cache.entries[key]
	cache.mu.RUnlock()
	if ok {
		cache.hits.Add(1)
	}
	return entry.batch, entry.segments, ok
}

func (cache *sqlColumnarLayoutCache) has(key sqlColumnarLayoutCacheKey) bool {
	cache.mu.RLock()
	_, ok := cache.entries[key]
	cache.mu.RUnlock()
	return ok
}

// observeOrder returns an immutable ascending row-ordinal projection only
// after the exact layout and requested order have each been observed twice.
// The sort itself happens outside the cache lock because cached batches are
// immutable; publication verifies that the layout was not invalidated first.
func (cache *sqlColumnarLayoutCache) observeOrder(key sqlColumnarLayoutCacheKey, field string) ([]uint32, bool) {
	orderKey := sqlColumnarLayoutOrderCacheKey{layout: key, field: field}
	cache.mu.Lock()
	entry, exists := cache.entries[key]
	if !exists {
		cache.mu.Unlock()
		return nil, false
	}
	if order, ok := entry.orders[field]; ok {
		cache.mu.Unlock()
		cache.hits.Add(1)
		return order, true
	}
	if cache.orderObservations == nil {
		cache.orderObservations = make(map[sqlColumnarLayoutOrderCacheKey]uint8)
	}
	reads := cache.orderObservations[orderKey] + 1
	if reads < sqlColumnarLayoutOrderCacheMinReads {
		if len(cache.orderObservations) >= sqlColumnarLayoutCacheMaxCandidates {
			for candidate := range cache.orderObservations {
				delete(cache.orderObservations, candidate)
				break
			}
		}
		cache.orderObservations[orderKey] = reads
		cache.mu.Unlock()
		return nil, false
	}
	delete(cache.orderObservations, orderKey)
	batch, sequence := entry.batch, entry.sequence
	cache.mu.Unlock()

	order, bytes, ok := sqlColumnarLayoutOrder(batch, field)
	if !ok {
		return nil, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry, exists = cache.entries[key]
	if !exists || entry.sequence != sequence {
		return nil, false
	}
	if existing, ok := entry.orders[field]; ok {
		cache.hits.Add(1)
		return existing, true
	}
	if cache.bytes+bytes > sqlColumnarLayoutCacheMaxBytes {
		return nil, false
	}
	if entry.orders == nil {
		entry.orders = make(map[string][]uint32)
	}
	entry.orders[field] = order
	entry.bytes += bytes
	cache.entries[key] = entry
	cache.bytes += bytes
	return order, true
}

func (cache *sqlColumnarLayoutCache) observe(key sqlColumnarLayoutCacheKey, batch hatSql.ColumnarBatch) {
	bytes, cacheable := sqlColumnarLayoutCacheBytes(batch)
	if !cacheable || bytes > sqlColumnarLayoutCacheMaxBytes {
		return
	}
	cached := cloneSQLColumnarBatch(batch)
	segments, segmentBytes := sqlColumnarNumericSegments(cached)
	bytes += segmentBytes

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, exists := cache.entries[key]; exists {
		return
	}
	if cache.observations == nil {
		cache.observations = make(map[sqlColumnarLayoutCacheKey]uint8)
	}
	reads := cache.observations[key] + 1
	if reads < sqlColumnarLayoutCacheMinReads {
		if len(cache.observations) >= sqlColumnarLayoutCacheMaxCandidates {
			for candidate := range cache.observations {
				delete(cache.observations, candidate)
				break
			}
		}
		cache.observations[key] = reads
		return
	}
	delete(cache.observations, key)
	if cache.entries == nil {
		cache.entries = make(map[sqlColumnarLayoutCacheKey]sqlColumnarLayoutCacheEntry)
	}
	for (len(cache.entries) >= sqlColumnarLayoutCacheMaxEntries || cache.bytes+bytes > sqlColumnarLayoutCacheMaxBytes) && len(cache.entries) > 0 {
		var oldestKey sqlColumnarLayoutCacheKey
		var oldest sqlColumnarLayoutCacheEntry
		for candidateKey, candidate := range cache.entries {
			if oldest.sequence == 0 || candidate.sequence < oldest.sequence {
				oldestKey, oldest = candidateKey, candidate
			}
		}
		delete(cache.entries, oldestKey)
		cache.bytes -= oldest.bytes
	}
	if cache.bytes+bytes > sqlColumnarLayoutCacheMaxBytes {
		return
	}
	cache.sequence++
	cache.entries[key] = sqlColumnarLayoutCacheEntry{batch: cached, segments: segments, bytes: bytes, sequence: cache.sequence}
	cache.bytes += bytes
}

func (cache *sqlColumnarLayoutCache) invalidate(sourceKeys ...string) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(sourceKeys) == 0 {
		clear(cache.entries)
		clear(cache.observations)
		clear(cache.orderObservations)
		cache.bytes = 0
		return
	}
	for _, sourceKey := range sourceKeys {
		for key, entry := range cache.entries {
			if key.sourceKey == sourceKey {
				delete(cache.entries, key)
				cache.bytes -= entry.bytes
			}
		}
		for key := range cache.observations {
			if key.sourceKey == sourceKey {
				delete(cache.observations, key)
			}
		}
		for key := range cache.orderObservations {
			if key.layout.sourceKey == sourceKey {
				delete(cache.orderObservations, key)
			}
		}
	}
}

func (cache *sqlColumnarLayoutCache) stats() sqlColumnarLayoutCacheStats {
	cache.mu.RLock()
	stats := sqlColumnarLayoutCacheStats{Entries: len(cache.entries), Bytes: cache.bytes, Hits: cache.hits.Load()}
	cache.mu.RUnlock()
	return stats
}

func cloneSQLColumnarBatch(batch hatSql.ColumnarBatch) hatSql.ColumnarBatch {
	clone := hatSql.ColumnarBatch{Rows: batch.Rows}
	if len(batch.Columns) > 0 {
		clone.Columns = make(map[string][]interface{}, len(batch.Columns))
		for field, values := range batch.Columns {
			clone.Columns[field] = append([]interface{}(nil), values...)
		}
	}
	if len(batch.Dictionaries) > 0 {
		clone.Dictionaries = make(map[string]hatSql.DictionaryColumn, len(batch.Dictionaries))
		for field, dictionary := range batch.Dictionaries {
			clone.Dictionaries[field] = hatSql.DictionaryColumn{
				Values: append([]string(nil), dictionary.Values...),
				Codes:  append([]uint32(nil), dictionary.Codes...),
			}
		}
	}
	return clone
}

func sqlColumnarLayoutOrder(batch hatSql.ColumnarBatch, field string) ([]uint32, int, bool) {
	if batch.Rows <= 0 || uint64(batch.Rows) > uint64(^uint32(0)) {
		return nil, 0, false
	}
	order := make([]uint32, batch.Rows)
	kind := byte(0)
	for row := 0; row < batch.Rows; row++ {
		value, available := batch.Value(field, row)
		if !available || value == nil {
			return nil, 0, false
		}
		if _, ok := value.(string); ok {
			if kind == 2 {
				return nil, 0, false
			}
			kind = 1
		} else if _, ok := hatSql.Number(value); ok {
			if kind == 1 {
				return nil, 0, false
			}
			kind = 2
		} else {
			return nil, 0, false
		}
		order[row] = uint32(row)
	}
	if kind == 0 {
		return nil, 0, false
	}
	sort.Slice(order, func(left, right int) bool {
		leftRow, rightRow := int(order[left]), int(order[right])
		leftValue, _ := batch.Value(field, leftRow)
		rightValue, _ := batch.Value(field, rightRow)
		if kind == 1 {
			leftText, rightText := leftValue.(string), rightValue.(string)
			if leftText != rightText {
				return leftText < rightText
			}
		} else {
			leftNumber, _ := hatSql.Number(leftValue)
			rightNumber, _ := hatSql.Number(rightValue)
			if leftNumber != rightNumber {
				return leftNumber < rightNumber
			}
		}
		return leftRow < rightRow
	})
	return order, len(order) * 4, true
}

func sqlColumnarLayoutCacheBytes(batch hatSql.ColumnarBatch) (int, bool) {
	if batch.Rows <= 0 {
		return 0, false
	}
	bytes := 0
	for field, values := range batch.Columns {
		if len(values) != batch.Rows {
			return 0, false
		}
		bytes += len(field) + len(values)*16
		for _, value := range values {
			switch typed := value.(type) {
			case nil:
			case bool:
				bytes++
			case float64:
				bytes += 8
			case string:
				bytes += len(typed) + 16
			default:
				return 0, false
			}
		}
	}
	for field, dictionary := range batch.Dictionaries {
		if len(dictionary.Codes) != batch.Rows {
			return 0, false
		}
		bytes += len(field) + len(dictionary.Codes)*4
		for _, value := range dictionary.Values {
			bytes += len(value) + 16
		}
	}
	if len(batch.Columns) == 0 && len(batch.Dictionaries) == 0 {
		return 0, false
	}
	return bytes * 2, true
}

func sqlColumnarNumericSegments(batch hatSql.ColumnarBatch) (*hatSql.ColumnarNumericSegments, int) {
	const rowsPerSegment = 256
	if batch.Rows < rowsPerSegment || len(batch.Columns) == 0 && len(batch.Dictionaries) == 0 {
		return nil, 0
	}
	columns := make(map[string][]hatSql.ColumnarNumericSegment)
	dictionaryCodeSets := make(map[string][]uint64)
	stringBloomFilters := make(map[string][]hatSql.ColumnarStringBloomSegment)
	bytes := 0
	for field, values := range batch.Columns {
		if len(values) != batch.Rows {
			continue
		}
		segments := make([]hatSql.ColumnarNumericSegment, 0, (batch.Rows+rowsPerSegment-1)/rowsPerSegment)
		numeric := false
		for start := 0; start < batch.Rows; start += rowsPerSegment {
			end := start + rowsPerSegment
			if end > batch.Rows {
				end = batch.Rows
			}
			segment := hatSql.ColumnarNumericSegment{}
			for _, value := range values[start:end] {
				number, ok := hatSql.Number(value)
				if !ok {
					continue
				}
				if !segment.Valid {
					segment.Minimum, segment.Maximum, segment.Valid = number, number, true
				} else if number < segment.Minimum {
					segment.Minimum = number
				} else if number > segment.Maximum {
					segment.Maximum = number
				}
			}
			if segment.Valid {
				numeric = true
			}
			segments = append(segments, segment)
		}
		if numeric {
			columns[field] = segments
			bytes += len(field) + 48 + len(segments)*32
		}
	}
	for field, values := range batch.Columns {
		if len(values) != batch.Rows {
			continue
		}
		filters := make([]hatSql.ColumnarStringBloomSegment, 0, (batch.Rows+rowsPerSegment-1)/rowsPerSegment)
		stringsOnly := true
		for start := 0; start < batch.Rows; start += rowsPerSegment {
			end := start + rowsPerSegment
			if end > batch.Rows {
				end = batch.Rows
			}
			filter := hatSql.ColumnarStringBloomSegment{}
			for _, value := range values[start:end] {
				text, ok := value.(string)
				if !ok {
					stringsOnly = false
					break
				}
				filter.Add(text)
			}
			if !stringsOnly {
				break
			}
			filters = append(filters, filter)
		}
		if stringsOnly {
			stringBloomFilters[field] = filters
			bytes += len(field) + 48 + len(filters)*128
		}
	}
	for field, dictionary := range batch.Dictionaries {
		if len(dictionary.Values) == 0 || len(dictionary.Values) > 64 || len(dictionary.Codes) != batch.Rows {
			continue
		}
		sets := make([]uint64, 0, (batch.Rows+rowsPerSegment-1)/rowsPerSegment)
		valid := true
		for start := 0; start < batch.Rows; start += rowsPerSegment {
			end := start + rowsPerSegment
			if end > batch.Rows {
				end = batch.Rows
			}
			set := uint64(0)
			for _, code := range dictionary.Codes[start:end] {
				if int(code) >= len(dictionary.Values) {
					valid = false
					break
				}
				set |= uint64(1) << code
			}
			if !valid {
				break
			}
			sets = append(sets, set)
		}
		if valid {
			dictionaryCodeSets[field] = sets
			bytes += len(field) + 48 + len(sets)*8
		}
	}
	if len(columns) == 0 && len(dictionaryCodeSets) == 0 && len(stringBloomFilters) == 0 {
		return nil, 0
	}
	return &hatSql.ColumnarNumericSegments{RowsPerSegment: rowsPerSegment, Columns: columns, DictionaryCodeSets: dictionaryCodeSets, StringBloomFilters: stringBloomFilters}, bytes
}
