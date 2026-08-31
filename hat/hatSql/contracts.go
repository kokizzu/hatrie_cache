package hatSql

import "context"

// QueryObserver receives one privacy-safe execution summary per query.
type QueryObserver interface {
	ObserveSQLQuery(QueryEvent)
}

// QueryObserverFunc adapts a function into QueryObserver.
type QueryObserverFunc func(QueryEvent)

func (fn QueryObserverFunc) ObserveSQLQuery(event QueryEvent) {
	if fn != nil {
		fn(event)
	}
}

// QueryEvent is an execution summary suitable for a structured log or metric.
// It deliberately excludes SQL text, cache keys, predicates, and row values.
type QueryEvent struct {
	QueryID            string          `json:"query_id"`
	ElapsedNanos       int64           `json:"elapsed_ns"`
	OutputRows         int             `json:"output_rows"`
	OutputColumns      int             `json:"output_columns"`
	ResultBytes        int             `json:"result_bytes"`
	OK                 bool            `json:"ok"`
	Slow               bool            `json:"slow"`
	Canceled           bool            `json:"canceled,omitempty"`
	CancellationReason string          `json:"cancellation_reason,omitempty"`
	Error              string          `json:"error,omitempty"`
	Operators          []QueryOperator `json:"operators,omitempty"`
}

// QueryOperator is a privacy-safe execution counter.
type QueryOperator struct {
	Node                 string   `json:"node"`
	InputRows            int      `json:"input_rows"`
	OutputRows           int      `json:"output_rows"`
	InputBytes           *int     `json:"input_bytes,omitempty"`
	OutputBytes          *int     `json:"output_bytes,omitempty"`
	ElapsedNanos         int64    `json:"elapsed_ns"`
	EstimatedRows        *int     `json:"estimated_rows,omitempty"`
	EstimateErrorPercent *float64 `json:"estimate_error_percent,omitempty"`
}

// SourceResolver supplies relational source rows. Nil rows are an empty source.
type SourceResolver interface {
	ResolveSQLSource(name string, key string) ([]Row, error)
}

// BorrowedSourceResolver optionally supplies an immutable source snapshot to
// the SQL executor. Returned row maps must remain valid for the query and must
// not be retained or mutated by the executor.
type BorrowedSourceResolver interface {
	BorrowSQLSource(name string, key string) ([]Row, bool, error)
}

// DictionaryColumn stores repeated text values once and addresses them through
// row-aligned codes. Values are ordered by first appearance for determinism.
type DictionaryColumn struct {
	Values []string
	Codes  []uint32
}

// ColumnarBatch stores one source scan as field-aligned value slices or compact
// dictionary columns. Every requested field must contain Rows values; absent
// JSON fields are nil in a plain column and are not dictionary encoded.
type ColumnarBatch struct {
	Columns      map[string][]interface{}
	Dictionaries map[string]DictionaryColumn
	Rows         int
}

// ColumnarNumericSegment stores the numeric value bounds for one contiguous
// columnar row segment. Invalid segments contain no numeric values.
type ColumnarNumericSegment struct {
	Minimum float64
	Maximum float64
	Valid   bool
}

// ColumnarStringBloomSegment is a fixed 1,024-bit Bloom filter for one
// contiguous string column segment. It has no false negatives and is used
// only to bypass segments that cannot satisfy a binary string equality.
type ColumnarStringBloomSegment struct {
	Bits [16]uint64
}

type columnarStringBloomProbe [3]uint16

// Add records one string in the segment Bloom filter.
func (segment *ColumnarStringBloomSegment) Add(value string) {
	if segment == nil {
		return
	}
	for _, bit := range newColumnarStringBloomProbe(value) {
		segment.Bits[bit>>6] |= uint64(1) << (bit & 63)
	}
}

// MayContain reports whether a string may be present in this segment.
func (segment ColumnarStringBloomSegment) MayContain(value string) bool {
	return segment.mayContainProbe(newColumnarStringBloomProbe(value))
}

func (segment ColumnarStringBloomSegment) mayContainProbe(probe columnarStringBloomProbe) bool {
	for _, bit := range probe {
		if segment.Bits[bit>>6]&(uint64(1)<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

func newColumnarStringBloomProbe(value string) columnarStringBloomProbe {
	first := columnarStringBloomHash(value)
	second := first ^ first>>33
	second *= 0xff51afd7ed558ccd
	second ^= second >> 33
	probe := columnarStringBloomProbe{}
	for index := uint64(0); index < uint64(len(probe)); index++ {
		probe[index] = uint16((first + index*second) & 1023)
	}
	return probe
}

func columnarStringBloomHash(value string) uint64 {
	hash := uint64(1469598103934665603)
	for index := 0; index < len(value); index++ {
		hash ^= uint64(value[index])
		hash *= 1099511628211
	}
	return hash
}

// ColumnarStringNGramBloomSegment is a fixed 1,024-bit Bloom filter over
// three-byte string grams. It can exclude a segment for a literal substring
// of at least three bytes without changing LIKE matching semantics.
type ColumnarStringNGramBloomSegment struct {
	Bits [16]uint64
}

// Add records every three-byte gram in value.
func (segment *ColumnarStringNGramBloomSegment) Add(value string) {
	if segment == nil {
		return
	}
	for index := 0; index+3 <= len(value); index++ {
		for _, bit := range newColumnarStringBloomProbe(value[index : index+3]) {
			segment.Bits[bit>>6] |= uint64(1) << (bit & 63)
		}
	}
}

// MayContainSubstring reports whether a substring can be present. Inputs
// shorter than three bytes are retained because this sidecar has no such gram.
func (segment ColumnarStringNGramBloomSegment) MayContainSubstring(value string) bool {
	if len(value) < 3 {
		return true
	}
	for index := 0; index+3 <= len(value); index++ {
		for _, bit := range newColumnarStringBloomProbe(value[index : index+3]) {
			if segment.Bits[bit>>6]&(uint64(1)<<(bit&63)) == 0 {
				return false
			}
		}
	}
	return true
}

// ColumnarNumericSegments stores immutable segment sidecars for one columnar
// batch. Columns holds numeric min/max bounds. DictionaryCodeSets holds exact
// membership masks for dictionary columns with at most 64 distinct values.
// StringBloomFilters holds fixed Bloom filters for all-string plain columns.
// Segment i covers RowsPerSegment consecutive rows.
type ColumnarNumericSegments struct {
	RowsPerSegment          int
	Columns                 map[string][]ColumnarNumericSegment
	DictionaryCodeSets      map[string][]uint64
	StringBloomFilters      map[string][]ColumnarStringBloomSegment
	StringNGramBloomFilters map[string][]ColumnarStringNGramBloomSegment
}

// FieldRows reports the physical row count retained for one field.
func (batch ColumnarBatch) FieldRows(field string) int {
	if dictionary, ok := batch.Dictionaries[field]; ok {
		return len(dictionary.Codes)
	}
	return len(batch.Columns[field])
}

// Value returns one logical field value regardless of its physical encoding.
func (batch ColumnarBatch) Value(field string, row int) (interface{}, bool) {
	if row < 0 {
		return nil, false
	}
	if dictionary, ok := batch.Dictionaries[field]; ok {
		if row >= len(dictionary.Codes) || int(dictionary.Codes[row]) >= len(dictionary.Values) {
			return nil, false
		}
		return dictionary.Values[dictionary.Codes[row]], true
	}
	values, ok := batch.Columns[field]
	if !ok || row >= len(values) {
		return nil, false
	}
	return values[row], true
}

// EncodeRepeatedStrings replaces highly repetitive all-string columns with a
// dictionary. The source value slice is released only when the encoded form is
// smaller in cardinality and has enough rows to justify its code array.
func (batch *ColumnarBatch) EncodeRepeatedStrings() {
	if batch == nil || batch.Rows < 4 || batch.Columns == nil {
		return
	}
	if batch.Dictionaries == nil {
		batch.Dictionaries = make(map[string]DictionaryColumn)
	}
	for field, values := range batch.Columns {
		if len(values) != batch.Rows {
			continue
		}
		positions := make(map[string]uint32)
		strings := make([]string, 0)
		codes := make([]uint32, len(values))
		allStrings := true
		for index, value := range values {
			text, ok := value.(string)
			if !ok {
				allStrings = false
				break
			}
			code, found := positions[text]
			if !found {
				code = uint32(len(strings))
				positions[text] = code
				strings = append(strings, text)
			}
			codes[index] = code
		}
		if !allStrings || len(strings)*2 > len(values) {
			continue
		}
		batch.Dictionaries[field] = DictionaryColumn{Values: strings, Codes: codes}
		delete(batch.Columns, field)
	}
}

// ColumnarSourceResolver optionally supplies selected source fields in
// columnar form. The SQL executor uses it only for a narrow single-source scan
// whose predicate and projection can retain the established row semantics.
type ColumnarSourceResolver interface {
	ResolveSQLColumnarSource(name, key string, fields []string) (ColumnarBatch, bool, error)
}

// BorrowedColumnarSourceResolver optionally supplies an immutable columnar
// batch to the SQL executor. Returned slices must remain valid for the query
// and must not be retained or mutated by the executor.
type BorrowedColumnarSourceResolver interface {
	BorrowSQLColumnarSource(name, key string, fields []string) (ColumnarBatch, bool, error)
}

// SegmentedColumnarSourceResolver optionally supplies an immutable batch with
// aligned numeric segment bounds. The executor uses the sidecar only for
// direct numeric predicates and otherwise retains the ordinary batch path.
type SegmentedColumnarSourceResolver interface {
	BorrowSQLColumnarSourceSegments(name, key string, fields []string) (ColumnarBatch, *ColumnarNumericSegments, bool, error)
}

// SortedColumnarSourceResolver optionally supplies an immutable ascending row
// ordinal projection for one exact cached columnar layout. Returned ordinals
// must remain valid for the query and must not be retained or mutated by the
// executor. Unavailable projections retain the ordinary columnar scan.
type SortedColumnarSourceResolver interface {
	BorrowSQLColumnarSourceOrder(name, key string, fields []string, orderField string) ([]uint32, bool, error)
}

// CompositeSortedColumnarSourceResolver optionally supplies an immutable
// ascending row-ordinal projection for an exact ordered field list. Callers
// must retain normal execution when a source declines the request.
type CompositeSortedColumnarSourceResolver interface {
	BorrowSQLColumnarSourceOrderFields(name, key string, fields, orderFields []string) ([]uint32, bool, error)
}

// DirectedCompositeSortedColumnarSourceResolver optionally supplies an
// immutable row-ordinal projection for an exact ordered field list and its
// direction per field. Callers must retain normal execution when a source
// declines the request.
type DirectedCompositeSortedColumnarSourceResolver interface {
	BorrowSQLColumnarSourceOrderBy(name, key string, fields, orderFields []string, descending []bool) ([]uint32, bool, error)
}

// ColumnarSourcePreferenceResolver optionally prefers a direct columnar scan
// for an exact source layout. It may return true only when the layout is
// immutable for the query and already available without source decoding.
// The preference changes the physical plan, never SQL result semantics.
type ColumnarSourcePreferenceResolver interface {
	PreferSQLColumnarSource(name, key string, fields []string) bool
}

// SourceVersionResolver optionally identifies an immutable source snapshot.
// The version must change whenever rows or values observable through the named
// source change. The condition cache uses it to avoid stale predicate matches;
// resolvers that cannot make this guarantee simply do not participate.
type SourceVersionResolver interface {
	SQLSourceVersion(name, key string) (version string, available bool, err error)
}

// SourceResolverFunc adapts a function into SourceResolver.
type SourceResolverFunc func(name string, key string) ([]Row, error)

func (fn SourceResolverFunc) ResolveSQLSource(name string, key string) ([]Row, error) {
	if fn == nil {
		return nil, nil
	}
	return fn(name, key)
}

// StreamSourceResolver supplies source rows one at a time for stream-compatible queries.
type StreamSourceResolver interface {
	StreamSQLSource(ctx context.Context, name string, key string, visit func(Row) error) error
}

// SnapshotLocker optionally coordinates a consistent source snapshot for a query.
type SnapshotLocker interface{ LockSQLSnapshot() func() }

// IndexedSourceResolver optionally resolves equality predicates through an index.
type IndexedSourceResolver interface {
	ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]Row, bool, error)
}

// CoveringIndexedSourceResolver optionally resolves an equality predicate from
// an index that already contains every required output field. Implementations
// must return rows containing only the requested fields and predicate field.
type CoveringIndexedSourceResolver interface {
	ResolveSQLCoveringSource(name, key, field string, value interface{}, fields []string) ([]Row, bool, error)
}

// RangeIndexedSourceResolver optionally resolves ordered comparisons through an index.
type RangeIndexedSourceResolver interface {
	ResolveSQLIndexedRangeSource(name, key, field, operator string, value interface{}) ([]Row, bool, error)
}

// TextIndexedSourceResolver optionally resolves an AND token query against a
// configured text field. Implementations must return only candidate rows; the
// executor evaluates CONTAINS again before returning results.
type TextIndexedSourceResolver interface {
	ResolveSQLTextSource(name, key, field, query string) ([]Row, bool, error)
}

// ExternalSourceResolver supplies a named, imported external table. It is
// used only by EXTERNAL('name') sources and never receives a filesystem path.
type ExternalSourceResolver interface {
	ResolveSQLExternalSource(name string) ([]Row, error)
}

// OrderedSourceResolver optionally reads one source field in SQL ORDER BY order.
type OrderedSourceResolver interface {
	ResolveSQLOrderedSource(name, key, field string, desc, nullsFirst, nullsLast bool) ([]Row, bool, error)
}

// OrderedStreamSourceResolver is the streaming counterpart of OrderedSourceResolver.
type OrderedStreamSourceResolver interface {
	StreamSQLOrderedSource(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, visit func(Row) error) (bool, error)
}

// CompositeIndexedSourceResolver optionally resolves multi-field equality predicates.
type CompositeIndexedSourceResolver interface {
	ResolveSQLCompositeIndexedSource(name, key string, fields []string, values []interface{}) ([]Row, bool, error)
}

// CompositeRangeIndexedSourceResolver optionally resolves equality predicates
// over an index prefix followed by one ordered range predicate.
type CompositeRangeIndexedSourceResolver interface {
	ResolveSQLCompositeIndexedRangeSource(name, key string, equalityFields []string, equalityValues []interface{}, rangeField, operator string, rangeValue interface{}) ([]Row, bool, error)
}

// BorrowedCompositeRangeIndexedSourceResolver supplies read-only indexed
// candidates to the SQL executor. Returned row maps must remain valid for the
// query and must not be retained or mutated by the executor.
type BorrowedCompositeRangeIndexedSourceResolver interface {
	BorrowSQLCompositeIndexedRangeSource(name, key string, equalityFields []string, equalityValues []interface{}, rangeField, operator string, rangeValue interface{}) ([]Row, bool, error)
}

// SecondaryIndexedSourceResolver optionally combines equality postings from
// independently configured secondary indexes. operation is either AND or OR;
// implementations return only candidate rows and the executor re-evaluates the
// complete predicate before publishing results.
type SecondaryIndexedSourceResolver interface {
	ResolveSQLSecondaryIndexedSource(name, key, operation string, fields []string, values []interface{}) ([]Row, bool, error)
}

// JSONIndexStatsResolver exposes exact current cardinality of a materialized
// JSON index without exposing indexed values. It lets the optimizer compare a
// hash build against index probes before materializing the right source.
type JSONIndexStatsResolver interface {
	SQLJSONIndexStats(key string, fields ...string) (JSONIndexStats, bool, error)
}

// IndexValueEstimator exposes the exact current posting-list size for one
// equality value. Implementations must return exact=false when a value cannot
// be represented by the index and available=false when no such index exists.
type IndexValueEstimator interface {
	SQLJSONIndexValueEstimate(key, field string, value interface{}) (rows int, exact bool, available bool, err error)
}

// JSONIndexFrequencyBucket reports a posting-list frequency without exposing values.
type JSONIndexFrequencyBucket struct {
	RowsPerKey   int `json:"rows_per_key"`
	DistinctKeys int `json:"distinct_keys"`
}

// JSONIndexStats describes one materialized JSON index.
type JSONIndexStats struct {
	Key                string
	Fields             []string
	Rows               int
	NullRows           int
	DistinctKeys       int
	MinRowsPerKey      int
	MaxRowsPerKey      int
	AverageRowsPerKey  float64
	FrequencyHistogram []JSONIndexFrequencyBucket
}
