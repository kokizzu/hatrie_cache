package hatSql

import (
	"context"
	"encoding/binary"
	"math"
	"math/bits"
	"strings"
)

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

// SQLSourcePartition is one ordered physical partition of a logical SQL
// source. Rows are read-only for the duration of a query; the executor keeps
// their existing row semantics and does not deduplicate rows across
// partitions.
type SQLSourcePartition struct {
	Name string
	Rows []Row
}

// PartitionedSourceResolver optionally exposes one logical source as ordered
// physical partitions. It is an opt-in extension of SourceResolver: returning
// available=false preserves the normal BorrowSQLSource/ResolveSQLSource path.
// Implementations should return partitions in a deterministic order.
type PartitionedSourceResolver interface {
	ResolveSQLSourcePartitions(name string, key string) ([]SQLSourcePartition, bool, error)
}

// PartitionedOrderedSourceResolver optionally exposes a logical source as
// independently ordered physical partitions. Each returned partition must be
// in the requested order, and its name must be stable and unique within the
// result. The keyset paginator merges these streams without flattening or
// globally sorting all partition rows. Implementations must not mutate rows
// while the query is running.
type PartitionedOrderedSourceResolver interface {
	ResolveSQLOrderedSourcePartitions(name, key, field string, desc, nullsFirst, nullsLast bool) ([]SQLSourcePartition, bool, error)
}

// SQLPartitionPredicate is a planner-proven literal predicate that may be
// used to select a subset of physical partitions. Values are read-only for
// the duration of the resolver call. The SQL executor still evaluates the
// complete original predicate after the source is resolved.
type SQLPartitionPredicate struct {
	Field    string
	Operator string
	Values   []interface{}
}

// PartitionPruningSourceResolver optionally prunes a partitioned source from
// a literal equality or IN predicate. Returning available=false asks the
// executor to use PartitionedSourceResolver or SourceResolver instead.
// Implementations must never omit a partition that could contain a matching
// row; pruning is an optimization, not a source of SQL semantics.
type PartitionPruningSourceResolver interface {
	ResolveSQLSourcePartitionsForPredicate(name string, key string, predicate SQLPartitionPredicate) ([]SQLSourcePartition, bool, error)
}

// HistoricalSourceResolver optionally resolves a source at an immutable
// sequence frontier. It is required for QuerySubscriptionDefinition.AsOf;
// callers that only need live UpTo/progress delivery can use SourceResolver.
type HistoricalSourceResolver interface {
	ResolveSQLSourceAt(name string, key string, frontier uint64) ([]Row, error)
}

// BorrowedSourceResolver optionally supplies an immutable source snapshot to
// the SQL executor. Returned row maps must remain valid for the query and must
// not be retained or mutated by the executor.
type BorrowedSourceResolver interface {
	BorrowSQLSource(name string, key string) ([]Row, bool, error)
}

// DictionaryColumn stores repeated text values once and addresses them through
// row-aligned codes. Values are ordered by first appearance for determinism.
// Values and Codes are the compatibility representation. PackedValueData and
// ValueOffsets are an optional contiguous string representation selected by
// PackDictionaryValues. PackedCodes is an optional byte-aligned code
// representation selected by PackDictionaryCodes.
type DictionaryColumn struct {
	Values      []string
	Codes       []uint32
	PackedCodes []byte
	CodeWidth   uint8
	// codesTrusted is set only by constructors which validate every row code.
	// Untrusted manual or decoded dictionaries retain query-time code checks.
	codesTrusted    bool
	PackedValueData string
	ValueOffsets    []uint32
}

// ColumnarPackedColumn stores a row-aligned nullable column as a validity
// bitmap and a dense slice containing only non-NULL values. Ranks stores the
// number of valid values before each validity byte, followed by the total.
// It is an optional representation selected by PackNullableColumns.
type ColumnarPackedColumn struct {
	Values   []interface{}
	Validity []byte
	Ranks    []uint32
	Rows     int
}

// ColumnarBoolColumn stores boolean values as one bit per row. Validity is
// nil when every row is non-NULL; otherwise it contains one bit per row with
// set bits marking non-NULL values. It is an optional representation selected
// by PackBooleanColumns.
type ColumnarBoolColumn struct {
	Bits     []byte
	Validity []byte
	Rows     int
}

// ColumnarNumericKind identifies the fixed-width numeric representation.
type ColumnarNumericKind uint8

const (
	ColumnarNumericInt64 ColumnarNumericKind = iota + 1
	ColumnarNumericFloat64
)

// ColumnarNumericColumn stores int64 or float64 values as little-endian
// 64-bit words. Validity is nil when every row is non-NULL; otherwise it
// contains one bit per row with set bits marking non-NULL values. It is an
// optional representation selected by PackNumericColumns.
type ColumnarNumericColumn struct {
	Kind     ColumnarNumericKind
	Data     []byte
	Validity []byte
	Rows     int
}

// ColumnarBatch stores one source scan as field-aligned value slices, compact
// dictionary, nullable-packed, bit-packed boolean, or fixed-width numeric
// columns, or offset-based array/nested columns. Every requested field must
// contain Rows logical values; absent JSON fields are nil in a plain column
// and are not dictionary encoded.
type ColumnarBatch struct {
	Columns        map[string][]interface{}
	Dictionaries   map[string]DictionaryColumn
	PackedColumns  map[string]ColumnarPackedColumn
	BoolColumns    map[string]ColumnarBoolColumn
	NumericColumns map[string]ColumnarNumericColumn
	ListColumns    map[string]ColumnarListColumn
	NestedColumns  map[string]ColumnarNestedColumn
	Rows           int
}

// PackCompressedColumns applies the specialized compact representations to
// plain and dictionary columns. It is explicit opt-in; the legacy layout is
// unchanged unless this method is called.
func (batch *ColumnarBatch) PackCompressedColumns() {
	if batch == nil {
		return
	}
	// Specialized encodings avoid retaining interface values when their type
	// and layout permit a smaller representation.
	batch.PackBooleanColumns()
	batch.PackNumericColumns()
	batch.PackNullableColumns()
	batch.PackDictionaryValues()
	batch.PackDictionaryCodes()
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
// Segment i covers RowsPerSegment consecutive rows. SparsePrimaryField names
// an optional numeric field whose complete segment bounds are nondecreasing;
// the executor may binary-search those bounds for direct range predicates.
// Providers must leave it empty unless that ordering guarantee is true.
type ColumnarNumericSegments struct {
	RowsPerSegment          int
	SparsePrimaryField      string
	Columns                 map[string][]ColumnarNumericSegment
	DictionaryCodeSets      map[string][]uint64
	StringBloomFilters      map[string][]ColumnarStringBloomSegment
	StringNGramBloomFilters map[string][]ColumnarStringNGramBloomSegment
}

// FieldRows reports the physical row count retained for one field.
func (batch ColumnarBatch) FieldRows(field string) int {
	if dictionary, ok := batch.Dictionaries[field]; ok {
		if !dictionary.ValuesValid() {
			return 0
		}
		return dictionary.RowCount()
	}
	if column, ok := batch.PackedColumns[field]; ok {
		return column.RowCount()
	}
	if column, ok := batch.BoolColumns[field]; ok {
		return column.RowCount()
	}
	if column, ok := batch.NumericColumns[field]; ok {
		return column.RowCount()
	}
	if values, ok := batch.Columns[field]; ok {
		return len(values)
	}
	if column, ok := batch.ListColumns[field]; ok {
		if len(column.Offsets) == 0 {
			return 0
		}
		return len(column.Offsets) - 1
	}
	if column, ok := batch.NestedColumns[field]; ok {
		if len(column.Offsets) == 0 {
			return 0
		}
		return len(column.Offsets) - 1
	}
	return 0
}

// Value returns one logical field value regardless of its physical encoding.
func (batch ColumnarBatch) Value(field string, row int) (interface{}, bool) {
	if row < 0 {
		return nil, false
	}
	if dictionary, ok := batch.Dictionaries[field]; ok {
		code, ok := dictionary.CodeAt(row)
		if !ok {
			return nil, false
		}
		value, ok := dictionary.ValueAt(code)
		if !ok {
			return nil, false
		}
		return value, true
	}
	if column, ok := batch.PackedColumns[field]; ok {
		return column.Value(row)
	}
	if column, ok := batch.BoolColumns[field]; ok {
		return column.Value(row)
	}
	if column, ok := batch.NumericColumns[field]; ok {
		return column.Value(row)
	}
	values, ok := batch.Columns[field]
	if ok {
		if row >= len(values) {
			return nil, false
		}
		return values[row], true
	}
	if column, ok := batch.ListColumns[field]; ok {
		return column.Value(row)
	}
	if column, ok := batch.NestedColumns[field]; ok {
		return column.Value(row)
	}
	return nil, false
}

func columnarPackedBitmapBytes(rows int) int {
	if rows <= 0 {
		return 0
	}
	bytes := rows >> 3
	if rows&7 != 0 {
		bytes++
	}
	return bytes
}

func columnarPackedRankCount(rows int) int {
	bitmapBytes := columnarPackedBitmapBytes(rows)
	if bitmapBytes == 0 {
		return 0
	}
	return bitmapBytes + 1
}

// RowCount returns the logical row count when the packed column metadata is
// structurally valid. A malformed bitmap or rank index returns zero so source
// validation rejects the batch instead of reading out of bounds.
func (column ColumnarPackedColumn) RowCount() int {
	if column.Rows < 0 || len(column.Validity) != columnarPackedBitmapBytes(column.Rows) || len(column.Ranks) != columnarPackedRankCount(column.Rows) {
		return 0
	}
	expected := uint64(0)
	for index, validity := range column.Validity {
		if uint64(column.Ranks[index]) != expected {
			return 0
		}
		expected += uint64(bits.OnesCount8(validity))
	}
	if remainder := column.Rows & 7; remainder != 0 {
		validBits := byte((1 << uint(remainder)) - 1)
		if column.Validity[len(column.Validity)-1]&^validBits != 0 {
			return 0
		}
	}
	if len(column.Ranks) == 0 || uint64(column.Ranks[len(column.Ranks)-1]) != expected || expected != uint64(len(column.Values)) {
		return 0
	}
	return column.Rows
}

// Value returns the logical value at row. A NULL row returns (nil, true),
// while an out-of-range or malformed packed row returns (nil, false).
func (column ColumnarPackedColumn) Value(row int) (interface{}, bool) {
	if row < 0 || row >= column.Rows || len(column.Validity) != columnarPackedBitmapBytes(column.Rows) || len(column.Ranks) != columnarPackedRankCount(column.Rows) {
		return nil, false
	}
	byteIndex := row >> 3
	mask := byte(1 << uint(row&7))
	if int(column.Ranks[len(column.Ranks)-1]) != len(column.Values) {
		return nil, false
	}
	if column.Validity[byteIndex]&mask == 0 {
		return nil, true
	}
	index := int(column.Ranks[byteIndex]) + bits.OnesCount8(column.Validity[byteIndex]&(mask-1))
	if index < 0 || index >= len(column.Values) {
		return nil, false
	}
	return column.Values[index], true
}

// PackNullableColumns moves plain columns with enough NULL values into a
// bitmap-plus-dense representation when the estimated retained storage is
// smaller. It is explicit opt-in; legacy Columns remains the default.
func (batch *ColumnarBatch) PackNullableColumns() {
	if batch == nil || len(batch.Columns) == 0 {
		return
	}
	for field, values := range batch.Columns {
		rows := len(values)
		if rows == 0 {
			continue
		}
		nonNull := 0
		for _, value := range values {
			if value != nil {
				nonNull++
			}
		}
		if nonNull == rows {
			continue
		}
		validityBytes := columnarPackedBitmapBytes(rows)
		rankCount := columnarPackedRankCount(rows)
		legacyBytes := rows * 16
		packedBytes := nonNull*16 + validityBytes + rankCount*4
		if packedBytes >= legacyBytes {
			continue
		}

		packedValues := make([]interface{}, 0, nonNull)
		validity := make([]byte, validityBytes)
		ranks := make([]uint32, rankCount)
		validCount := 0
		for row, value := range values {
			if row&7 == 0 {
				ranks[row>>3] = uint32(validCount)
			}
			if value == nil {
				continue
			}
			validity[row>>3] |= byte(1 << uint(row&7))
			packedValues = append(packedValues, value)
			validCount++
		}
		ranks[len(ranks)-1] = uint32(validCount)
		if batch.PackedColumns == nil {
			batch.PackedColumns = make(map[string]ColumnarPackedColumn)
		}
		batch.PackedColumns[field] = ColumnarPackedColumn{
			Values:   packedValues,
			Validity: validity,
			Ranks:    ranks,
			Rows:     rows,
		}
		delete(batch.Columns, field)
	}
}

func columnarBitmapHasNoTrailingBits(bitmap []byte, rows int) bool {
	if len(bitmap) == 0 || rows&7 == 0 {
		return true
	}
	validBits := byte((1 << uint(rows&7)) - 1)
	return bitmap[len(bitmap)-1]&^validBits == 0
}

// RowCount returns the logical row count when the boolean bitmap metadata is
// structurally valid. Invalid lengths or trailing bits return zero so source
// validation rejects the batch instead of reading outside the logical rows.
func (column ColumnarBoolColumn) RowCount() int {
	bitmapBytes := columnarPackedBitmapBytes(column.Rows)
	if column.Rows < 0 || len(column.Bits) != bitmapBytes || (column.Validity != nil && len(column.Validity) != bitmapBytes) {
		return 0
	}
	if !columnarBitmapHasNoTrailingBits(column.Bits, column.Rows) || !columnarBitmapHasNoTrailingBits(column.Validity, column.Rows) {
		return 0
	}
	return column.Rows
}

// Value returns the logical boolean value at row. A NULL row returns
// (nil, true), while an out-of-range or malformed row returns (nil, false).
func (column ColumnarBoolColumn) Value(row int) (interface{}, bool) {
	bitmapBytes := columnarPackedBitmapBytes(column.Rows)
	if row < 0 || row >= column.Rows || len(column.Bits) != bitmapBytes || (column.Validity != nil && len(column.Validity) != bitmapBytes) {
		return nil, false
	}
	if !columnarBitmapHasNoTrailingBits(column.Bits, column.Rows) || !columnarBitmapHasNoTrailingBits(column.Validity, column.Rows) {
		return nil, false
	}
	byteIndex := row >> 3
	mask := byte(1 << uint(row&7))
	if column.Validity != nil && column.Validity[byteIndex]&mask == 0 {
		return nil, true
	}
	return column.Bits[byteIndex]&mask != 0, true
}

// PackBooleanColumns moves plain boolean columns into a bit-packed
// representation when the estimated retained storage is smaller. It is
// explicit opt-in; legacy Columns remains the default.
func (batch *ColumnarBatch) PackBooleanColumns() {
	if batch == nil || len(batch.Columns) == 0 {
		return
	}
	for field, values := range batch.Columns {
		rows := len(values)
		if rows == 0 {
			continue
		}
		hasNull := false
		valid := true
		for _, value := range values {
			if value == nil {
				hasNull = true
				continue
			}
			if _, ok := value.(bool); !ok {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}
		bitmapBytes := columnarPackedBitmapBytes(rows)
		packedBytes := bitmapBytes
		if hasNull {
			packedBytes += bitmapBytes
		}
		if packedBytes >= rows*16 {
			continue
		}
		bitsBitmap := make([]byte, bitmapBytes)
		var validityBitmap []byte
		if hasNull {
			validityBitmap = make([]byte, bitmapBytes)
		}
		for row, value := range values {
			if value == nil {
				continue
			}
			mask := byte(1 << uint(row&7))
			byteIndex := row >> 3
			if hasNull {
				validityBitmap[byteIndex] |= mask
			}
			if value.(bool) {
				bitsBitmap[byteIndex] |= mask
			}
		}
		if batch.BoolColumns == nil {
			batch.BoolColumns = make(map[string]ColumnarBoolColumn)
		}
		batch.BoolColumns[field] = ColumnarBoolColumn{Bits: bitsBitmap, Validity: validityBitmap, Rows: rows}
		delete(batch.Columns, field)
	}
}

func columnarFixedWidthBytes(rows, width int) int {
	if rows <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if rows > maxInt/width {
		return -1
	}
	return rows * width
}

// RowCount returns the logical row count when the numeric column metadata is
// structurally valid. Invalid widths, kinds, or trailing bitmap bits return
// zero so source validation rejects the batch.
func (column ColumnarNumericColumn) RowCount() int {
	dataBytes := columnarFixedWidthBytes(column.Rows, 8)
	bitmapBytes := columnarPackedBitmapBytes(column.Rows)
	if column.Rows < 0 || dataBytes < 0 || len(column.Data) != dataBytes || (column.Validity != nil && len(column.Validity) != bitmapBytes) {
		return 0
	}
	if column.Kind != ColumnarNumericInt64 && column.Kind != ColumnarNumericFloat64 {
		return 0
	}
	if !columnarBitmapHasNoTrailingBits(column.Validity, column.Rows) {
		return 0
	}
	return column.Rows
}

// Value returns the logical numeric value at row. A NULL row returns
// (nil, true), while an out-of-range or malformed row returns (nil, false).
func (column ColumnarNumericColumn) Value(row int) (interface{}, bool) {
	dataBytes := columnarFixedWidthBytes(column.Rows, 8)
	bitmapBytes := columnarPackedBitmapBytes(column.Rows)
	if row < 0 || row >= column.Rows || dataBytes < 0 || len(column.Data) != dataBytes || (column.Validity != nil && len(column.Validity) != bitmapBytes) {
		return nil, false
	}
	if column.Kind != ColumnarNumericInt64 && column.Kind != ColumnarNumericFloat64 || !columnarBitmapHasNoTrailingBits(column.Validity, column.Rows) {
		return nil, false
	}
	byteIndex := row >> 3
	mask := byte(1 << uint(row&7))
	if column.Validity != nil && column.Validity[byteIndex]&mask == 0 {
		return nil, true
	}
	offset := row << 3
	bitsValue := binary.LittleEndian.Uint64(column.Data[offset : offset+8])
	if column.Kind == ColumnarNumericInt64 {
		return int64(bitsValue), true
	}
	return math.Float64frombits(bitsValue), true
}

// PackNumericColumns moves plain int64 and float64 columns into a fixed-width
// little-endian representation when the estimated retained storage is smaller.
// It is explicit opt-in; legacy Columns remains the default.
func (batch *ColumnarBatch) PackNumericColumns() {
	if batch == nil || len(batch.Columns) == 0 {
		return
	}
	for field, values := range batch.Columns {
		rows := len(values)
		if rows == 0 {
			continue
		}
		kind := ColumnarNumericKind(0)
		hasNull := false
		valid := true
		for _, value := range values {
			if value == nil {
				hasNull = true
				continue
			}
			valueKind := ColumnarNumericKind(0)
			switch value.(type) {
			case int64:
				valueKind = ColumnarNumericInt64
			case float64:
				valueKind = ColumnarNumericFloat64
			default:
				valid = false
			}
			if !valid {
				break
			}
			if kind == 0 {
				kind = valueKind
			} else if kind != valueKind {
				valid = false
				break
			}
		}
		if !valid || kind == 0 {
			continue
		}
		dataBytes := columnarFixedWidthBytes(rows, 8)
		legacyBytes := columnarFixedWidthBytes(rows, 16)
		if dataBytes < 0 || legacyBytes < 0 {
			continue
		}
		bitmapBytes := 0
		if hasNull {
			bitmapBytes = columnarPackedBitmapBytes(rows)
		}
		if dataBytes+bitmapBytes >= legacyBytes {
			continue
		}
		data := make([]byte, dataBytes)
		var validityBitmap []byte
		if hasNull {
			validityBitmap = make([]byte, bitmapBytes)
		}
		for row, value := range values {
			if value == nil {
				continue
			}
			byteIndex := row >> 3
			mask := byte(1 << uint(row&7))
			if hasNull {
				validityBitmap[byteIndex] |= mask
			}
			var bitsValue uint64
			if kind == ColumnarNumericInt64 {
				bitsValue = uint64(value.(int64))
			} else {
				bitsValue = math.Float64bits(value.(float64))
			}
			binary.LittleEndian.PutUint64(data[row<<3:], bitsValue)
		}
		if batch.NumericColumns == nil {
			batch.NumericColumns = make(map[string]ColumnarNumericColumn)
		}
		batch.NumericColumns[field] = ColumnarNumericColumn{Kind: kind, Data: data, Validity: validityBitmap, Rows: rows}
		delete(batch.Columns, field)
	}
}

// ValueCount returns the number of dictionary values in either representation.
func (dictionary DictionaryColumn) ValueCount() int {
	if dictionary.Values != nil {
		return len(dictionary.Values)
	}
	if len(dictionary.ValueOffsets) < 1 {
		return 0
	}
	return len(dictionary.ValueOffsets) - 1
}

// ValuesValid verifies contiguous dictionary offsets without allocating. A
// legacy Values slice is already self-describing and therefore valid.
func (dictionary DictionaryColumn) ValuesValid() bool {
	if dictionary.Values != nil {
		return true
	}
	if dictionary.ValueOffsets == nil {
		return true
	}
	if len(dictionary.ValueOffsets) == 0 || dictionary.ValueOffsets[0] != 0 {
		return false
	}
	previous := uint64(0)
	for _, offset := range dictionary.ValueOffsets {
		current := uint64(offset)
		if current < previous || current > uint64(len(dictionary.PackedValueData)) {
			return false
		}
		previous = current
	}
	return previous == uint64(len(dictionary.PackedValueData))
}

// ValueAt returns one dictionary value without materializing the packed blob.
func (dictionary DictionaryColumn) ValueAt(code uint32) (string, bool) {
	if dictionary.Values != nil {
		if uint64(code) >= uint64(len(dictionary.Values)) {
			return "", false
		}
		return dictionary.Values[code], true
	}
	if uint64(code)+1 >= uint64(len(dictionary.ValueOffsets)) {
		return "", false
	}
	start := uint64(dictionary.ValueOffsets[code])
	end := uint64(dictionary.ValueOffsets[code+1])
	if start > end || end > uint64(len(dictionary.PackedValueData)) {
		return "", false
	}
	return dictionary.PackedValueData[int(start):int(end)], true
}

// PackDictionaryValues replaces valid dictionary string headers with one
// immutable string blob and row-independent offsets. It is explicit opt-in;
// Values remains the default representation for compatibility and predictable
// access cost.
func (batch *ColumnarBatch) PackDictionaryValues() {
	if batch == nil || len(batch.Dictionaries) == 0 {
		return
	}
	for field, dictionary := range batch.Dictionaries {
		if dictionary.Values == nil || len(dictionary.Values) < 2 {
			continue
		}
		total := 0
		valid := true
		for _, value := range dictionary.Values {
			if len(value) > int(^uint(0)>>1)-total {
				valid = false
				break
			}
			total += len(value)
		}
		if !valid || uint64(total) > uint64(^uint32(0)) {
			continue
		}
		legacyBytes := len(dictionary.Values)*16 + total
		packedBytes := 16 + (len(dictionary.Values)+1)*4 + total
		if packedBytes >= legacyBytes {
			continue
		}
		offsets := make([]uint32, len(dictionary.Values)+1)
		var builder strings.Builder
		builder.Grow(total)
		for index, value := range dictionary.Values {
			builder.WriteString(value)
			offsets[index+1] = uint32(builder.Len())
		}
		dictionary.PackedValueData = builder.String()
		dictionary.ValueOffsets = offsets
		dictionary.Values = nil
		batch.Dictionaries[field] = dictionary
	}
}

// RowCount returns the number of logical rows in a dictionary column. Invalid
// packed byte lengths return zero so source validation rejects the batch.
func (dictionary DictionaryColumn) RowCount() int {
	if dictionary.Codes != nil {
		return len(dictionary.Codes)
	}
	switch dictionary.CodeWidth {
	case 1:
		return len(dictionary.PackedCodes)
	case 2:
		if len(dictionary.PackedCodes)&1 != 0 {
			return 0
		}
		return len(dictionary.PackedCodes) >> 1
	default:
		return 0
	}
}

// CodeAt returns one validated dictionary code without materializing the
// packed representation. It accepts both the legacy and packed layouts.
func (dictionary DictionaryColumn) CodeAt(row int) (uint32, bool) {
	if row < 0 {
		return 0, false
	}
	if dictionary.Codes != nil {
		if row >= len(dictionary.Codes) {
			return 0, false
		}
		code := dictionary.Codes[row]
		if uint64(code) >= uint64(dictionary.ValueCount()) {
			return 0, false
		}
		return code, true
	}
	var code uint32
	switch dictionary.CodeWidth {
	case 1:
		if row >= len(dictionary.PackedCodes) {
			return 0, false
		}
		code = uint32(dictionary.PackedCodes[row])
	case 2:
		offset := row << 1
		if len(dictionary.PackedCodes)&1 != 0 || offset < 0 || offset+1 >= len(dictionary.PackedCodes) {
			return 0, false
		}
		code = uint32(dictionary.PackedCodes[offset]) | uint32(dictionary.PackedCodes[offset+1])<<8
	default:
		return 0, false
	}
	if uint64(code) >= uint64(dictionary.ValueCount()) {
		return 0, false
	}
	return code, true
}

// PackDictionaryCodes replaces valid low-cardinality uint32 row codes with a
// byte-aligned 8- or 16-bit representation. Wider dictionaries retain the
// legacy representation because packing them would not reduce storage.
func (batch *ColumnarBatch) PackDictionaryCodes() {
	if batch == nil || len(batch.Dictionaries) == 0 {
		return
	}
	for field, dictionary := range batch.Dictionaries {
		if dictionary.Codes == nil || len(dictionary.Codes) == 0 || dictionary.ValueCount() == 0 {
			continue
		}
		width := 0
		switch {
		case dictionary.ValueCount() <= 1<<8:
			width = 1
		case dictionary.ValueCount() <= 1<<16:
			width = 2
		default:
			continue
		}
		if len(dictionary.Codes) > int(^uint(0)>>1)/width {
			continue
		}
		packed := make([]byte, len(dictionary.Codes)*width)
		valid := true
		for row, code := range dictionary.Codes {
			if uint64(code) >= uint64(dictionary.ValueCount()) {
				valid = false
				break
			}
			offset := row * width
			packed[offset] = byte(code)
			if width == 2 {
				packed[offset+1] = byte(code >> 8)
			}
		}
		if !valid {
			continue
		}
		dictionary.PackedCodes = packed
		dictionary.CodeWidth = uint8(width)
		dictionary.Codes = nil
		batch.Dictionaries[field] = dictionary
	}
}

// EncodeRepeatedStrings replaces all-string columns with a dictionary when the
// estimated retained layout is smaller. The estimate accounts for row count,
// string width, dictionary codes, and unique string headers.
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
		totalStringBytes := 0
		uniqueStringBytes := 0
		allStrings := true
		maxUnique := columnarDictionaryMaximumUnique(len(values))
		tooManyUnique := false
		var codes []uint32
		var prefixCodes [8]uint32
		duplicateIndex := -1
		for index, value := range values {
			text, ok := value.(string)
			if !ok {
				allStrings = false
				break
			}
			code, found := positions[text]
			if !found {
				if len(strings)+1 > maxUnique {
					tooManyUnique = true
					break
				}
				code = uint32(len(strings))
				positions[text] = code
				strings = append(strings, text)
				uniqueStringBytes += len(text)
			}
			totalStringBytes += len(text)
			if index < len(prefixCodes) {
				prefixCodes[index] = code
			}
			if !found {
				continue
			}
			duplicateIndex = index
			break
		}
		if duplicateIndex >= 0 && allStrings && !tooManyUnique {
			codes = make([]uint32, len(values))
			prefixCount := duplicateIndex
			if prefixCount > len(prefixCodes) {
				prefixCount = len(prefixCodes)
			}
			copy(codes, prefixCodes[:prefixCount])
			for previous := prefixCount; previous < duplicateIndex; previous++ {
				codes[previous] = positions[values[previous].(string)]
			}
			codes[duplicateIndex] = positions[values[duplicateIndex].(string)]
			for index := duplicateIndex + 1; index < len(values); index++ {
				text, ok := values[index].(string)
				if !ok {
					allStrings = false
					break
				}
				code, found := positions[text]
				if !found {
					if len(strings)+1 > maxUnique {
						tooManyUnique = true
						break
					}
					code = uint32(len(strings))
					positions[text] = code
					strings = append(strings, text)
					uniqueStringBytes += len(text)
				}
				totalStringBytes += len(text)
				codes[index] = code
			}
		}
		if !allStrings || tooManyUnique || !columnarDictionaryLayoutSmaller(len(values), len(strings), totalStringBytes, uniqueStringBytes) {
			continue
		}
		if codes == nil {
			continue
		}
		batch.Dictionaries[field] = DictionaryColumn{Values: strings, Codes: codes, codesTrusted: true}
		delete(batch.Columns, field)
	}
}

func columnarDictionaryMaximumUnique(rows int) int {
	if rows <= 0 {
		return 0
	}
	maximum := rows - rows/4
	if rows%4 != 0 {
		maximum--
	}
	return maximum
}

func columnarDictionaryLayoutSmaller(rows, unique, totalStringBytes, uniqueStringBytes int) bool {
	if rows < 4 || unique == 0 || uniqueStringBytes < 0 || totalStringBytes < uniqueStringBytes {
		return false
	}
	// Keep the dictionary's fixed code and string-header storage lower than the
	// plain interface slice even when repeated strings share backing bytes.
	if uint64(unique)*4 > uint64(rows)*3 {
		return false
	}
	plainBytes := uint64(rows)*16 + uint64(totalStringBytes)
	dictionaryBytes := uint64(rows)*4 + uint64(unique)*16 + uint64(uniqueStringBytes)
	return dictionaryBytes < plainBytes
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

// MultikeyIndexedSourceResolver optionally resolves ARRAY_CONTAINS predicates
// against an index that stores one posting per distinct array element. It
// returns candidates only; the executor evaluates ARRAY_CONTAINS again before
// publishing results.
type MultikeyIndexedSourceResolver interface {
	ResolveSQLMultikeySource(name, key, field string, value interface{}) ([]Row, bool, error)
}

// BorrowedIndexedSourceResolver optionally resolves equality predicates through
// an immutable index posting list. The returned rows must stay valid for the
// active SQL snapshot and callers must not mutate them. SQL uses this contract
// for joins to avoid copying the same dimension rows for every probe.
type BorrowedIndexedSourceResolver interface {
	BorrowSQLIndexedSource(name, key, field string, value interface{}) ([]Row, bool, error)
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

// PrefixIndexedSourceResolver optionally resolves a simple binary-collation
// LIKE prefix through an ordered index. Implementations must return only
// candidate rows; the executor evaluates LIKE again before publishing results.
type PrefixIndexedSourceResolver interface {
	ResolveSQLPrefixSource(name, key, field, prefix string) ([]Row, bool, error)
}

// BorrowedPrefixIndexedSourceResolver optionally resolves a simple binary
// collation LIKE prefix through an immutable ordered index without cloning row
// maps. Returned rows must remain valid for the active SQL snapshot, and the
// executor will not mutate them.
type BorrowedPrefixIndexedSourceResolver interface {
	BorrowSQLPrefixSource(name, key, field, prefix string) ([]Row, bool, error)
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

// LookupSourceResolver optionally resolves a literal equality predicate from
// an arrangement owned by an external or remote source. Returned rows are
// candidates: the executor evaluates the complete predicate before publishing
// results. Returning available=false retains the ordinary full-source scan.
type LookupSourceResolver interface {
	ResolveSQLLookupSource(name, key, field string, value interface{}) ([]Row, bool, error)
}

// OrderedSourceResolver optionally reads one source field in SQL ORDER BY order.
type OrderedSourceResolver interface {
	ResolveSQLOrderedSource(name, key, field string, desc, nullsFirst, nullsLast bool) ([]Row, bool, error)
}

// OrderedStreamSourceResolver is the streaming counterpart of OrderedSourceResolver.
type OrderedStreamSourceResolver interface {
	StreamSQLOrderedSource(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, visit func(Row) error) (bool, error)
}

// KeysetPosition identifies one row in an ordered source. Tie is a
// source-defined stable position among equal values, and Valid distinguishes
// the first page from a position whose order value is NULL.
type KeysetPosition struct {
	Value interface{} `json:"value,omitempty"`
	Tie   uint64      `json:"tie"`
	Valid bool        `json:"valid"`
}

// KeysetOrderedStreamSourceResolver optionally starts an ordered source after
// a stable position. It is an opt-in extension; callers can keep using the
// offset cursor when a source does not provide it.
type KeysetOrderedStreamSourceResolver interface {
	StreamSQLOrderedSourceAfter(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, after KeysetPosition, visit func(Row, KeysetPosition) error) (bool, error)
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
