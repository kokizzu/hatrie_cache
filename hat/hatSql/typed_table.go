package hatSql

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ErrTypedTableChangesCompacted reports that a requested changefeed sequence
// is older than the retained changefeed boundary.
var ErrTypedTableChangesCompacted = errors.New("typed table changes compacted")

// TypedTableKind identifies the fixed physical representation of one column.
type TypedTableKind uint8

const (
	TypedTableNull TypedTableKind = iota
	TypedTableString
	TypedTableInt64
	TypedTableFloat64
	TypedTableBool
)

// TypedTableColumn declares one schema-checked table column.
type TypedTableColumn struct {
	Name string
	Kind TypedTableKind
}

const (
	typedTableColumnarCacheDefaultMaxBytes       = 4 << 20
	typedTableColumnarCacheDefaultMinReads       = 2
	typedTableColumnarCacheDefaultRowsPerSegment = 256
)

// TypedTableColumnarCacheOptions configures the optional immutable SQL layout
// cache. It is disabled by default so existing typed tables keep their current
// memory behavior.
type TypedTableColumnarCacheOptions struct {
	Enabled          bool
	MaxBytes         int
	MinReads         int
	RowsPerSegment   int
	AdaptiveSegments bool
}

// TypedTableSchema describes one compact table. Name is used as the SQL source
// key, and SourceName defaults to CACHE for compatibility with cache SQL.
type TypedTableSchema struct {
	Name          string
	SourceName    string
	Columns       []TypedTableColumn
	ColumnarCache TypedTableColumnarCacheOptions
}

// TypedTableValue stores one scalar table value. A value with Valid false is
// SQL NULL; use the constructors to create non-null values.
type TypedTableValue struct {
	Kind    TypedTableKind
	String  string
	Int64   int64
	Float64 float64
	Bool    bool
	Valid   bool
}

func TypedNull() TypedTableValue { return TypedTableValue{} }
func TypedString(value string) TypedTableValue {
	return TypedTableValue{Kind: TypedTableString, String: value, Valid: true}
}
func TypedInt64(value int64) TypedTableValue {
	return TypedTableValue{Kind: TypedTableInt64, Int64: value, Valid: true}
}
func TypedFloat64(value float64) TypedTableValue {
	return TypedTableValue{Kind: TypedTableFloat64, Float64: value, Valid: true}
}
func TypedBool(value bool) TypedTableValue {
	return TypedTableValue{Kind: TypedTableBool, Bool: value, Valid: true}
}

// TypedTableChange is one immutable before/after mutation suitable for exact
// incremental aggregates. Insert has a nil Before; delete has a nil After.
type TypedTableChange struct {
	Sequence       uint64
	Operation, Key string
	Before, After  []TypedTableValue
}

type typedTableColumnStorage struct {
	kind    TypedTableKind
	strings []string
	int64s  []int64
	floats  []float64
	bools   []bool
	valid   []bool
}

type typedTableColumnarLayout struct {
	batch    ColumnarBatch
	segments *ColumnarNumericSegments
	bytes    int
	touched  uint64
}

type typedTableColumnarCache struct {
	mu           sync.Mutex
	options      TypedTableColumnarCacheOptions
	layouts      map[string]typedTableColumnarLayout
	observations map[string]int
	bytes        int
	tick         uint64
}

func (storage *typedTableColumnStorage) append(value TypedTableValue) {
	storage.valid = append(storage.valid, value.Valid)
	switch storage.kind {
	case TypedTableString:
		storage.strings = append(storage.strings, value.String)
	case TypedTableInt64:
		storage.int64s = append(storage.int64s, value.Int64)
	case TypedTableFloat64:
		storage.floats = append(storage.floats, value.Float64)
	case TypedTableBool:
		storage.bools = append(storage.bools, value.Bool)
	}
}

func (storage *typedTableColumnStorage) set(index int, value TypedTableValue) {
	storage.valid[index] = value.Valid
	switch storage.kind {
	case TypedTableString:
		storage.strings[index] = value.String
	case TypedTableInt64:
		storage.int64s[index] = value.Int64
	case TypedTableFloat64:
		storage.floats[index] = value.Float64
	case TypedTableBool:
		storage.bools[index] = value.Bool
	}
}

func (storage *typedTableColumnStorage) value(index int) TypedTableValue {
	value := TypedTableValue{Kind: storage.kind, Valid: storage.valid[index]}
	switch storage.kind {
	case TypedTableString:
		value.String = storage.strings[index]
	case TypedTableInt64:
		value.Int64 = storage.int64s[index]
	case TypedTableFloat64:
		value.Float64 = storage.floats[index]
	case TypedTableBool:
		value.Bool = storage.bools[index]
	}
	return value
}

func (storage *typedTableColumnStorage) copy(index, from int) {
	storage.valid[index] = storage.valid[from]
	switch storage.kind {
	case TypedTableString:
		storage.strings[index] = storage.strings[from]
	case TypedTableInt64:
		storage.int64s[index] = storage.int64s[from]
	case TypedTableFloat64:
		storage.floats[index] = storage.floats[from]
	case TypedTableBool:
		storage.bools[index] = storage.bools[from]
	}
}

func (storage *typedTableColumnStorage) truncate(length int) {
	storage.valid = storage.valid[:length]
	switch storage.kind {
	case TypedTableString:
		storage.strings = storage.strings[:length]
	case TypedTableInt64:
		storage.int64s = storage.int64s[:length]
	case TypedTableFloat64:
		storage.floats = storage.floats[:length]
	case TypedTableBool:
		storage.bools = storage.bools[:length]
	}
}

// TypedTable is a schema-checked row store with per-column primitive slices.
// It is opt-in and implements the established source-resolver contracts.
type TypedTable struct {
	mu        sync.RWMutex
	schema    TypedTableSchema
	columns   []typedTableColumnStorage
	byName    map[string]int
	keys      []string
	positions map[string]int
	columnar  typedTableColumnarCache

	changes          []TypedTableChange
	compactedThrough uint64
	sequence         uint64
}

// NewTypedTable validates schema and creates an empty compact table.
func NewTypedTable(schema TypedTableSchema) (*TypedTable, error) {
	schema.Name = strings.TrimSpace(schema.Name)
	if schema.Name == "" {
		return nil, fmt.Errorf("typed table name is required")
	}
	schema.SourceName = strings.ToUpper(strings.TrimSpace(schema.SourceName))
	if schema.SourceName == "" {
		schema.SourceName = "CACHE"
	}
	if len(schema.Columns) == 0 {
		return nil, fmt.Errorf("typed table columns are required")
	}
	schema.ColumnarCache = normalizeTypedTableColumnarCacheOptions(schema.ColumnarCache)
	table := &TypedTable{
		schema:    schema,
		byName:    make(map[string]int, len(schema.Columns)),
		positions: make(map[string]int),
		columnar: typedTableColumnarCache{
			options: schema.ColumnarCache,
		},
	}
	table.columns = make([]typedTableColumnStorage, len(schema.Columns))
	for index := range table.schema.Columns {
		column := &table.schema.Columns[index]
		column.Name = strings.TrimSpace(column.Name)
		if column.Name == "" {
			return nil, fmt.Errorf("typed table column %d has an empty name", index)
		}
		if _, exists := table.byName[column.Name]; exists {
			return nil, fmt.Errorf("typed table has duplicate column %q", column.Name)
		}
		if column.Kind < TypedTableString || column.Kind > TypedTableBool {
			return nil, fmt.Errorf("typed table column %q has invalid kind", column.Name)
		}
		table.byName[column.Name] = index
		table.columns[index].kind = column.Kind
	}
	return table, nil
}

func normalizeTypedTableColumnarCacheOptions(options TypedTableColumnarCacheOptions) TypedTableColumnarCacheOptions {
	if !options.Enabled {
		return TypedTableColumnarCacheOptions{}
	}
	if options.MaxBytes <= 0 {
		options.MaxBytes = typedTableColumnarCacheDefaultMaxBytes
	}
	if options.MinReads <= 0 {
		options.MinReads = typedTableColumnarCacheDefaultMinReads
	}
	if options.RowsPerSegment <= 0 {
		options.RowsPerSegment = typedTableColumnarCacheDefaultRowsPerSegment
	}
	return options
}

// Upsert inserts or replaces a complete schema-ordered row and returns its
// ordered changefeed record.
func (table *TypedTable) Upsert(key string, values []TypedTableValue) (TypedTableChange, error) {
	if table == nil {
		return TypedTableChange{}, fmt.Errorf("typed table is nil")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return TypedTableChange{}, fmt.Errorf("typed table key is required")
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if err := table.validateValues(values); err != nil {
		return TypedTableChange{}, err
	}
	table.clearColumnarLayoutsLocked()
	index, exists := table.positions[key]
	change := TypedTableChange{Key: key, After: cloneTypedTableValues(values)}
	if exists {
		change.Operation = "UPDATE"
		change.Before = table.rowLocked(index)
		for column := range table.columns {
			table.columns[column].set(index, values[column])
		}
	} else {
		change.Operation = "INSERT"
		index = len(table.keys)
		table.positions[key] = index
		table.keys = append(table.keys, key)
		for column := range table.columns {
			table.columns[column].append(values[column])
		}
	}
	return table.appendChangeLocked(change), nil
}

// Delete removes key in O(columns) time and returns false only when key does
// not exist. Physical row order is unspecified, as in SQL without ORDER BY.
func (table *TypedTable) Delete(key string) (TypedTableChange, error) {
	if table == nil {
		return TypedTableChange{}, fmt.Errorf("typed table is nil")
	}
	key = strings.TrimSpace(key)
	table.mu.Lock()
	defer table.mu.Unlock()
	index, exists := table.positions[key]
	if !exists {
		return TypedTableChange{}, fmt.Errorf("typed table key %q does not exist", key)
	}
	table.clearColumnarLayoutsLocked()
	change := TypedTableChange{Operation: "DELETE", Key: key, Before: table.rowLocked(index)}
	last := len(table.keys) - 1
	if index != last {
		moved := table.keys[last]
		table.keys[index] = moved
		table.positions[moved] = index
		for column := range table.columns {
			table.columns[column].copy(index, last)
		}
	}
	delete(table.positions, key)
	table.keys = table.keys[:last]
	for column := range table.columns {
		table.columns[column].truncate(last)
	}
	return table.appendChangeLocked(change), nil
}

// ChangesAfter returns at most limit ordered immutable changes and the current
// tail sequence. A caller behind CompactedThrough must rebuild from a trusted
// table snapshot rather than silently skipping mutations.
func (table *TypedTable) ChangesAfter(sequence uint64, limit int) ([]TypedTableChange, uint64, error) {
	if table == nil {
		return nil, 0, fmt.Errorf("typed table is nil")
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if sequence < table.compactedThrough {
		return nil, table.sequence, ErrTypedTableChangesCompacted
	}
	if limit <= 0 || sequence >= table.sequence {
		return nil, table.sequence, nil
	}
	start := int(sequence - table.compactedThrough)
	end := start + limit
	if end > len(table.changes) {
		end = len(table.changes)
	}
	changes := make([]TypedTableChange, end-start)
	for index := range changes {
		changes[index] = cloneTypedTableChange(table.changes[start+index])
	}
	return changes, table.sequence, nil
}

// CompactChangesThrough discards changefeed entries through sequence. It never
// modifies table rows and is safe only after every consumer checkpoint passed
// the requested sequence.
func (table *TypedTable) CompactChangesThrough(sequence uint64) error {
	if table == nil {
		return fmt.Errorf("typed table is nil")
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if sequence < table.compactedThrough || sequence > table.sequence {
		return fmt.Errorf("typed table compaction sequence %d is outside %d..%d", sequence, table.compactedThrough, table.sequence)
	}
	drop := int(sequence - table.compactedThrough)
	if drop > 0 {
		retained := make([]TypedTableChange, len(table.changes)-drop)
		copy(retained, table.changes[drop:])
		table.changes = retained
		table.compactedThrough = sequence
	}
	return nil
}

// ResolveSQLSource exposes a snapshot through the established row resolver.
func (table *TypedTable) ResolveSQLSource(name string, key string) ([]Row, error) {
	if table == nil || strings.ToUpper(strings.TrimSpace(name)) != table.schema.SourceName || key != table.schema.Name {
		return nil, nil
	}
	return table.Rows(), nil
}

// ResolveSQLColumnarSource exposes only requested primitive columns so the
// existing SQL columnar path can avoid constructing source row maps.
func (table *TypedTable) ResolveSQLColumnarSource(name string, key string, fields []string) (ColumnarBatch, bool, error) {
	if table == nil || strings.ToUpper(strings.TrimSpace(name)) != table.schema.SourceName || key != table.schema.Name {
		return ColumnarBatch{}, false, nil
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	layoutKey := typedTableColumnarLayoutKey(fields)
	if batch, found := table.lookupColumnarLayoutLocked(layoutKey); found {
		return batch, true, nil
	}
	batch := table.columnarBatchLocked(fields)
	table.observeColumnarLayoutLocked(layoutKey, batch)
	return batch, true, nil
}

// BorrowSQLColumnarSource returns a cached immutable layout after its repeated
// field set has been admitted. Cold layouts retain ResolveSQLColumnarSource.
func (table *TypedTable) BorrowSQLColumnarSource(name string, key string, fields []string) (ColumnarBatch, bool, error) {
	if table == nil || strings.ToUpper(strings.TrimSpace(name)) != table.schema.SourceName || key != table.schema.Name {
		return ColumnarBatch{}, false, nil
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	batch, found := table.lookupColumnarLayoutLocked(typedTableColumnarLayoutKey(fields))
	return batch, found, nil
}

// BorrowSQLColumnarSourceSegments returns an immutable cached layout with
// aligned numeric bounds. Cold or oversized layouts retain the normal scan.
func (table *TypedTable) BorrowSQLColumnarSourceSegments(name string, key string, fields []string) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	if table == nil || strings.ToUpper(strings.TrimSpace(name)) != table.schema.SourceName || key != table.schema.Name {
		return ColumnarBatch{}, nil, false, nil
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	layout, found := table.lookupColumnarLayoutWithSegmentsLocked(typedTableColumnarLayoutKey(fields))
	if !found {
		return ColumnarBatch{}, nil, false, nil
	}
	return layout.batch, layout.segments, true, nil
}

// PreferSQLColumnarSource reports whether an immutable cached layout can
// serve the exact field set without rebuilding interface columns.
func (table *TypedTable) PreferSQLColumnarSource(name string, key string, fields []string) bool {
	if table == nil || strings.ToUpper(strings.TrimSpace(name)) != table.schema.SourceName || key != table.schema.Name {
		return false
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	_, found := table.lookupColumnarLayoutLocked(typedTableColumnarLayoutKey(fields))
	return found
}

// SQLSourceVersion identifies the current cached-table snapshot for safe
// condition-cache reuse. Disabled layout caches retain prior resolver behavior.
func (table *TypedTable) SQLSourceVersion(name string, key string) (string, bool, error) {
	if table == nil || strings.ToUpper(strings.TrimSpace(name)) != table.schema.SourceName || key != table.schema.Name {
		return "", false, nil
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if !table.columnar.options.Enabled {
		return "", false, nil
	}
	return strconv.FormatUint(table.sequence, 10), true, nil
}

// Rows returns independent row maps for diagnostics or row-resolver callers.
func (table *TypedTable) Rows() []Row {
	if table == nil {
		return nil
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	rows := make([]Row, len(table.keys))
	for row := range rows {
		values := table.rowLocked(row)
		rows[row] = table.rowMapLocked(values)
	}
	return rows
}

// Schema returns a copy of the immutable table schema.
func (table *TypedTable) Schema() TypedTableSchema {
	if table == nil {
		return TypedTableSchema{}
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	schema := table.schema
	schema.Columns = append([]TypedTableColumn(nil), schema.Columns...)
	return schema
}

func (table *TypedTable) columnarBatchLocked(fields []string) ColumnarBatch {
	batch := ColumnarBatch{Columns: make(map[string][]interface{}, len(fields)), Rows: len(table.keys)}
	for _, field := range fields {
		column, found := table.byName[field]
		if !found {
			continue
		}
		values := make([]interface{}, len(table.keys))
		for row := range values {
			values[row] = typedTableValueInterface(table.columns[column].value(row))
		}
		batch.Columns[field] = values
	}
	batch.EncodeRepeatedStrings()
	return batch
}

func (table *TypedTable) lookupColumnarLayoutLocked(key string) (ColumnarBatch, bool) {
	layout, found := table.lookupColumnarLayoutWithSegmentsLocked(key)
	return layout.batch, found
}

func (table *TypedTable) lookupColumnarLayoutWithSegmentsLocked(key string) (typedTableColumnarLayout, bool) {
	cache := &table.columnar
	if !cache.options.Enabled {
		return typedTableColumnarLayout{}, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	layout, found := cache.layouts[key]
	if !found {
		return typedTableColumnarLayout{}, false
	}
	cache.tick++
	layout.touched = cache.tick
	cache.layouts[key] = layout
	return layout, true
}

func (table *TypedTable) observeColumnarLayoutLocked(key string, batch ColumnarBatch) {
	cache := &table.columnar
	if !cache.options.Enabled {
		return
	}
	cache.mu.Lock()
	if layout, found := cache.layouts[key]; found {
		cache.tick++
		layout.touched = cache.tick
		cache.layouts[key] = layout
		cache.mu.Unlock()
		return
	}
	if cache.observations == nil {
		cache.observations = make(map[string]int)
	}
	cache.observations[key]++
	if cache.observations[key] < cache.options.MinReads {
		cache.mu.Unlock()
		return
	}
	delete(cache.observations, key)
	cache.mu.Unlock()

	segments := table.columnarNumericSegmentsLocked(batch)
	bytes := typedTableColumnarBatchBytes(batch, segments)
	if bytes > cache.options.MaxBytes {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if layout, found := cache.layouts[key]; found {
		cache.tick++
		layout.touched = cache.tick
		cache.layouts[key] = layout
		return
	}
	for cache.bytes+bytes > cache.options.MaxBytes && len(cache.layouts) > 0 {
		cache.evictOldestLocked()
	}
	if cache.bytes+bytes > cache.options.MaxBytes {
		return
	}
	if cache.layouts == nil {
		cache.layouts = make(map[string]typedTableColumnarLayout)
	}
	cache.tick++
	cache.layouts[key] = typedTableColumnarLayout{batch: batch, segments: segments, bytes: bytes, touched: cache.tick}
	cache.bytes += bytes
}

func (cache *typedTableColumnarCache) evictOldestLocked() {
	var oldestKey string
	var oldest typedTableColumnarLayout
	for key, layout := range cache.layouts {
		if oldestKey == "" || layout.touched < oldest.touched {
			oldestKey, oldest = key, layout
		}
	}
	if oldestKey == "" {
		return
	}
	delete(cache.layouts, oldestKey)
	cache.bytes -= oldest.bytes
}

func (table *TypedTable) clearColumnarLayoutsLocked() {
	cache := &table.columnar
	if !cache.options.Enabled {
		return
	}
	cache.mu.Lock()
	cache.layouts = nil
	cache.observations = nil
	cache.bytes = 0
	cache.tick = 0
	cache.mu.Unlock()
}

func (table *TypedTable) columnarNumericSegmentsLocked(batch ColumnarBatch) *ColumnarNumericSegments {
	rowsPerSegment := table.columnar.options.RowsPerSegment
	if table.columnar.options.AdaptiveSegments {
		rowsPerSegment = typedTableAdaptiveRowsPerSegment(rowsPerSegment, batch.Rows)
	}
	if rowsPerSegment <= 0 || batch.Rows == 0 {
		return nil
	}
	segments := &ColumnarNumericSegments{RowsPerSegment: rowsPerSegment, Columns: make(map[string][]ColumnarNumericSegment)}
	for field := range batch.Columns {
		column, found := table.byName[field]
		if !found || table.columns[column].kind != TypedTableInt64 && table.columns[column].kind != TypedTableFloat64 {
			continue
		}
		bounds := make([]ColumnarNumericSegment, (batch.Rows+rowsPerSegment-1)/rowsPerSegment)
		for segment := range bounds {
			start := segment * rowsPerSegment
			end := start + rowsPerSegment
			if end > batch.Rows {
				end = batch.Rows
			}
			for row := start; row < end; row++ {
				if !table.columns[column].valid[row] {
					continue
				}
				value := 0.0
				if table.columns[column].kind == TypedTableInt64 {
					value = float64(table.columns[column].int64s[row])
				} else {
					value = table.columns[column].floats[row]
				}
				if math.IsNaN(value) {
					bounds[segment].Valid = false
					break
				}
				if !bounds[segment].Valid {
					bounds[segment] = ColumnarNumericSegment{Minimum: value, Maximum: value, Valid: true}
					continue
				}
				if value < bounds[segment].Minimum {
					bounds[segment].Minimum = value
				}
				if value > bounds[segment].Maximum {
					bounds[segment].Maximum = value
				}
			}
		}
		segments.Columns[field] = bounds
	}
	if len(segments.Columns) == 0 {
		return nil
	}
	return segments
}

// typedTableAdaptiveRowsPerSegment keeps a bounded number of smaller
// power-of-two segments for selective queries without exceeding the configured
// segment maximum. It is only used by the explicit opt-in cache option.
func typedTableAdaptiveRowsPerSegment(maximum, rows int) int {
	if maximum <= 0 || rows <= maximum {
		return maximum
	}
	const minimum = 32
	const targetSegments = 64
	rowsPerSegment := (rows + targetSegments - 1) / targetSegments
	if rowsPerSegment < minimum {
		rowsPerSegment = minimum
	}
	for power := minimum; power < rowsPerSegment && power < maximum; power <<= 1 {
		rowsPerSegment = power << 1
	}
	if rowsPerSegment > maximum {
		return maximum
	}
	return rowsPerSegment
}

func typedTableColumnarLayoutKey(fields []string) string {
	var builder strings.Builder
	for _, field := range fields {
		builder.WriteString(strconv.Itoa(len(field)))
		builder.WriteByte(':')
		builder.WriteString(field)
	}
	return builder.String()
}

func typedTableColumnarBatchBytes(batch ColumnarBatch, segments *ColumnarNumericSegments) int {
	bytes := len(batch.Columns)*64 + len(batch.Dictionaries)*64
	for _, values := range batch.Columns {
		bytes += len(values) * 16
	}
	for _, dictionary := range batch.Dictionaries {
		bytes += len(dictionary.Codes) * 4
		for _, value := range dictionary.Values {
			bytes += 16 + len(value)
		}
	}
	if segments != nil {
		bytes += len(segments.Columns) * 64
		for _, values := range segments.Columns {
			bytes += len(values) * 24
		}
	}
	return bytes
}

func (table *TypedTable) validateValues(values []TypedTableValue) error {
	if len(values) != len(table.columns) {
		return fmt.Errorf("typed table row has %d values, want %d", len(values), len(table.columns))
	}
	for index, value := range values {
		if !value.Valid {
			continue
		}
		if value.Kind != table.columns[index].kind {
			return fmt.Errorf("typed table column %q requires kind %d", table.schema.Columns[index].Name, table.columns[index].kind)
		}
	}
	return nil
}

func (table *TypedTable) rowLocked(index int) []TypedTableValue {
	values := make([]TypedTableValue, len(table.columns))
	for column := range table.columns {
		values[column] = table.columns[column].value(index)
	}
	return values
}

func (table *TypedTable) rowMapLocked(values []TypedTableValue) Row {
	row := make(Row, len(values))
	for index, value := range values {
		row[table.schema.Columns[index].Name] = typedTableValueInterface(value)
	}
	return row
}

func (table *TypedTable) appendChangeLocked(change TypedTableChange) TypedTableChange {
	table.sequence++
	change.Sequence = table.sequence
	table.changes = append(table.changes, change)
	return cloneTypedTableChange(change)
}

func cloneTypedTableValues(values []TypedTableValue) []TypedTableValue {
	return append([]TypedTableValue(nil), values...)
}

func cloneTypedTableChange(change TypedTableChange) TypedTableChange {
	change.Before = cloneTypedTableValues(change.Before)
	change.After = cloneTypedTableValues(change.After)
	return change
}

func typedTableValueInterface(value TypedTableValue) interface{} {
	if !value.Valid {
		return nil
	}
	switch value.Kind {
	case TypedTableString:
		return value.String
	case TypedTableInt64:
		return value.Int64
	case TypedTableFloat64:
		return value.Float64
	case TypedTableBool:
		return value.Bool
	default:
		return nil
	}
}

// TypedTableAggregateDefinition declares an exact changefeed aggregate. It
// always emits count; SumField, MinField, and MaxField are optional numeric
// schema columns. Min and max retain per-group counted values so deletes and
// updates remain exact.
type TypedTableAggregateDefinition struct {
	GroupBy  []string
	SumField string
	MinField string
	MaxField string
}

type typedTableAggregateGroup struct {
	values    []TypedTableValue
	count     int64
	sum       float64
	minValues map[TypedTableValue]int64
	maxValues map[TypedTableValue]int64
	minimum   TypedTableValue
	maximum   TypedTableValue
	hasMin    bool
	hasMax    bool
}

// TypedTableAggregate maintains exact grouped COUNT and optional SUM, MIN, and
// MAX results from ordered TypedTableChange records without rescanning the table.
type TypedTableAggregate struct {
	table      *TypedTable
	groupBy    []int
	sumField   int
	minField   int
	maxField   int
	groups     map[string]typedTableAggregateGroup
	checkpoint uint64
}

// NewTypedTableAggregate validates an exact delta aggregate for table.
func NewTypedTableAggregate(table *TypedTable, definition TypedTableAggregateDefinition) (*TypedTableAggregate, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table is nil")
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	aggregate := &TypedTableAggregate{table: table, sumField: -1, minField: -1, maxField: -1, groups: make(map[string]typedTableAggregateGroup)}
	seen := make(map[int]struct{}, len(definition.GroupBy))
	for _, field := range definition.GroupBy {
		index, exists := table.byName[strings.TrimSpace(field)]
		if !exists {
			return nil, fmt.Errorf("typed table aggregate group field %q does not exist", field)
		}
		if _, duplicate := seen[index]; duplicate {
			return nil, fmt.Errorf("typed table aggregate has duplicate group field %q", field)
		}
		seen[index] = struct{}{}
		aggregate.groupBy = append(aggregate.groupBy, index)
	}
	if definition.SumField != "" {
		index, exists := table.byName[strings.TrimSpace(definition.SumField)]
		if !exists || (table.columns[index].kind != TypedTableInt64 && table.columns[index].kind != TypedTableFloat64) {
			return nil, fmt.Errorf("typed table aggregate sum field %q must be numeric", definition.SumField)
		}
		aggregate.sumField = index
	}
	if definition.MinField != "" {
		index, exists := table.byName[strings.TrimSpace(definition.MinField)]
		if !exists || (table.columns[index].kind != TypedTableInt64 && table.columns[index].kind != TypedTableFloat64) {
			return nil, fmt.Errorf("typed table aggregate min field %q must be numeric", definition.MinField)
		}
		aggregate.minField = index
	}
	if definition.MaxField != "" {
		index, exists := table.byName[strings.TrimSpace(definition.MaxField)]
		if !exists || (table.columns[index].kind != TypedTableInt64 && table.columns[index].kind != TypedTableFloat64) {
			return nil, fmt.Errorf("typed table aggregate max field %q must be numeric", definition.MaxField)
		}
		aggregate.maxField = index
	}
	return aggregate, nil
}

// Apply advances an aggregate through a contiguous changefeed batch. Replayed
// changes are ignored; a gap is rejected to preserve exactness.
func (aggregate *TypedTableAggregate) Apply(changes []TypedTableChange) error {
	if aggregate == nil {
		return fmt.Errorf("typed table aggregate is nil")
	}
	for _, change := range changes {
		if change.Sequence <= aggregate.checkpoint {
			continue
		}
		if change.Sequence != aggregate.checkpoint+1 {
			return fmt.Errorf("typed table aggregate change sequence %d follows %d", change.Sequence, aggregate.checkpoint)
		}
		if len(change.Before) > 0 {
			if err := aggregate.applyRow(change.Before, -1); err != nil {
				return err
			}
		}
		if len(change.After) > 0 {
			if err := aggregate.applyRow(change.After, 1); err != nil {
				return err
			}
		}
		aggregate.checkpoint = change.Sequence
	}
	return nil
}

// Checkpoint returns the last exact change sequence applied by the aggregate.
func (aggregate *TypedTableAggregate) Checkpoint() uint64 {
	if aggregate == nil {
		return 0
	}
	return aggregate.checkpoint
}

// Rows returns a deterministic snapshot with group columns, count, and
// optional sum, min, and max. Rows with count zero are absent.
func (aggregate *TypedTableAggregate) Rows() []Row {
	if aggregate == nil {
		return nil
	}
	keys := make([]string, 0, len(aggregate.groups))
	for key := range aggregate.groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]Row, 0, len(keys))
	for _, key := range keys {
		group := aggregate.groups[key]
		rowFields := len(aggregate.groupBy) + 1
		if aggregate.sumField >= 0 {
			rowFields++
		}
		if aggregate.minField >= 0 {
			rowFields++
		}
		if aggregate.maxField >= 0 {
			rowFields++
		}
		row := make(Row, rowFields)
		for index, column := range aggregate.groupBy {
			row[aggregate.table.schema.Columns[column].Name] = typedTableValueInterface(group.values[index])
		}
		row["count"] = group.count
		if aggregate.sumField >= 0 {
			row["sum"] = group.sum
		}
		if group.hasMin {
			row["min"] = typedTableValueInterface(group.minimum)
		}
		if group.hasMax {
			row["max"] = typedTableValueInterface(group.maximum)
		}
		rows = append(rows, row)
	}
	return rows
}

func (aggregate *TypedTableAggregate) applyRow(values []TypedTableValue, delta int64) error {
	if len(values) != len(aggregate.table.columns) {
		return fmt.Errorf("typed table aggregate row has %d values, want %d", len(values), len(aggregate.table.columns))
	}
	key, groupValues := aggregate.groupKey(values)
	group := aggregate.groups[key]
	if delta > 0 && group.values == nil {
		group.values = groupValues
	}
	if err := aggregate.checkExtrema(group, values, delta); err != nil {
		return err
	}
	group.count += delta
	if group.count < 0 {
		return fmt.Errorf("typed table aggregate group count became negative")
	}
	if aggregate.sumField >= 0 && values[aggregate.sumField].Valid {
		switch values[aggregate.sumField].Kind {
		case TypedTableInt64:
			group.sum += float64(delta * values[aggregate.sumField].Int64)
		case TypedTableFloat64:
			group.sum += float64(delta) * values[aggregate.sumField].Float64
		}
	}
	aggregate.adjustExtrema(&group, values, delta)
	if group.count == 0 {
		delete(aggregate.groups, key)
		return nil
	}
	aggregate.groups[key] = group
	return nil
}

func (aggregate *TypedTableAggregate) checkExtrema(group typedTableAggregateGroup, values []TypedTableValue, delta int64) error {
	if delta >= 0 {
		return nil
	}
	if aggregate.minField >= 0 {
		if value, valid := typedTableAggregateExtremaValue(values[aggregate.minField]); valid && group.minValues[value] <= 0 {
			return fmt.Errorf("typed table aggregate min value is absent")
		}
	}
	if aggregate.maxField >= 0 && aggregate.maxField != aggregate.minField {
		if value, valid := typedTableAggregateExtremaValue(values[aggregate.maxField]); valid && group.maxValues[value] <= 0 {
			return fmt.Errorf("typed table aggregate max value is absent")
		}
	}
	return nil
}

func (aggregate *TypedTableAggregate) adjustExtrema(group *typedTableAggregateGroup, values []TypedTableValue, delta int64) {
	if aggregate.minField >= 0 {
		if value, valid := typedTableAggregateExtremaValue(values[aggregate.minField]); valid {
			group.minValues = typedTableAggregateAdjustValueCount(group.minValues, value, delta)
			group.minimum, group.hasMin = typedTableAggregateUpdatedExtreme(group.minValues, group.minimum, group.hasMin, value, delta, true)
		}
	}
	if aggregate.maxField >= 0 {
		if aggregate.maxField == aggregate.minField {
			group.maxValues = group.minValues
			if value, valid := typedTableAggregateExtremaValue(values[aggregate.maxField]); valid {
				group.maximum, group.hasMax = typedTableAggregateUpdatedExtreme(group.maxValues, group.maximum, group.hasMax, value, delta, false)
			}
			return
		}
		if value, valid := typedTableAggregateExtremaValue(values[aggregate.maxField]); valid {
			group.maxValues = typedTableAggregateAdjustValueCount(group.maxValues, value, delta)
			group.maximum, group.hasMax = typedTableAggregateUpdatedExtreme(group.maxValues, group.maximum, group.hasMax, value, delta, false)
		}
	}
}

func typedTableAggregateUpdatedExtreme(values map[TypedTableValue]int64, current TypedTableValue, found bool, changed TypedTableValue, delta int64, minimum bool) (TypedTableValue, bool) {
	if delta > 0 && (!found || typedTableAggregateValueLess(changed, current) == minimum && changed != current) {
		return changed, true
	}
	if delta < 0 && found && current == changed && values[changed] == 0 {
		return typedTableAggregateExtreme(values, minimum)
	}
	return current, found
}

func typedTableAggregateExtremaValue(value TypedTableValue) (TypedTableValue, bool) {
	if !value.Valid || value.Kind != TypedTableInt64 && value.Kind != TypedTableFloat64 || value.Kind == TypedTableFloat64 && math.IsNaN(value.Float64) {
		return TypedTableValue{}, false
	}
	return value, true
}

func typedTableAggregateAdjustValueCount(values map[TypedTableValue]int64, value TypedTableValue, delta int64) map[TypedTableValue]int64 {
	if values == nil {
		values = make(map[TypedTableValue]int64)
	}
	values[value] += delta
	if values[value] == 0 {
		delete(values, value)
	}
	return values
}

func typedTableAggregateExtreme(values map[TypedTableValue]int64, minimum bool) (TypedTableValue, bool) {
	var selected TypedTableValue
	found := false
	for value, count := range values {
		if count <= 0 || !found {
			selected, found = value, count > 0
			continue
		}
		less := typedTableAggregateValueLess(value, selected)
		if minimum && less || !minimum && !less && value != selected {
			selected = value
		}
	}
	return selected, found
}

func typedTableAggregateValueLess(left, right TypedTableValue) bool {
	if left.Kind == TypedTableFloat64 {
		return left.Float64 < right.Float64
	}
	return left.Int64 < right.Int64
}

func (aggregate *TypedTableAggregate) groupKey(values []TypedTableValue) (string, []TypedTableValue) {
	if len(aggregate.groupBy) == 0 {
		return "all", nil
	}
	var builder strings.Builder
	groupValues := make([]TypedTableValue, len(aggregate.groupBy))
	for index, column := range aggregate.groupBy {
		value := values[column]
		groupValues[index] = value
		builder.WriteByte(byte(value.Kind))
		if !value.Valid {
			builder.WriteByte('n')
			continue
		}
		switch value.Kind {
		case TypedTableString:
			builder.WriteString(strconv.Itoa(len(value.String)))
			builder.WriteByte(':')
			builder.WriteString(value.String)
		case TypedTableInt64:
			builder.WriteString(strconv.FormatInt(value.Int64, 10))
		case TypedTableFloat64:
			builder.WriteString(strconv.FormatUint(math.Float64bits(value.Float64), 16))
		case TypedTableBool:
			if value.Bool {
				builder.WriteByte('1')
			} else {
				builder.WriteByte('0')
			}
		}
		builder.WriteByte('|')
	}
	return builder.String(), groupValues
}
