package hatSql

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrTypedTableChangesCompacted reports that a requested changefeed sequence
// is older than the retained changefeed boundary.
var ErrTypedTableChangesCompacted = errors.New("typed table changes compacted")

// ErrTypedTableMemoryBudgetExceeded reports that a mutation would exceed the
// configured typed-table logical memory budget.
var ErrTypedTableMemoryBudgetExceeded = errors.New("typed table memory budget exceeded")

// ErrTypedTableMemoryBudgetInvalid reports an invalid typed-table budget.
var ErrTypedTableMemoryBudgetInvalid = errors.New("typed table memory budget is invalid")

// TypedTableKind identifies the fixed physical representation of one column.
type TypedTableKind uint8

const (
	TypedTableNull TypedTableKind = iota
	TypedTableString
	TypedTableInt64
	TypedTableFloat64
	TypedTableBool
)

// TypedTableGeneratedFunc derives a column from the candidate row. The input
// is schema ordered and must be treated as read-only. Generated callbacks
// should be deterministic and side-effect free so changefeeds and restores
// remain reproducible.
type TypedTableGeneratedFunc func([]TypedTableValue) (TypedTableValue, error)

// TypedTableGeneratedMode controls when a generated callback is evaluated.
// The zero value is materialized for compatibility with the original
// Generated field behavior.
type TypedTableGeneratedMode uint8

const (
	TypedTableGeneratedMaterialized TypedTableGeneratedMode = iota
	TypedTableGeneratedDefault
)

// TypedTableColumn declares one schema-checked table column.
type TypedTableColumn struct {
	Name string
	Kind TypedTableKind
	// TTL independently masks this column after expiry while retaining its row.
	TTL               TypedTableTTLOptions
	DictionaryEncoded bool
	// DictionaryAdaptive samples the first bounded batch of string rows and
	// promotes the column only when the observed cardinality is low enough to
	// justify dictionary storage. DictionaryEncoded takes precedence.
	DictionaryAdaptive bool
	Generated          TypedTableGeneratedFunc
	// GeneratedMode is meaningful only when Generated is set. Materialized
	// callbacks always replace the supplied value; default callbacks run only
	// when the supplied value is null.
	GeneratedMode TypedTableGeneratedMode
	// GeneratedDependencies names columns that must be available before this
	// callback runs. Dependencies may be ordinary or generated columns.
	GeneratedDependencies []string
}

const (
	typedTableDictionaryProbeRows                = 256
	typedTableDictionaryProbeDistinctDenominator = 8
	typedTableDictionaryProbeMaxDistinct         = typedTableDictionaryProbeRows / typedTableDictionaryProbeDistinctDenominator
)

const (
	typedTableColumnarCacheDefaultMaxBytes                  = 4 << 20
	typedTableColumnarCacheDefaultMinReads                  = 2
	typedTableColumnarCacheDefaultRowsPerSegment            = 256
	typedTableColumnarCacheDefaultSparseMarkMaxBytes        = 1 << 20
	typedTableColumnarCacheDefaultDecompressedBlockMaxBytes = 1 << 20
	typedTableColumnarCacheDefaultDecompressedBlockRows     = 256
	typedTableColumnarCacheDefaultDecompressedBlockMinReads = 2
)

// TypedTableColumnarCacheOptions configures the optional immutable SQL layout
// cache. It is disabled by default so existing typed tables keep their current
// memory behavior.
type TypedTableColumnarCacheOptions struct {
	Enabled bool
	// FieldOffsetCache is built only for cached layouts and is disabled by default.
	FieldOffsetCache          bool
	SortedOrderCache          bool
	CompressedBatches         bool
	DecompressedBlockCache    bool
	DecompressedBlockMaxBytes int
	DecompressedBlockRows     int
	DecompressedBlockMinReads int
	MaxBytes                  int
	MinReads                  int
	RowsPerSegment            int
	AdaptiveSegments          bool
	SparsePrimaryIndex        bool
	SparsePrimaryField        string
	SparsePrimaryFields       []string
	SparsePrimaryMarkCache    bool
	SparsePrimaryMarkMaxBytes int
}

// TypedTableSchema describes one compact table. Name is used as the SQL source
// key, and SourceName defaults to CACHE for compatibility with cache SQL.
type TypedTableSchema struct {
	Name          string
	SourceName    string
	Columns       []TypedTableColumn
	TTL           TypedTableTTLOptions
	MemoryBudget  TypedTableMemoryBudgetOptions
	ColumnarCache TypedTableColumnarCacheOptions
	PatchParts    TypedTablePatchOptions
	StorageEvents TypedTableStorageEventLogOptions
	MVCC          TypedTableMVCCOptions
}

// TypedTableMemoryBudgetOptions configures an optional logical resident-data
// budget. The estimate includes key bytes, scalar payload bytes, validity
// bytes, and a fixed row allowance; it deliberately excludes Go map capacity,
// index structures, query working memory, and allocator fragmentation. It is
// therefore an admission guard, not an exact process-heap limit.
// MaxBytes zero disables admission checks and preserves the legacy path.
type TypedTableMemoryBudgetOptions struct {
	MaxBytes int64
}

// TypedTableMemoryUsage reports the bounded logical bytes tracked by a table.
// Deleted rows remain charged until patch-part compaction physically removes
// them, matching the retained storage behavior.
type TypedTableMemoryUsage struct {
	MaxBytes       int64 `json:"max_bytes"`
	UsedBytes      int64 `json:"used_bytes"`
	AvailableBytes int64 `json:"available_bytes"`
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
	kind                TypedTableKind
	strings             []string
	dictionary          bool
	dictionaryAdaptive  bool
	adaptiveDictionary  *typedTableDictionaryProbe
	dictionaryValues    []string
	dictionaryCodes     []uint32
	dictionaryPositions map[string]uint32
	dictionaryCounts    []uint32
	dictionaryFree      []uint32
	int64s              []int64
	floats              []float64
	bools               []bool
	valid               []bool
}

type typedTableDictionaryProbe struct {
	rows   int
	values []string
}

type typedTableColumnarLayout struct {
	batch          ColumnarBatch
	segments       *ColumnarNumericSegments
	orders         map[string][]uint32
	bytes          int
	touched        uint64
	sourceSequence uint64
}

type typedTableColumnarCache struct {
	mu                 sync.Mutex
	options            TypedTableColumnarCacheOptions
	layouts            map[string]typedTableColumnarLayout
	observations       map[string]int
	orderObservations  map[typedTableColumnarOrderCacheKey]uint8
	sparsePrimaryMarks typedTableSparsePrimaryMarkCache
	bytes              int
	tick               uint64
}

func (storage *typedTableColumnStorage) append(value TypedTableValue) {
	storage.valid = append(storage.valid, value.Valid)
	switch storage.kind {
	case TypedTableString:
		if storage.dictionary {
			storage.dictionaryCodes = append(storage.dictionaryCodes, 0)
			if value.Valid {
				storage.dictionaryCodes[len(storage.dictionaryCodes)-1] = storage.retainDictionaryValue(value.String)
				storage.maybeDemoteAdaptiveDictionary()
			}
		} else {
			storage.strings = append(storage.strings, value.String)
			storage.observeAdaptiveDictionary(value)
		}
	case TypedTableInt64:
		storage.int64s = append(storage.int64s, value.Int64)
	case TypedTableFloat64:
		storage.floats = append(storage.floats, value.Float64)
	case TypedTableBool:
		storage.bools = append(storage.bools, value.Bool)
	}
}

func (storage *typedTableColumnStorage) set(index int, value TypedTableValue) {
	wasValid := storage.valid[index]
	storage.valid[index] = value.Valid
	switch storage.kind {
	case TypedTableString:
		if storage.dictionary {
			if wasValid {
				storage.releaseDictionaryValue(storage.dictionaryCodes[index])
			}
			if value.Valid {
				storage.dictionaryCodes[index] = storage.retainDictionaryValue(value.String)
				storage.maybeDemoteAdaptiveDictionary()
			} else {
				storage.dictionaryCodes[index] = 0
			}
		} else {
			storage.strings[index] = value.String
			storage.noteAdaptiveDictionaryValue(value)
		}
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
		if storage.dictionary {
			if value.Valid {
				value.String = storage.dictionaryValues[storage.dictionaryCodes[index]]
			}
		} else {
			value.String = storage.strings[index]
		}
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
	if index == from {
		return
	}
	wasValid, sourceValid := storage.valid[index], storage.valid[from]
	storage.valid[index] = storage.valid[from]
	switch storage.kind {
	case TypedTableString:
		if storage.dictionary {
			if wasValid {
				storage.releaseDictionaryValue(storage.dictionaryCodes[index])
			}
			if sourceValid {
				code := storage.dictionaryCodes[from]
				storage.dictionaryCounts[code]++
				storage.dictionaryCodes[index] = code
			} else {
				storage.dictionaryCodes[index] = 0
			}
		} else {
			storage.strings[index] = storage.strings[from]
		}
	case TypedTableInt64:
		storage.int64s[index] = storage.int64s[from]
	case TypedTableFloat64:
		storage.floats[index] = storage.floats[from]
	case TypedTableBool:
		storage.bools[index] = storage.bools[from]
	}
}

func (storage *typedTableColumnStorage) truncate(length int) {
	if storage.dictionary && storage.kind == TypedTableString {
		for index := length; index < len(storage.valid); index++ {
			if storage.valid[index] {
				storage.releaseDictionaryValue(storage.dictionaryCodes[index])
			}
		}
	}
	storage.valid = storage.valid[:length]
	switch storage.kind {
	case TypedTableString:
		if storage.dictionary {
			storage.dictionaryCodes = storage.dictionaryCodes[:length]
		} else {
			storage.strings = storage.strings[:length]
		}
	case TypedTableInt64:
		storage.int64s = storage.int64s[:length]
	case TypedTableFloat64:
		storage.floats = storage.floats[:length]
	case TypedTableBool:
		storage.bools = storage.bools[:length]
	}
}

func (storage *typedTableColumnStorage) retainDictionaryValue(value string) uint32 {
	if code, found := storage.dictionaryPositions[value]; found {
		storage.dictionaryCounts[code]++
		return code
	}
	var code uint32
	if length := len(storage.dictionaryFree); length > 0 {
		code = storage.dictionaryFree[length-1]
		storage.dictionaryFree = storage.dictionaryFree[:length-1]
		storage.dictionaryValues[code] = value
		storage.dictionaryCounts[code] = 1
	} else {
		code = uint32(len(storage.dictionaryValues))
		storage.dictionaryValues = append(storage.dictionaryValues, value)
		storage.dictionaryCounts = append(storage.dictionaryCounts, 1)
	}
	storage.dictionaryPositions[value] = code
	return code
}

func (storage *typedTableColumnStorage) releaseDictionaryValue(code uint32) {
	storage.dictionaryCounts[code]--
	if storage.dictionaryCounts[code] != 0 {
		return
	}
	value := storage.dictionaryValues[code]
	delete(storage.dictionaryPositions, value)
	storage.dictionaryValues[code] = ""
	if int(code) == len(storage.dictionaryValues)-1 {
		storage.dictionaryValues = storage.dictionaryValues[:code]
		storage.dictionaryCounts = storage.dictionaryCounts[:code]
		return
	}
	storage.dictionaryFree = append(storage.dictionaryFree, code)
}

func (storage *typedTableColumnStorage) maybeDemoteAdaptiveDictionary() {
	if storage == nil || !storage.dictionary || !storage.dictionaryAdaptive || len(storage.dictionaryPositions) <= typedTableDictionaryProbeMaxDistinct {
		return
	}
	storage.demoteAdaptiveDictionary()
}

func (storage *typedTableColumnStorage) demoteAdaptiveDictionary() {
	if storage == nil || !storage.dictionary || !storage.dictionaryAdaptive {
		return
	}
	values := make([]string, len(storage.dictionaryCodes))
	for index, code := range storage.dictionaryCodes {
		if storage.valid[index] {
			values[index] = storage.dictionaryValues[code]
		}
	}
	storage.dictionary = false
	storage.dictionaryAdaptive = false
	storage.strings = values
	storage.adaptiveDictionary = nil
	storage.dictionaryValues = nil
	storage.dictionaryCodes = nil
	storage.dictionaryPositions = nil
	storage.dictionaryCounts = nil
	storage.dictionaryFree = nil
}

func (storage *typedTableColumnStorage) observeAdaptiveDictionary(value TypedTableValue) {
	probe := storage.adaptiveDictionary
	if probe == nil {
		return
	}
	probe.rows++
	probe.observe(value)
	if len(probe.values) > typedTableDictionaryProbeMaxDistinct {
		storage.adaptiveDictionary = nil
		return
	}
	if probe.rows < typedTableDictionaryProbeRows {
		return
	}
	if len(probe.values)*typedTableDictionaryProbeDistinctDenominator > probe.rows {
		storage.adaptiveDictionary = nil
		return
	}
	storage.promoteAdaptiveDictionary()
}

func (storage *typedTableColumnStorage) noteAdaptiveDictionaryValue(value TypedTableValue) {
	if storage.adaptiveDictionary == nil || !value.Valid {
		return
	}
	storage.adaptiveDictionary.observe(value)
}

func (probe *typedTableDictionaryProbe) observe(value TypedTableValue) {
	if !value.Valid {
		return
	}
	for _, existing := range probe.values {
		if existing == value.String {
			return
		}
	}
	probe.values = append(probe.values, value.String)
}

func (storage *typedTableColumnStorage) promoteAdaptiveDictionary() {
	values := storage.strings
	positions := make(map[string]uint32, len(storage.adaptiveDictionary.values))
	dictionaryValues := make([]string, 0, len(storage.adaptiveDictionary.values))
	dictionaryCounts := make([]uint32, 0, len(storage.adaptiveDictionary.values))
	codes := make([]uint32, len(values))
	for index, value := range values {
		if !storage.valid[index] {
			continue
		}
		code, found := positions[value]
		if !found {
			code = uint32(len(dictionaryValues))
			positions[value] = code
			dictionaryValues = append(dictionaryValues, value)
			dictionaryCounts = append(dictionaryCounts, 0)
		}
		codes[index] = code
		dictionaryCounts[code]++
	}
	storage.dictionary = true
	storage.strings = nil
	storage.dictionaryValues = dictionaryValues
	storage.dictionaryCodes = codes
	storage.dictionaryPositions = positions
	storage.dictionaryCounts = dictionaryCounts
	storage.dictionaryFree = nil
	storage.adaptiveDictionary = nil
}

// TypedTable is a schema-checked row store with per-column primitive slices.
// It is opt-in and implements the established source-resolver contracts.
type TypedTable struct {
	mu                   sync.RWMutex
	schema               TypedTableSchema
	columns              []typedTableColumnStorage
	byName               map[string]int
	keys                 []string
	positions            map[string]int
	generated            bool
	generatedOrder       []int
	columnar             typedTableColumnarCache
	patchParts           *typedTablePatchState
	storageEvents        *typedTableStorageEventLog
	mvcc                 *typedTableMVCCState
	ttl                  *typedTableTTLState
	columnTTLs           []*typedTableColumnTTLState
	appendOnly           bool
	statsCache           TypedTableStats
	statsCacheValid      bool
	histogramCache       map[typedTableHistogramCacheKey]TypedTableHistogram
	memoryBudgetMaxBytes int64
	memoryBytes          int64
	memoryRowBytes       []int64

	changes          []TypedTableChange
	compactedThrough uint64
	sequence         uint64
	changeReadHolds  *typedTableChangeReadHoldSet
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
	if schema.MemoryBudget.MaxBytes < 0 {
		return nil, fmt.Errorf("%w: max bytes must be non-negative", ErrTypedTableMemoryBudgetInvalid)
	}
	if len(schema.Columns) == 0 {
		return nil, fmt.Errorf("typed table columns are required")
	}
	schema.ColumnarCache = normalizeTypedTableColumnarCacheOptions(schema.ColumnarCache)
	schema.PatchParts = normalizeTypedTablePatchOptions(schema.PatchParts)
	schema.StorageEvents = normalizeTypedTableStorageEventLogOptions(schema.StorageEvents)
	table := &TypedTable{
		schema:    schema,
		byName:    make(map[string]int, len(schema.Columns)),
		positions: make(map[string]int),
		columnar: typedTableColumnarCache{
			options: schema.ColumnarCache,
		},
		patchParts:           newTypedTablePatchState(schema.PatchParts),
		appendOnly:           true,
		memoryBudgetMaxBytes: schema.MemoryBudget.MaxBytes,
	}
	if table.memoryBudgetMaxBytes > 0 {
		table.memoryRowBytes = make([]int64, 0)
	}
	if schema.MVCC.Enabled {
		table.mvcc = newTypedTableMVCCState()
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
		if column.Generated == nil {
			if column.GeneratedMode != TypedTableGeneratedMaterialized {
				return nil, fmt.Errorf("typed table column %q has generated mode without a generated callback", column.Name)
			}
			if len(column.GeneratedDependencies) != 0 {
				return nil, fmt.Errorf("typed table column %q has generated dependencies without a generated callback", column.Name)
			}
		} else if column.GeneratedMode != TypedTableGeneratedMaterialized && column.GeneratedMode != TypedTableGeneratedDefault {
			return nil, fmt.Errorf("typed table column %q has invalid generated mode", column.Name)
		}
		table.byName[column.Name] = index
		table.columns[index].kind = column.Kind
		if column.Generated != nil {
			table.generated = true
		}
		table.columns[index].dictionary = column.Kind == TypedTableString && column.DictionaryEncoded
		if table.columns[index].dictionary {
			table.columns[index].dictionaryPositions = make(map[string]uint32)
		} else if column.Kind == TypedTableString && column.DictionaryAdaptive {
			table.columns[index].dictionaryAdaptive = true
			table.columns[index].adaptiveDictionary = &typedTableDictionaryProbe{}
		}
	}
	if table.generated {
		generatedOrder, err := typedTableGeneratedOrder(table.schema.Columns, table.byName)
		if err != nil {
			return nil, err
		}
		table.generatedOrder = generatedOrder
	}
	for index := range table.schema.Columns {
		columnTTL, err := newTypedTableColumnTTLState(index, table.schema.Columns[index].TTL, table.schema.Columns, table.byName)
		if err != nil {
			return nil, err
		}
		if columnTTL == nil {
			continue
		}
		if table.columnTTLs == nil {
			table.columnTTLs = make([]*typedTableColumnTTLState, len(table.schema.Columns))
		}
		table.columnTTLs[index] = columnTTL
		table.schema.Columns[index].TTL = columnTTL.options
	}
	table.storageEvents = newTypedTableStorageEventLog(schema.StorageEvents)
	ttl, err := newTypedTableTTLState(schema.TTL, table.schema.Columns, table.byName)
	if err != nil {
		return nil, err
	}
	table.ttl = ttl
	if table.ttl != nil {
		table.schema.TTL = table.ttl.options
	}
	return table, nil
}

const typedTableSparsePrimaryMaxFields = 8

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
	if !options.CompressedBatches || !options.DecompressedBlockCache {
		options.DecompressedBlockCache = false
		options.DecompressedBlockMaxBytes = 0
		options.DecompressedBlockRows = 0
		options.DecompressedBlockMinReads = 0
	} else {
		if options.DecompressedBlockMaxBytes <= 0 {
			options.DecompressedBlockMaxBytes = typedTableColumnarCacheDefaultDecompressedBlockMaxBytes
		}
		if options.DecompressedBlockRows <= 0 {
			options.DecompressedBlockRows = typedTableColumnarCacheDefaultDecompressedBlockRows
		}
		if options.DecompressedBlockMinReads <= 0 {
			options.DecompressedBlockMinReads = typedTableColumnarCacheDefaultDecompressedBlockMinReads
		}
	}
	if options.SparsePrimaryIndex {
		fields := options.SparsePrimaryFields
		if len(fields) == 0 && strings.TrimSpace(options.SparsePrimaryField) != "" {
			fields = []string{options.SparsePrimaryField}
		}
		if len(fields) > typedTableSparsePrimaryMaxFields {
			fields = nil
		} else if len(fields) == 1 {
			field := strings.TrimSpace(fields[0])
			if field == "" {
				fields = nil
			} else {
				fields = []string{field}
			}
		} else if len(fields) > 0 {
			seen := make(map[string]struct{}, len(fields))
			normalized := make([]string, len(fields))
			for index, field := range fields {
				field = strings.TrimSpace(field)
				if field == "" {
					fields = nil
					break
				}
				if _, duplicate := seen[field]; duplicate {
					fields = nil
					break
				}
				seen[field] = struct{}{}
				normalized[index] = field
			}
			if fields != nil {
				fields = normalized
			}
		}
		if len(fields) == 0 {
			options.SparsePrimaryIndex = false
			options.SparsePrimaryField = ""
			options.SparsePrimaryFields = nil
		} else {
			options.SparsePrimaryField = fields[0]
			if len(fields) > 1 {
				options.SparsePrimaryFields = fields
			} else {
				options.SparsePrimaryFields = nil
			}
		}
	} else {
		options.SparsePrimaryField = ""
		options.SparsePrimaryFields = nil
	}
	if !options.SparsePrimaryMarkCache || !options.SparsePrimaryIndex {
		options.SparsePrimaryMarkCache = false
		options.SparsePrimaryMarkMaxBytes = 0
	} else if options.SparsePrimaryMarkMaxBytes <= 0 {
		options.SparsePrimaryMarkMaxBytes = typedTableColumnarCacheDefaultSparseMarkMaxBytes
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
	values, err := table.applyGeneratedValues(values)
	if err != nil {
		return TypedTableChange{}, err
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if err := table.validateValues(values); err != nil {
		return TypedTableChange{}, err
	}
	index, exists := table.positions[key]
	var rowBytes int64
	if table.memoryBudgetMaxBytes > 0 {
		rowBytes = typedTableEstimatedRowBytes(key, values)
		var previousBytes int64
		if exists {
			previousBytes = table.memoryRowBytes[index]
		}
		if err := table.checkTypedTableMemoryBudgetLocked(previousBytes, rowBytes); err != nil {
			return TypedTableChange{}, err
		}
	}
	table.clearColumnarLayoutsLocked()
	table.invalidateTypedTableDerivedCachesLocked()
	var ttlNow time.Time
	if table.ttl != nil && table.ttl.options.Mode == TypedTableTTLProcessingTime {
		ttlNow = table.typedTableTTLNow()
	}
	newBasePart := !exists && len(table.keys) == 0
	change := TypedTableChange{Key: key, After: cloneTypedTableValues(values)}
	if exists && !table.typedTableRowDeletedLocked(index) {
		change.Operation = "UPDATE"
		table.appendOnly = false
		change.Before = table.rowLocked(index)
		for column := range table.columns {
			table.columns[column].set(index, values[column])
		}
		table.replaceTypedTableMemoryRowLocked(index, rowBytes)
		if table.ttl != nil {
			table.setTypedTableTTLDeadlineLocked(index, ttlNow)
		}
	} else if exists {
		change.Operation = "INSERT"
		if table.patchParts != nil {
			table.patchParts.deleted.clear(index)
			table.patchParts.deletedCount--
		}
		for column := range table.columns {
			table.columns[column].set(index, values[column])
		}
		table.replaceTypedTableMemoryRowLocked(index, rowBytes)
		if table.ttl != nil {
			table.setTypedTableTTLDeadlineLocked(index, ttlNow)
		}
	} else {
		change.Operation = "INSERT"
		index = len(table.keys)
		table.positions[key] = index
		table.keys = append(table.keys, key)
		if table.patchParts != nil {
			table.patchParts.deleted.ensure(index + 1)
		}
		for column := range table.columns {
			table.columns[column].append(values[column])
		}
		table.appendTypedTableMemoryRowLocked(rowBytes)
		if table.ttl != nil {
			table.setTypedTableTTLDeadlineLocked(index, ttlNow)
		}
	}
	table.setTypedTableColumnTTLDeadlineLocked(index)
	change = table.appendChangeLocked(change)
	if newBasePart {
		table.recordStorageEventLocked(TypedTableStorageEventBasePartCreated, 0, len(table.keys), 0, 0, 0)
	}
	return change, nil
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
	if !exists || table.typedTableRowDeletedLocked(index) {
		return TypedTableChange{}, fmt.Errorf("typed table key %q does not exist", key)
	}
	table.clearColumnarLayoutsLocked()
	table.invalidateTypedTableDerivedCachesLocked()
	table.appendOnly = false
	return table.deleteIndexLocked(index), nil
}

func (table *TypedTable) deleteIndexLocked(index int) TypedTableChange {
	change := TypedTableChange{Operation: "DELETE", Key: table.keys[index], Before: table.rowLocked(index)}
	if table.ttl != nil {
		table.ttl.expiryRemove(index)
	}
	if table.patchParts != nil {
		pendingDeletesBefore := table.patchParts.deletedCount
		table.patchParts.deleted.set(index)
		table.patchParts.deletedCount++
		change = table.appendChangeLocked(change)
		if pendingDeletesBefore == 0 {
			physicalRows := len(table.keys)
			table.recordStorageEventLocked(TypedTableStorageEventPatchPartCreated, physicalRows, physicalRows, table.patchParts.deletedCount, 1, 0)
		}
		table.scheduleTypedTablePatchCompactionLocked()
		return change
	}
	last := len(table.keys) - 1
	if index != last {
		moved := table.keys[last]
		table.keys[index] = moved
		table.positions[moved] = index
		for column := range table.columns {
			table.columns[column].copy(index, last)
		}
		if table.memoryBudgetMaxBytes > 0 {
			table.memoryRowBytes[index] = table.memoryRowBytes[last]
		}
		if table.ttl != nil && table.ttl.options.Mode == TypedTableTTLProcessingTime {
			table.ttl.deadlines[index] = table.ttl.deadlines[last]
		}
		if table.ttl != nil {
			table.ttl.expiryMove(last, index)
		}
		table.moveTypedTableColumnTTLDeadlineLocked(index, last)
	}
	delete(table.positions, change.Key)
	table.keys = table.keys[:last]
	for column := range table.columns {
		table.columns[column].truncate(last)
	}
	if table.memoryBudgetMaxBytes > 0 {
		table.memoryBytes -= table.memoryRowBytes[last]
		table.memoryRowBytes = table.memoryRowBytes[:last]
	}
	if table.ttl != nil && table.ttl.options.Mode == TypedTableTTLProcessingTime {
		table.ttl.deadlines = table.ttl.deadlines[:last]
	}
	table.truncateTypedTableColumnTTLDeadlinesLocked(last)
	if table.ttl != nil {
		table.ttl.expiryTruncate(last)
	}
	return table.appendChangeLocked(change)
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
	return table.changesAfterLocked(sequence, limit)
}

func (table *TypedTable) changesAfterLocked(sequence uint64, limit int) ([]TypedTableChange, uint64, error) {
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
		if table.changeReadHolds != nil && !table.changeReadHolds.CanCompactThrough(sequence) {
			return fmt.Errorf("%w: sequence %d", ErrTypedTableChangeReadHoldActive, sequence)
		}
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

// SQLSourceCardinality returns the current active row count without creating
// row maps. It is a planning hint and is read under the same table lock as
// Rows, so deleted and TTL-expired rows are excluded consistently.
func (table *TypedTable) SQLSourceCardinality(name string, key string) (int, bool, bool, error) {
	if table == nil || strings.ToUpper(strings.TrimSpace(name)) != table.schema.SourceName || key != table.schema.Name {
		return 0, false, false, nil
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	rows := len(table.keys)
	if table.patchParts != nil {
		rows -= table.patchParts.deletedCount
	}
	if table.ttl != nil {
		now := table.typedTableTTLNow()
		rows = 0
		for index := range table.keys {
			if !table.typedTableRowHiddenLocked(index, now) {
				rows++
			}
		}
	}
	if rows < 0 {
		rows = 0
	}
	return rows, true, true, nil
}

// ResolveSQLColumnarSource exposes only requested primitive columns so the
// existing SQL columnar path can avoid constructing source row maps.
func (table *TypedTable) ResolveSQLColumnarSource(name string, key string, fields []string) (ColumnarBatch, bool, error) {
	if table == nil || strings.ToUpper(strings.TrimSpace(name)) != table.schema.SourceName || key != table.schema.Name {
		return ColumnarBatch{}, false, nil
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if table.ttl != nil || table.columnTTLs != nil {
		return table.columnarBatchLocked(fields), true, nil
	}
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
	if table.ttl != nil || table.columnTTLs != nil {
		return ColumnarBatch{}, false, nil
	}
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
	if table.ttl != nil || table.columnTTLs != nil {
		return ColumnarBatch{}, nil, false, nil
	}
	layout, found := table.lookupColumnarLayoutWithSegmentsLocked(typedTableColumnarLayoutKey(fields))
	if !found {
		segments, found := table.columnar.lookupSparsePrimaryMarkLocked(typedTableColumnarLayoutKey(fields))
		if !found {
			return ColumnarBatch{}, nil, false, nil
		}
		return table.columnarBatchLocked(fields), segments, true, nil
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
	if table.ttl != nil || table.columnTTLs != nil {
		return false
	}
	_, found := table.lookupColumnarLayoutLocked(typedTableColumnarLayoutKey(fields))
	return found
}

// SQLSourceVersion identifies the current cached-table snapshot for safe
// condition-cache reuse. Disabled layout caches retain prior resolver behavior.
const typedTableMemoryRowOverhead int64 = 32
const typedTableMemoryMaxInt64 = int64(^uint64(0) >> 1)

func typedTableEstimatedRowBytes(key string, values []TypedTableValue) int64 {
	total := typedTableMemoryRowOverhead + int64(len(key))
	for _, value := range values {
		total = typedTableAddMemoryBytes(total, 1)
		if !value.Valid {
			continue
		}
		scalarBytes := int64(0)
		switch value.Kind {
		case TypedTableString:
			scalarBytes = int64(len(value.String))
		case TypedTableInt64, TypedTableFloat64:
			scalarBytes = 8
		case TypedTableBool:
			scalarBytes = 1
		}
		total = typedTableAddMemoryBytes(total, scalarBytes)
	}
	return total
}

func typedTableAddMemoryBytes(total, additional int64) int64 {
	if additional < 0 || total > typedTableMemoryMaxInt64-additional {
		return typedTableMemoryMaxInt64
	}
	return total + additional
}

func (table *TypedTable) checkTypedTableMemoryBudgetLocked(previousBytes, nextBytes int64) error {
	if table.memoryBudgetMaxBytes <= 0 || nextBytes <= previousBytes {
		return nil
	}
	increase := nextBytes - previousBytes
	if table.memoryBytes > table.memoryBudgetMaxBytes || increase > table.memoryBudgetMaxBytes-table.memoryBytes {
		return fmt.Errorf("%w: maximum %d bytes, current %d bytes, requested %d bytes", ErrTypedTableMemoryBudgetExceeded, table.memoryBudgetMaxBytes, table.memoryBytes, nextBytes)
	}
	return nil
}

func (table *TypedTable) replaceTypedTableMemoryRowLocked(index int, rowBytes int64) {
	if table.memoryBudgetMaxBytes <= 0 {
		return
	}
	previous := table.memoryRowBytes[index]
	table.memoryRowBytes[index] = rowBytes
	table.memoryBytes += rowBytes - previous
}

func (table *TypedTable) appendTypedTableMemoryRowLocked(rowBytes int64) {
	if table.memoryBudgetMaxBytes <= 0 {
		return
	}
	table.memoryRowBytes = append(table.memoryRowBytes, rowBytes)
	table.memoryBytes += rowBytes
}

func (table *TypedTable) SQLSourceVersion(name string, key string) (string, bool, error) {
	if table == nil || strings.ToUpper(strings.TrimSpace(name)) != table.schema.SourceName || key != table.schema.Name {
		return "", false, nil
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if table.ttl != nil || table.columnTTLs != nil {
		return "", false, nil
	}
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
	activeRows := len(table.keys)
	if table.patchParts != nil {
		activeRows -= table.patchParts.deletedCount
	}
	if table.ttl == nil && table.columnTTLs == nil {
		rows := make([]Row, 0, activeRows)
		if table.patchParts != nil && table.patchParts.deletedCount >= typedTableDeleteBitmapWordBits {
			physicalRows := len(table.keys)
			wordCount := (physicalRows + typedTableDeleteBitmapWordBits - 1) / typedTableDeleteBitmapWordBits
			for wordIndex := 0; wordIndex < wordCount; wordIndex++ {
				live := table.patchParts.deleted.liveWord(wordIndex, physicalRows)
				for live != 0 {
					row := wordIndex*typedTableDeleteBitmapWordBits + bits.TrailingZeros64(live)
					values := table.rowLocked(row)
					rows = append(rows, table.rowMapLocked(values))
					live &= live - 1
				}
			}
			return rows
		}
		for row := range table.keys {
			if table.typedTableRowDeletedLocked(row) {
				continue
			}
			values := table.rowLocked(row)
			rows = append(rows, table.rowMapLocked(values))
		}
		return rows
	}
	now := table.typedTableTTLNow()
	activeRows = 0
	for row := range table.keys {
		if !table.typedTableRowHiddenLocked(row, now) {
			activeRows++
		}
	}
	rows := make([]Row, 0, activeRows)
	for row := range table.keys {
		if table.typedTableRowHiddenLocked(row, now) {
			continue
		}
		values := table.rowLocked(row)
		if table.columnTTLs != nil {
			table.maskTypedTableExpiredColumnsLocked(row, values)
		}
		rows = append(rows, table.rowMapLocked(values))
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

// MemoryUsage returns the current logical budget usage. It is intentionally a
// conservative admission metric rather than a process heap measurement.
func (table *TypedTable) MemoryUsage() TypedTableMemoryUsage {
	if table == nil {
		return TypedTableMemoryUsage{}
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	usage := TypedTableMemoryUsage{
		MaxBytes:  table.memoryBudgetMaxBytes,
		UsedBytes: table.memoryBytes,
	}
	if usage.MaxBytes > usage.UsedBytes {
		usage.AvailableBytes = usage.MaxBytes - usage.UsedBytes
	}
	return usage
}

func (table *TypedTable) columnarBatchLocked(fields []string) ColumnarBatch {
	activeRows := len(table.keys)
	if table.patchParts != nil {
		activeRows -= table.patchParts.deletedCount
	}
	if table.ttl == nil && table.columnTTLs == nil {
		batch := ColumnarBatch{Columns: make(map[string][]interface{}, len(fields)), Rows: activeRows}
		for _, field := range fields {
			column, found := table.byName[field]
			if !found {
				continue
			}
			values := make([]interface{}, 0, activeRows)
			for row := range table.keys {
				if table.typedTableRowDeletedLocked(row) {
					continue
				}
				values = append(values, typedTableValueInterface(table.columns[column].value(row)))
			}
			batch.Columns[field] = values
		}
		batch.EncodeRepeatedStrings()
		if table.columnar.options.CompressedBatches {
			batch.PackCompressedColumns()
			if table.columnar.options.DecompressedBlockCache && batch.hasDecompressedBlockColumns() {
				batch.decompressedBlockCache = newColumnarDecompressedBlockCache(batch,
					table.columnar.options.DecompressedBlockMaxBytes,
					table.columnar.options.DecompressedBlockRows,
					table.columnar.options.DecompressedBlockMinReads,
				)
			}
		}
		if table.columnar.options.FieldOffsetCache {
			batch.PrepareFieldOffsets()
		}
		return batch
	}
	now := table.typedTableTTLNow()
	activeRows = 0
	for row := range table.keys {
		if !table.typedTableRowHiddenLocked(row, now) {
			activeRows++
		}
	}
	batch := ColumnarBatch{Columns: make(map[string][]interface{}, len(fields)), Rows: activeRows}
	for _, field := range fields {
		column, found := table.byName[field]
		if !found {
			continue
		}
		values := make([]interface{}, 0, activeRows)
		for row := range table.keys {
			if table.typedTableRowHiddenLocked(row, now) {
				continue
			}
			value := table.columns[column].value(row)
			if table.columnTTLs != nil && table.typedTableColumnExpiredLocked(column, row) {
				value = TypedNull()
			}
			values = append(values, typedTableValueInterface(value))
		}
		batch.Columns[field] = values
	}
	batch.EncodeRepeatedStrings()
	if table.columnar.options.CompressedBatches {
		batch.PackCompressedColumns()
		if table.columnar.options.DecompressedBlockCache && batch.hasDecompressedBlockColumns() {
			batch.decompressedBlockCache = newColumnarDecompressedBlockCache(batch,
				table.columnar.options.DecompressedBlockMaxBytes,
				table.columnar.options.DecompressedBlockRows,
				table.columnar.options.DecompressedBlockMinReads,
			)
		}
	}
	if table.columnar.options.FieldOffsetCache {
		batch.PrepareFieldOffsets()
	}
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
	cache.touchSparsePrimaryMarkLocked(key)
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
	cache.mu.Lock()
	cache.observeSparsePrimaryMarkLocked(key, segments)
	cache.mu.Unlock()
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
	cache.layouts[key] = typedTableColumnarLayout{batch: batch, segments: segments, bytes: bytes, touched: cache.tick, sourceSequence: table.sequence}
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
	for candidate := range cache.orderObservations {
		if candidate.layout == oldestKey {
			delete(cache.orderObservations, candidate)
		}
	}
}

func (table *TypedTable) clearColumnarLayoutsLocked() {
	cache := &table.columnar
	if !cache.options.Enabled {
		return
	}
	cache.mu.Lock()
	cache.layouts = nil
	cache.observations = nil
	cache.orderObservations = nil
	cache.bytes = 0
	cache.tick = 0
	cache.clearSparsePrimaryMarksLocked()
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
	for _, schemaColumn := range table.schema.Columns {
		field := schemaColumn.Name
		column, found := table.byName[field]
		if !found || table.columns[column].kind != TypedTableInt64 && table.columns[column].kind != TypedTableFloat64 || !columnarBatchHasField(batch, field) {
			continue
		}
		primaryField := table.columnar.options.SparsePrimaryIndex && table.columnar.options.SparsePrimaryField == field
		primaryOrdered := primaryField
		primaryHasValue := false
		var previousPrimaryValue float64
		bounds := make([]ColumnarNumericSegment, (batch.Rows+rowsPerSegment-1)/rowsPerSegment)
		for segment := range bounds {
			start := segment * rowsPerSegment
			end := start + rowsPerSegment
			if end > batch.Rows {
				end = batch.Rows
			}
			for row := start; row < end; row++ {
				value, numeric := columnarBatchNumericValue(batch, field, row)
				if !numeric {
					primaryOrdered = false
					continue
				}
				if primaryField && primaryHasValue && value < previousPrimaryValue {
					primaryOrdered = false
				}
				if primaryField {
					previousPrimaryValue = value
					primaryHasValue = true
				}
				if math.IsNaN(value) {
					bounds[segment].Valid = false
					primaryOrdered = false
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
		if primaryField && primaryOrdered && primaryHasValue {
			segments.SparsePrimaryField = field
		}
	}
	table.columnarCompositeSparsePrimarySegmentsLocked(batch, segments)
	for field, dictionary := range batch.Dictionaries {
		valueCount := dictionary.ValueCount()
		if !dictionary.codesTrusted || valueCount == 0 || valueCount > 64 || dictionary.RowCount() != batch.Rows {
			continue
		}
		sets, valid := columnarDictionaryCodeSets(dictionary, valueCount, batch.Rows, rowsPerSegment)
		if valid {
			if segments.DictionaryCodeSets == nil {
				segments.DictionaryCodeSets = make(map[string][]uint64)
			}
			segments.DictionaryCodeSets[field] = sets
		}
	}
	if len(segments.Columns) == 0 && len(segments.DictionaryCodeSets) == 0 {
		return nil
	}
	return segments
}

func (table *TypedTable) columnarCompositeSparsePrimarySegmentsLocked(batch ColumnarBatch, segments *ColumnarNumericSegments) {
	fields := table.columnar.options.SparsePrimaryFields
	fieldCount := len(fields)
	if segments == nil || fieldCount < 2 || fieldCount > typedTableSparsePrimaryMaxFields {
		return
	}
	for _, field := range fields {
		column, found := table.byName[field]
		if !found || (table.columns[column].kind != TypedTableInt64 && table.columns[column].kind != TypedTableFloat64) || !columnarBatchHasField(batch, field) {
			return
		}
	}
	segmentCount := len(segments.Columns[fields[0]])
	if segmentCount == 0 {
		return
	}
	minimum := make([]float64, segmentCount*fieldCount)
	maximum := make([]float64, segmentCount*fieldCount)
	var tuple [typedTableSparsePrimaryMaxFields]float64
	var previous [typedTableSparsePrimaryMaxFields]float64
	previousValid := false
	for row := 0; row < batch.Rows; row++ {
		for fieldIndex, field := range fields {
			value, numeric := columnarBatchNumericValue(batch, field, row)
			if !numeric || math.IsNaN(value) {
				return
			}
			tuple[fieldIndex] = value
		}
		if previousValid && sqlColumnarCompareNumericTuple(tuple[:fieldCount], previous[:fieldCount]) < 0 {
			return
		}
		segment := row / segments.RowsPerSegment
		segmentOffset := segment * fieldCount
		if row%segments.RowsPerSegment == 0 {
			copy(minimum[segmentOffset:segmentOffset+fieldCount], tuple[:fieldCount])
		}
		if row%segments.RowsPerSegment == segments.RowsPerSegment-1 || row == batch.Rows-1 {
			copy(maximum[segmentOffset:segmentOffset+fieldCount], tuple[:fieldCount])
		}
		copy(previous[:fieldCount], tuple[:fieldCount])
		previousValid = true
	}
	if !previousValid {
		return
	}
	segments.SparsePrimaryFields = append([]string(nil), fields...)
	segments.SparsePrimaryTupleMinimum = minimum
	segments.SparsePrimaryTupleMaximum = maximum
}

func sqlColumnarCompareNumericTuple(left, right []float64) int {
	for index := range left {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}

func columnarDictionaryCodeSets(dictionary DictionaryColumn, valueCount, rows, rowsPerSegment int) ([]uint64, bool) {
	sets := make([]uint64, (rows+rowsPerSegment-1)/rowsPerSegment)
	if dictionary.Codes != nil {
		if len(dictionary.Codes) != rows {
			return nil, false
		}
		for row, code := range dictionary.Codes {
			if int(code) >= valueCount || code >= 64 {
				return nil, false
			}
			sets[row/rowsPerSegment] |= uint64(1) << code
		}
		return sets, true
	}
	for row := 0; row < rows; row++ {
		code, ok := dictionary.CodeAt(row)
		if !ok || int(code) >= valueCount || code >= 64 {
			return nil, false
		}
		sets[row/rowsPerSegment] |= uint64(1) << code
	}
	return sets, true
}

func columnarBatchHasField(batch ColumnarBatch, field string) bool {
	if values, ok := batch.Columns[field]; ok {
		return len(values) == batch.Rows
	}
	if column, ok := batch.NumericColumns[field]; ok {
		return column.RowCount() == batch.Rows
	}
	if column, ok := batch.PackedColumns[field]; ok {
		return column.RowCount() == batch.Rows
	}
	return false
}

func columnarBatchNumericValue(batch ColumnarBatch, field string, row int) (float64, bool) {
	if values, ok := batch.Columns[field]; ok {
		if row < 0 || row >= len(values) {
			return 0, false
		}
		return sqlNumber(values[row])
	}
	value, ok := batch.Value(field, row)
	if !ok {
		return 0, false
	}
	return sqlNumber(value)
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
		if dictionary.Codes != nil {
			bytes += len(dictionary.Codes) * 4
		} else {
			bytes += len(dictionary.PackedCodes)
		}
		if dictionary.Values != nil {
			for _, value := range dictionary.Values {
				bytes += 16 + len(value)
			}
		} else {
			bytes += 16 + len(dictionary.PackedValueData) + len(dictionary.ValueOffsets)*4
		}
	}
	bytes += len(batch.PackedColumns) * 64
	for _, column := range batch.PackedColumns {
		bytes += len(column.Values)*16 + len(column.Validity) + len(column.Ranks)*4
	}
	bytes += len(batch.BoolColumns) * 64
	for _, column := range batch.BoolColumns {
		bytes += len(column.Bits) + len(column.Validity)
	}
	bytes += len(batch.NumericColumns) * 64
	for _, column := range batch.NumericColumns {
		bytes += len(column.Data) + len(column.Validity)
	}
	if segments != nil {
		bytes += len(segments.Columns) * 64
		for _, values := range segments.Columns {
			bytes += len(values) * 24
		}
		bytes += len(segments.DictionaryCodeSets) * 64
		for _, values := range segments.DictionaryCodeSets {
			bytes += len(values) * 8
		}
	}
	return bytes
}

func typedTableGeneratedOrder(columns []TypedTableColumn, byName map[string]int) ([]int, error) {
	state := make([]uint8, len(columns))
	order := make([]int, 0, len(columns))
	var visit func(int) error
	visit = func(index int) error {
		switch state[index] {
		case 1:
			return fmt.Errorf("typed table generated column dependency cycle includes %q", columns[index].Name)
		case 2:
			return nil
		}
		state[index] = 1
		column := &columns[index]
		seen := make(map[string]struct{}, len(column.GeneratedDependencies))
		for dependencyIndex, dependencyName := range column.GeneratedDependencies {
			dependencyName = strings.TrimSpace(dependencyName)
			if dependencyName == "" {
				return fmt.Errorf("typed table generated column %q has an empty dependency at index %d", column.Name, dependencyIndex)
			}
			if _, duplicate := seen[dependencyName]; duplicate {
				return fmt.Errorf("typed table generated column %q has duplicate dependency %q", column.Name, dependencyName)
			}
			seen[dependencyName] = struct{}{}
			column.GeneratedDependencies[dependencyIndex] = dependencyName
			dependency, exists := byName[dependencyName]
			if !exists {
				return fmt.Errorf("typed table generated column %q depends on unknown column %q", column.Name, dependencyName)
			}
			if columns[dependency].Generated != nil {
				if err := visit(dependency); err != nil {
					return err
				}
			}
		}
		state[index] = 2
		order = append(order, index)
		return nil
	}
	for index := range columns {
		if columns[index].Generated == nil {
			continue
		}
		if err := visit(index); err != nil {
			return nil, err
		}
	}
	return order, nil
}

func (table *TypedTable) validateValues(values []TypedTableValue) error {
	if len(values) != len(table.columns) {
		return fmt.Errorf("typed table row has %d values, want %d", len(values), len(table.columns))
	}
	for index, value := range values {
		column := table.schema.Columns[index]
		if column.Generated != nil && (column.GeneratedMode == TypedTableGeneratedMaterialized || !value.Valid) {
			continue
		}
		if !value.Valid {
			continue
		}
		if value.Kind != table.columns[index].kind {
			return fmt.Errorf("typed table column %q requires kind %d", table.schema.Columns[index].Name, table.columns[index].kind)
		}
	}
	return nil
}

func (table *TypedTable) applyGeneratedValues(values []TypedTableValue) ([]TypedTableValue, error) {
	if !table.generated {
		return values, nil
	}
	if len(values) != len(table.schema.Columns) {
		return nil, fmt.Errorf("typed table row has %d values, want %d", len(values), len(table.schema.Columns))
	}
	computed := values
	cloned := false
	for _, index := range table.generatedOrder {
		column := table.schema.Columns[index]
		if column.Generated == nil {
			continue
		}
		if column.GeneratedMode == TypedTableGeneratedDefault && computed[index].Valid {
			continue
		}
		if !cloned {
			computed = cloneTypedTableValues(values)
			cloned = true
		}
		value, err := column.Generated(cloneTypedTableValues(computed))
		if err != nil {
			return nil, fmt.Errorf("typed table generated column %q: %w", column.Name, err)
		}
		if value.Valid && value.Kind != column.Kind {
			return nil, fmt.Errorf("typed table generated column %q requires kind %d", column.Name, column.Kind)
		}
		computed[index] = value
	}
	return computed, nil
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
	if table.mvcc != nil {
		table.mvcc.record(change)
	}
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
// schema columns. DistinctField is an optional scalar schema column. Min, max,
// and distinct values retain per-group counts so deletes and updates remain
// exact. DictionaryEncodeGroups is disabled by default; when enabled, string
// group columns use per-aggregate dictionary codes to reduce retained key
// memory and cache repeated output ordering.
type TypedTableAggregateDefinition struct {
	GroupBy                []string
	SumField               string
	MinField               string
	MaxField               string
	DistinctField          string
	DictionaryEncodeGroups bool
}

type typedTableDistinctValue struct {
	kind        TypedTableKind
	stringValue string
	int64Value  int64
	floatBits   uint64
	boolValue   bool
}

type typedTableAggregateGroup struct {
	key            string
	values         []TypedTableValue
	codes          []uint32
	kinds          []TypedTableKind
	count          int64
	sum            float64
	minValues      map[TypedTableValue]int64
	maxValues      map[TypedTableValue]int64
	distinctValues map[typedTableDistinctValue]int64
	minimum        TypedTableValue
	maximum        TypedTableValue
	hasMin         bool
	hasMax         bool
}

type typedTableAggregateGroupBucket struct {
	group      typedTableAggregateGroup
	collisions []typedTableAggregateGroup
}

type typedTableAggregateGroupReference struct {
	hash      uint64
	collision int
}

// TypedTableAggregate maintains exact grouped COUNT and optional SUM, MIN, MAX,
// and COUNT DISTINCT results from ordered TypedTableChange records without
// rescanning the table.
type TypedTableAggregate struct {
	table                  *TypedTable
	groupBy                []int
	groupValueIndexes      []int
	groupCodeIndexes       []int
	groupValueCount        int
	groupDictionaries      []typedTableAggregateStringDictionary
	dictionaryEncodeGroups bool
	sumField               int
	minField               int
	maxField               int
	distinctField          int
	groups                 map[uint64]typedTableAggregateGroupBucket
	compactGroupOrder      []typedTableAggregateGroupReference
	pendingGroupOrder      []typedTableAggregateGroupReference
	groupCount             int
	checkpoint             uint64
	compactionCount        uint64
	groupKeysReady         bool
}

// NewTypedTableAggregate validates an exact delta aggregate for table.
func NewTypedTableAggregate(table *TypedTable, definition TypedTableAggregateDefinition) (*TypedTableAggregate, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table is nil")
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	aggregate := &TypedTableAggregate{
		table:                  table,
		sumField:               -1,
		minField:               -1,
		maxField:               -1,
		distinctField:          -1,
		dictionaryEncodeGroups: definition.DictionaryEncodeGroups,
		groups:                 make(map[uint64]typedTableAggregateGroupBucket),
	}
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
		if aggregate.dictionaryEncodeGroups && table.columns[index].kind == TypedTableString {
			aggregate.groupValueIndexes = append(aggregate.groupValueIndexes, -1)
			aggregate.groupCodeIndexes = append(aggregate.groupCodeIndexes, len(aggregate.groupDictionaries))
			aggregate.groupDictionaries = append(aggregate.groupDictionaries, typedTableAggregateStringDictionary{})
		} else {
			aggregate.groupValueIndexes = append(aggregate.groupValueIndexes, aggregate.groupValueCount)
			aggregate.groupCodeIndexes = append(aggregate.groupCodeIndexes, -1)
			aggregate.groupValueCount++
		}
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
	if definition.DistinctField != "" {
		index, exists := table.byName[strings.TrimSpace(definition.DistinctField)]
		if !exists {
			return nil, fmt.Errorf("typed table aggregate distinct field %q does not exist", definition.DistinctField)
		}
		aggregate.distinctField = index
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

func (aggregate *TypedTableAggregate) ensureGroupKeys() {
	if aggregate == nil {
		return
	}
	if aggregate.groupKeysReady {
		return
	}
	for hash, bucket := range aggregate.groups {
		if bucket.group.key == "" {
			bucket.group.key = typedTableAggregateGroupValuesKey(bucket.group.values)
		}
		for index := range bucket.collisions {
			if bucket.collisions[index].key == "" {
				bucket.collisions[index].key = typedTableAggregateGroupValuesKey(bucket.collisions[index].values)
			}
		}
		aggregate.groups[hash] = bucket
	}
	aggregate.groupKeysReady = true
}

// Rows returns a deterministic snapshot with group columns, count, and
// optional sum, min, max, and count_distinct. Rows with count zero are absent.
func (aggregate *TypedTableAggregate) Rows() []Row {
	if aggregate == nil {
		return nil
	}
	groups := aggregate.compactOrderedGroups()
	rows := make([]Row, 0, len(groups))
	for _, group := range groups {
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
		if aggregate.distinctField >= 0 {
			rowFields++
		}
		row := make(Row, rowFields)
		for index, column := range aggregate.groupBy {
			value := TypedTableValue{}
			if aggregate.dictionaryEncodeGroups {
				value = aggregate.groupValue(group, index)
			} else {
				value = group.values[index]
			}
			row[aggregate.table.schema.Columns[column].Name] = typedTableValueInterface(value)
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
		if aggregate.distinctField >= 0 {
			row["count_distinct"] = int64(len(group.distinctValues))
		}
		rows = append(rows, row)
	}
	return rows
}

func (aggregate *TypedTableAggregate) applyRow(values []TypedTableValue, delta int64) error {
	if len(values) != len(aggregate.table.columns) {
		return fmt.Errorf("typed table aggregate row has %d values, want %d", len(values), len(aggregate.table.columns))
	}
	hash := typedTableAggregateGroupHash(values, aggregate.groupBy)
	bucket, bucketExists := aggregate.groups[hash]
	groupIndex := -1
	var group typedTableAggregateGroup
	if bucketExists {
		if aggregate.dictionaryEncodeGroups && aggregate.groupValuesEqual(bucket.group, values) || !aggregate.dictionaryEncodeGroups && typedTableAggregateGroupValuesEqual(bucket.group.values, values, aggregate.groupBy) {
			group = bucket.group
			groupIndex = 0
		} else {
			for index := range bucket.collisions {
				if aggregate.dictionaryEncodeGroups && aggregate.groupValuesEqual(bucket.collisions[index], values) || !aggregate.dictionaryEncodeGroups && typedTableAggregateGroupValuesEqual(bucket.collisions[index].values, values, aggregate.groupBy) {
					group = bucket.collisions[index]
					groupIndex = index + 1
					break
				}
			}
		}
	}
	if delta > 0 && groupIndex < 0 {
		if aggregate.dictionaryEncodeGroups {
			group = aggregate.newGroup(values)
		} else {
			group.values = make([]TypedTableValue, len(aggregate.groupBy))
			for index, column := range aggregate.groupBy {
				group.values[index] = values[column]
			}
		}
	}
	if err := aggregate.checkDistinct(group, values, delta); err != nil {
		return err
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
	aggregate.adjustDistinct(&group, values, delta)
	if group.count == 0 {
		if groupIndex >= 0 {
			if aggregate.dictionaryEncodeGroups {
				aggregate.releaseGroup(group)
			}
			aggregate.deleteGroup(hash, bucket, groupIndex)
		}
		return nil
	}
	if groupIndex < 0 {
		aggregate.groupCount++
		var reference typedTableAggregateGroupReference
		if !bucketExists {
			aggregate.groups[hash] = typedTableAggregateGroupBucket{group: group}
		} else {
			reference.collision = len(bucket.collisions) + 1
			bucket.collisions = append(bucket.collisions, group)
			aggregate.groups[hash] = bucket
		}
		reference.hash = hash
		aggregate.noteGroupAdded(reference)
	} else if groupIndex == 0 {
		bucket.group = group
		aggregate.groups[hash] = bucket
	} else {
		bucket.collisions[groupIndex-1] = group
		aggregate.groups[hash] = bucket
	}
	return nil
}

func (aggregate *TypedTableAggregate) deleteGroup(hash uint64, bucket typedTableAggregateGroupBucket, groupIndex int) {
	aggregate.groupKeysReady = false
	aggregate.compactGroupOrder = nil
	aggregate.pendingGroupOrder = nil
	aggregate.groupCount--
	if groupIndex == 0 {
		if len(bucket.collisions) == 0 {
			delete(aggregate.groups, hash)
			return
		}
		bucket.group = bucket.collisions[0]
		bucket.collisions = bucket.collisions[1:]
		aggregate.groups[hash] = bucket
		return
	}
	collisionIndex := groupIndex - 1
	copy(bucket.collisions[collisionIndex:], bucket.collisions[collisionIndex+1:])
	bucket.collisions = bucket.collisions[:len(bucket.collisions)-1]
	aggregate.groups[hash] = bucket
}

func (aggregate *TypedTableAggregate) checkDistinct(group typedTableAggregateGroup, values []TypedTableValue, delta int64) error {
	if delta >= 0 || aggregate.distinctField < 0 {
		return nil
	}
	value, valid := typedTableAggregateDistinctValue(values[aggregate.distinctField])
	if valid && group.distinctValues[value] <= 0 {
		return fmt.Errorf("typed table aggregate distinct value is absent")
	}
	return nil
}

func (aggregate *TypedTableAggregate) adjustDistinct(group *typedTableAggregateGroup, values []TypedTableValue, delta int64) {
	if aggregate.distinctField < 0 {
		return
	}
	value, valid := typedTableAggregateDistinctValue(values[aggregate.distinctField])
	if !valid {
		return
	}
	if group.distinctValues == nil {
		group.distinctValues = make(map[typedTableDistinctValue]int64)
	}
	group.distinctValues[value] += delta
	if group.distinctValues[value] == 0 {
		delete(group.distinctValues, value)
	}
}

func typedTableAggregateDistinctValue(value TypedTableValue) (typedTableDistinctValue, bool) {
	if !value.Valid {
		return typedTableDistinctValue{}, false
	}
	distinct := typedTableDistinctValue{kind: value.Kind}
	switch value.Kind {
	case TypedTableString:
		distinct.stringValue = value.String
	case TypedTableInt64:
		distinct.int64Value = value.Int64
	case TypedTableFloat64:
		distinct.floatBits = math.Float64bits(value.Float64)
	case TypedTableBool:
		distinct.boolValue = value.Bool
	default:
		return typedTableDistinctValue{}, false
	}
	return distinct, true
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
	groupValues := make([]TypedTableValue, len(aggregate.groupBy))
	for index, column := range aggregate.groupBy {
		groupValues[index] = values[column]
	}
	return typedTableAggregateLegacyGroupKey(values, aggregate.groupBy), groupValues
}
