package hatSql

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrMemtxTableNil reports a method call on a nil row engine.
	ErrMemtxTableNil = errors.New("memtx table is nil")
	// ErrMemtxTableKeyNotFound reports a missing primary key.
	ErrMemtxTableKeyNotFound = errors.New("memtx table key does not exist")
	// ErrMemtxTableUniqueViolation reports a duplicate unique-index value.
	ErrMemtxTableUniqueViolation = errors.New("memtx table unique index violation")
)

// MemtxIndexDefinition declares one optional equality index over a scalar
// column. The primary key is the string passed to Upsert and is always
// indexed. Unique secondary indexes reject duplicate non-NULL values.
type MemtxIndexDefinition struct {
	Name   string
	Field  string
	Unique bool
}

// MemtxTableSchema describes the predictable in-memory tuple layout. The
// engine intentionally supports the plain scalar subset; advanced columnar
// features remain available through TypedTable.
type MemtxTableSchema struct {
	Name       string
	SourceName string
	Columns    []TypedTableColumn
	Indexes    []MemtxIndexDefinition
}

// MemtxTableMemoryUsage reports logical row and index bytes. It is an
// admission-oriented estimate, not a process heap measurement.
type MemtxTableMemoryUsage struct {
	Rows         int   `json:"rows"`
	IndexEntries int   `json:"index_entries"`
	LogicalBytes int64 `json:"logical_bytes"`
}

type memtxIndexValue struct {
	kind      TypedTableKind
	valid     bool
	stringVal string
	intVal    int64
	floatBits uint64
	boolVal   bool
}

type memtxTableIndex struct {
	definition MemtxIndexDefinition
	field      int
	postings   map[memtxIndexValue][]int
}

type memtxTuple struct {
	key          string
	values       []TypedTableValue
	logicalBytes int64
}

// MemtxTable is an opt-in Tarantool memtx-style row engine. It keeps each
// tuple together, maintains a primary-key map, and can maintain bounded scalar
// equality indexes. It implements the SQL source contracts without changing
// the existing TypedTable default.
type MemtxTable struct {
	mu           sync.RWMutex
	schema       MemtxTableSchema
	byName       map[string]int
	rows         []memtxTuple
	positions    map[string]int
	indexes      []memtxTableIndex
	indexByField map[string]int
	sequence     uint64
	logicalBytes int64
}

// NewMemtxTable validates a plain scalar schema and creates an empty row
// engine. Generated columns, TTL, dictionaries, and columnar caches are
// rejected instead of being silently ignored.
func NewMemtxTable(schema MemtxTableSchema) (*MemtxTable, error) {
	schema.Name = strings.TrimSpace(schema.Name)
	if schema.Name == "" {
		return nil, fmt.Errorf("memtx table name is required")
	}
	schema.SourceName = strings.ToUpper(strings.TrimSpace(schema.SourceName))
	if schema.SourceName == "" {
		schema.SourceName = "CACHE"
	}
	if len(schema.Columns) == 0 {
		return nil, fmt.Errorf("memtx table columns are required")
	}
	table := &MemtxTable{
		schema:       schema,
		byName:       make(map[string]int, len(schema.Columns)),
		positions:    make(map[string]int),
		indexByField: make(map[string]int, len(schema.Indexes)),
	}
	table.schema.Columns = append([]TypedTableColumn(nil), schema.Columns...)
	table.schema.Indexes = append([]MemtxIndexDefinition(nil), schema.Indexes...)
	for index := range table.schema.Columns {
		column := &table.schema.Columns[index]
		column.Name = strings.TrimSpace(column.Name)
		if column.Name == "" {
			return nil, fmt.Errorf("memtx table column %d has an empty name", index)
		}
		if _, exists := table.byName[column.Name]; exists {
			return nil, fmt.Errorf("memtx table has duplicate column %q", column.Name)
		}
		if column.Kind < TypedTableString || column.Kind > TypedTableBool {
			return nil, fmt.Errorf("memtx table column %q has invalid kind", column.Name)
		}
		if column.Generated != nil || column.GeneratedMode != TypedTableGeneratedMaterialized || len(column.GeneratedDependencies) != 0 {
			return nil, fmt.Errorf("memtx table column %q does not support generated values", column.Name)
		}
		if column.DictionaryEncoded || column.DictionaryAdaptive {
			return nil, fmt.Errorf("memtx table column %q does not support dictionaries", column.Name)
		}
		if column.TTL.Mode != TypedTableTTLDisabled || column.TTL.Field != "" || column.TTL.Lifetime != 0 || column.TTL.Clock != nil {
			return nil, fmt.Errorf("memtx table column %q does not support column TTL", column.Name)
		}
		table.byName[column.Name] = index
	}
	for index := range table.schema.Indexes {
		definition := &table.schema.Indexes[index]
		definition.Name = strings.TrimSpace(definition.Name)
		definition.Field = strings.TrimSpace(definition.Field)
		if definition.Name == "" || definition.Field == "" {
			return nil, fmt.Errorf("memtx table index %d requires name and field", index)
		}
		field, exists := table.byName[definition.Field]
		if !exists {
			return nil, fmt.Errorf("memtx table index %q references unknown field %q", definition.Name, definition.Field)
		}
		if _, exists := table.indexByField[definition.Field]; exists {
			return nil, fmt.Errorf("memtx table has multiple indexes for field %q", definition.Field)
		}
		for previous := 0; previous < index; previous++ {
			if table.schema.Indexes[previous].Name == definition.Name {
				return nil, fmt.Errorf("memtx table has duplicate index %q", definition.Name)
			}
		}
		table.indexByField[definition.Field] = index
		table.indexes = append(table.indexes, memtxTableIndex{
			definition: *definition,
			field:      field,
			postings:   make(map[memtxIndexValue][]int),
		})
	}
	return table, nil
}

// Upsert inserts or replaces one complete tuple and returns a copy-safe
// mutation record. Updates preserve the tuple's physical position.
func (table *MemtxTable) Upsert(key string, values []TypedTableValue) (TypedTableChange, error) {
	if table == nil {
		return TypedTableChange{}, ErrMemtxTableNil
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return TypedTableChange{}, fmt.Errorf("memtx table key is required")
	}
	if err := table.validateValues(values); err != nil {
		return TypedTableChange{}, err
	}
	values = cloneTypedTableValues(values)
	table.mu.Lock()
	defer table.mu.Unlock()
	position, exists := table.positions[key]
	if err := table.validateUniqueValuesLocked(values, position, exists); err != nil {
		return TypedTableChange{}, err
	}
	rowBytes := memtxTupleLogicalBytes(key, values)
	change := TypedTableChange{Key: key, After: cloneTypedTableValues(values)}
	if exists {
		old := table.rows[position]
		change.Operation = "UPDATE"
		change.Before = cloneTypedTableValues(old.values)
		table.removeIndexEntriesLocked(position, old.values)
		table.logicalBytes -= old.logicalBytes
		table.rows[position] = memtxTuple{key: key, values: values, logicalBytes: rowBytes}
		table.logicalBytes += rowBytes
		table.addIndexEntriesLocked(position, values)
	} else {
		change.Operation = "INSERT"
		position = len(table.rows)
		table.positions[key] = position
		table.rows = append(table.rows, memtxTuple{key: key, values: values, logicalBytes: rowBytes})
		table.logicalBytes += rowBytes
		table.addIndexEntriesLocked(position, values)
	}
	table.sequence++
	change.Sequence = table.sequence
	return change, nil
}

// Delete removes a tuple and keeps the remaining rows densely packed.
func (table *MemtxTable) Delete(key string) (TypedTableChange, error) {
	if table == nil {
		return TypedTableChange{}, ErrMemtxTableNil
	}
	key = strings.TrimSpace(key)
	table.mu.Lock()
	defer table.mu.Unlock()
	position, exists := table.positions[key]
	if !exists {
		return TypedTableChange{}, fmt.Errorf("%w: %q", ErrMemtxTableKeyNotFound, key)
	}
	removed := table.rows[position]
	change := TypedTableChange{Operation: "DELETE", Key: removed.key, Before: cloneTypedTableValues(removed.values)}
	table.removeIndexEntriesLocked(position, removed.values)
	last := len(table.rows) - 1
	if position != last {
		moved := table.rows[last]
		table.removeIndexEntriesLocked(last, moved.values)
		table.rows[position] = moved
		table.positions[moved.key] = position
		table.addIndexEntriesLocked(position, moved.values)
	}
	delete(table.positions, removed.key)
	table.rows = table.rows[:last]
	table.logicalBytes -= removed.logicalBytes
	table.sequence++
	change.Sequence = table.sequence
	return change, nil
}

// Get returns an independent row for the primary key.
func (table *MemtxTable) Get(key string) (Row, bool, error) {
	if table == nil {
		return nil, false, ErrMemtxTableNil
	}
	key = strings.TrimSpace(key)
	table.mu.RLock()
	defer table.mu.RUnlock()
	position, exists := table.positions[key]
	if !exists {
		return nil, false, nil
	}
	return table.rowLocked(position), true, nil
}

// Rows returns independent row maps in current physical tuple order.
func (table *MemtxTable) Rows() []Row {
	if table == nil {
		return nil
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	rows := make([]Row, len(table.rows))
	for index := range table.rows {
		rows[index] = table.rowLocked(index)
	}
	return rows
}

// ResolveSQLSource implements the ordinary SQL source contract.
func (table *MemtxTable) ResolveSQLSource(name, key string) ([]Row, error) {
	if table == nil {
		return nil, ErrMemtxTableNil
	}
	if !strings.EqualFold(strings.TrimSpace(name), table.schema.SourceName) || strings.TrimSpace(key) != table.schema.Name {
		return nil, nil
	}
	return table.Rows(), nil
}

// SQLSourceCardinality exposes exact row count without materializing tuples.
func (table *MemtxTable) SQLSourceCardinality(name, key string) (int, bool, bool, error) {
	if table == nil {
		return 0, false, false, ErrMemtxTableNil
	}
	if !strings.EqualFold(strings.TrimSpace(name), table.schema.SourceName) || strings.TrimSpace(key) != table.schema.Name {
		return 0, false, false, nil
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	return len(table.rows), true, true, nil
}

// ResolveSQLIndexedSource returns candidates from a configured equality index.
// The SQL executor still evaluates the complete predicate.
func (table *MemtxTable) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]Row, bool, error) {
	if table == nil {
		return nil, false, ErrMemtxTableNil
	}
	if !strings.EqualFold(strings.TrimSpace(name), table.schema.SourceName) || strings.TrimSpace(key) != table.schema.Name {
		return nil, false, nil
	}
	field = strings.TrimSpace(field)
	table.mu.RLock()
	defer table.mu.RUnlock()
	indexPosition, exists := table.indexByField[field]
	if !exists {
		return nil, false, nil
	}
	index := &table.indexes[indexPosition]
	indexValue, ok := memtxIndexValueFromInterface(value, table.schema.Columns[index.field].Kind)
	if !ok || !indexValue.valid {
		return nil, false, nil
	}
	positions := append([]int(nil), index.postings[indexValue]...)
	sort.Ints(positions)
	rows := make([]Row, 0, len(positions))
	for _, position := range positions {
		if position >= 0 && position < len(table.rows) {
			rows = append(rows, table.rowLocked(position))
		}
	}
	return rows, true, nil
}

// MemoryUsage reports logical tuple and index-entry bytes.
func (table *MemtxTable) MemoryUsage() MemtxTableMemoryUsage {
	if table == nil {
		return MemtxTableMemoryUsage{}
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	entries := 0
	for index := range table.indexes {
		for _, posting := range table.indexes[index].postings {
			entries += len(posting)
		}
	}
	return MemtxTableMemoryUsage{Rows: len(table.rows), IndexEntries: entries, LogicalBytes: table.logicalBytes}
}

// Schema returns an independent schema copy.
func (table *MemtxTable) Schema() MemtxTableSchema {
	if table == nil {
		return MemtxTableSchema{}
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	schema := table.schema
	schema.Columns = append([]TypedTableColumn(nil), table.schema.Columns...)
	schema.Indexes = append([]MemtxIndexDefinition(nil), table.schema.Indexes...)
	return schema
}

func (table *MemtxTable) validateValues(values []TypedTableValue) error {
	if len(values) != len(table.schema.Columns) {
		return fmt.Errorf("memtx table requires %d values, got %d", len(table.schema.Columns), len(values))
	}
	for index, value := range values {
		if value.Valid && value.Kind != table.schema.Columns[index].Kind {
			return fmt.Errorf("memtx table column %q requires kind %d", table.schema.Columns[index].Name, table.schema.Columns[index].Kind)
		}
	}
	return nil
}

func (table *MemtxTable) validateUniqueValuesLocked(values []TypedTableValue, current int, exists bool) error {
	for index := range table.indexes {
		if !table.indexes[index].definition.Unique {
			continue
		}
		field := table.indexes[index].field
		value, ok := memtxIndexValueFromTypedValue(values[field])
		if !ok || !value.valid {
			continue
		}
		for _, position := range table.indexes[index].postings[value] {
			if !exists || position != current {
				return fmt.Errorf("%w: index %q value for field %q", ErrMemtxTableUniqueViolation, table.indexes[index].definition.Name, table.schema.Columns[field].Name)
			}
		}
	}
	return nil
}

func (table *MemtxTable) addIndexEntriesLocked(position int, values []TypedTableValue) {
	for index := range table.indexes {
		value, ok := memtxIndexValueFromTypedValue(values[table.indexes[index].field])
		if ok && value.valid {
			table.indexes[index].postings[value] = append(table.indexes[index].postings[value], position)
		}
	}
}

func (table *MemtxTable) removeIndexEntriesLocked(position int, values []TypedTableValue) {
	for index := range table.indexes {
		value, ok := memtxIndexValueFromTypedValue(values[table.indexes[index].field])
		if !ok || !value.valid {
			continue
		}
		posting := table.indexes[index].postings[value]
		for postingIndex, candidate := range posting {
			if candidate != position {
				continue
			}
			copy(posting[postingIndex:], posting[postingIndex+1:])
			posting = posting[:len(posting)-1]
			if len(posting) == 0 {
				delete(table.indexes[index].postings, value)
			} else {
				table.indexes[index].postings[value] = posting
			}
			break
		}
	}
}

func (table *MemtxTable) rowLocked(position int) Row {
	row := make(Row, len(table.rows[position].values))
	for index, value := range table.rows[position].values {
		row[table.schema.Columns[index].Name] = typedTableValueInterface(value)
	}
	return row
}

func memtxTupleLogicalBytes(key string, values []TypedTableValue) int64 {
	bytes := int64(40 + len(key) + len(values)*24)
	for _, value := range values {
		if value.Valid && value.Kind == TypedTableString {
			bytes += int64(len(value.String))
		}
	}
	return bytes
}

func memtxIndexValueFromTypedValue(value TypedTableValue) (memtxIndexValue, bool) {
	if !value.Valid {
		return memtxIndexValue{}, true
	}
	key := memtxIndexValue{kind: value.Kind, valid: true}
	switch value.Kind {
	case TypedTableString:
		key.stringVal = value.String
	case TypedTableInt64:
		key.intVal = value.Int64
	case TypedTableFloat64:
		key.floatBits = memtxFloatBits(value.Float64)
	case TypedTableBool:
		key.boolVal = value.Bool
	default:
		return memtxIndexValue{}, false
	}
	return key, true
}

func memtxIndexValueFromInterface(value interface{}, kind TypedTableKind) (memtxIndexValue, bool) {
	if value == nil {
		return memtxIndexValue{}, true
	}
	switch kind {
	case TypedTableString:
		text, ok := value.(string)
		if !ok {
			return memtxIndexValue{}, false
		}
		return memtxIndexValue{kind: kind, valid: true, stringVal: text}, true
	case TypedTableInt64:
		var number int64
		switch typed := value.(type) {
		case int:
			number = int64(typed)
		case int8:
			number = int64(typed)
		case int16:
			number = int64(typed)
		case int32:
			number = int64(typed)
		case int64:
			number = typed
		case uint:
			if uint64(typed) > math.MaxInt64 {
				return memtxIndexValue{}, false
			}
			number = int64(typed)
		case uint8:
			number = int64(typed)
		case uint16:
			number = int64(typed)
		case uint32:
			number = int64(typed)
		case uint64:
			if typed > math.MaxInt64 {
				return memtxIndexValue{}, false
			}
			number = int64(typed)
		default:
			return memtxIndexValue{}, false
		}
		return memtxIndexValue{kind: kind, valid: true, intVal: number}, true
	case TypedTableFloat64:
		switch typed := value.(type) {
		case float32:
			return memtxIndexValue{kind: kind, valid: true, floatBits: memtxFloatBits(float64(typed))}, true
		case float64:
			return memtxIndexValue{kind: kind, valid: true, floatBits: memtxFloatBits(typed)}, true
		default:
			return memtxIndexValue{}, false
		}
	case TypedTableBool:
		boolean, ok := value.(bool)
		if !ok {
			return memtxIndexValue{}, false
		}
		return memtxIndexValue{kind: kind, valid: true, boolVal: boolean}, true
	default:
		return memtxIndexValue{}, false
	}
}

func memtxFloatBits(value float64) uint64 {
	if value == 0 {
		value = 0
	}
	return math.Float64bits(value)
}
