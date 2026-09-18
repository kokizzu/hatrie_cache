package hatSchema

import (
	"errors"
	"fmt"
	"hatrie_cache/hat/hatSql"
	"strings"
	"sync"
)

var (
	ErrMaterializedSourceNil            = errors.New("hatSchema: materialized source is nil")
	ErrMaterializedSourceColumnRequired = errors.New("hatSchema: materialized source index column is required")
	ErrMaterializedSourceColumnUnknown  = errors.New("hatSchema: materialized source index column is unknown")
)

type GeneratedValue func(Row) (interface{}, error)

type DerivedColumn struct {
	Name      string
	Default   interface{}
	Identity  bool
	Sequence  string
	Generated GeneratedValue
	Indexed   bool
}

type SQLResolverAdapter struct {
	Base    hatSql.SourceResolver
	Sources map[string]*MaterializedSource
}

func (adapter SQLResolverAdapter) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if strings.EqualFold(name, "CACHE") {
		if source := adapter.Sources[strings.ToLower(key)]; source != nil {
			return sqlRows(source.Rows()), nil
		}
	}
	if adapter.Base == nil {
		return nil, nil
	}
	return adapter.Base.ResolveSQLSource(name, key)
}

func (adapter SQLResolverAdapter) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]hatSql.Row, bool, error) {
	if strings.EqualFold(name, "CACHE") {
		if source := adapter.Sources[strings.ToLower(key)]; source != nil {
			if !source.HasIndex(field) {
				return nil, false, nil
			}
			return sqlRows(source.Lookup(field, value)), true, nil
		}
	}
	if indexed, ok := adapter.Base.(hatSql.IndexedSourceResolver); ok {
		return indexed.ResolveSQLIndexedSource(name, key, field, value)
	}
	return nil, false, nil
}

func sqlRows(rows []Row) []hatSql.Row {
	converted := make([]hatSql.Row, len(rows))
	for index, row := range rows {
		converted[index] = hatSql.Row(row)
	}
	return converted
}

// SecondaryIndexBuildReport describes one online secondary-index build.
type SecondaryIndexBuildReport struct {
	Field    string
	Rows     int
	Attempts int
}

type MaterializedSource struct {
	mu            sync.RWMutex
	columns       []DerivedColumn
	nextID        map[string]int64
	rows          []Row
	indexes       map[string]map[string][]int
	indexedFields map[string]struct{}
	generation    uint64
}

func NewMaterializedSource(columns []DerivedColumn) *MaterializedSource {
	indexedFields := make(map[string]struct{})
	for _, column := range columns {
		if column.Indexed {
			indexedFields[column.Name] = struct{}{}
		}
	}
	return &MaterializedSource{
		columns:       append([]DerivedColumn(nil), columns...),
		nextID:        map[string]int64{},
		indexes:       map[string]map[string][]int{},
		indexedFields: indexedFields,
	}
}

func (source *MaterializedSource) Insert(row Row) (Row, error) {
	if source == nil {
		return nil, fmt.Errorf("hatSchema: materialized source is nil")
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	materialized := cloneRow(row)
	for _, column := range source.columns {
		if column.Name == "" {
			return nil, fmt.Errorf("hatSchema: derived column name is required")
		}
		if _, exists := materialized[column.Name]; !exists && column.Default != nil {
			materialized[column.Name] = column.Default
		}
		if _, exists := materialized[column.Name]; !exists && (column.Identity || column.Sequence != "") {
			sequence := column.Name
			if column.Sequence != "" {
				sequence = column.Sequence
			}
			source.nextID[sequence]++
			materialized[column.Name] = source.nextID[sequence]
		}
	}
	for _, column := range source.columns {
		if column.Generated == nil {
			continue
		}
		value, err := column.Generated(cloneRow(materialized))
		if err != nil {
			return nil, fmt.Errorf("hatSchema: generate %q: %w", column.Name, err)
		}
		materialized[column.Name] = value
	}
	position := len(source.rows)
	source.rows = append(source.rows, cloneRow(materialized))
	for field := range source.indexedFields {
		if source.indexes[field] == nil {
			source.indexes[field] = map[string][]int{}
		}
		key := materializedIndexKey(materialized[field])
		source.indexes[field][key] = append(source.indexes[field][key], position)
	}
	source.generation++
	return cloneRow(materialized), nil
}

// HasIndex reports whether field has a maintained equality index.
func (source *MaterializedSource) HasIndex(field string) bool {
	if source == nil {
		return false
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return false
	}
	source.mu.RLock()
	_, indexed := source.indexedFields[field]
	source.mu.RUnlock()
	return indexed
}

// BuildSecondaryIndex builds and atomically installs an equality index while
// allowing inserts and reads to continue. The builder snapshots row headers,
// performs the expensive map construction outside the lock, and retries when
// a concurrent insert changes the source generation before publication.
func (source *MaterializedSource) BuildSecondaryIndex(field string) (SecondaryIndexBuildReport, error) {
	if source == nil {
		return SecondaryIndexBuildReport{}, ErrMaterializedSourceNil
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return SecondaryIndexBuildReport{}, ErrMaterializedSourceColumnRequired
	}
	report := SecondaryIndexBuildReport{Field: field}
	for {
		source.mu.RLock()
		if !source.hasColumnLocked(field) {
			source.mu.RUnlock()
			return SecondaryIndexBuildReport{}, fmt.Errorf("%w: %s", ErrMaterializedSourceColumnUnknown, field)
		}
		generation := source.generation
		rows := append([]Row(nil), source.rows...)
		source.mu.RUnlock()

		index := make(map[string][]int, len(rows))
		for position, row := range rows {
			key := materializedIndexKey(row[field])
			index[key] = append(index[key], position)
		}
		report.Attempts++

		source.mu.Lock()
		if source.generation != generation {
			source.mu.Unlock()
			continue
		}
		if source.indexes == nil {
			source.indexes = make(map[string]map[string][]int)
		}
		if source.indexedFields == nil {
			source.indexedFields = make(map[string]struct{})
		}
		source.indexes[field] = index
		source.indexedFields[field] = struct{}{}
		source.mu.Unlock()
		report.Rows = len(rows)
		return report, nil
	}
}

func (source *MaterializedSource) Lookup(field string, value interface{}) []Row {
	if source == nil {
		return nil
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	positions := source.indexes[field][materializedIndexKey(value)]
	rows := make([]Row, 0, len(positions))
	for _, position := range positions {
		rows = append(rows, cloneRow(source.rows[position]))
	}
	return rows
}

func (source *MaterializedSource) hasColumnLocked(field string) bool {
	for _, column := range source.columns {
		if column.Name == field {
			return true
		}
	}
	return false
}

func materializedIndexKey(value interface{}) string {
	return fmt.Sprintf("%T:%v", value, value)
}

func (source *MaterializedSource) Rows() []Row {
	if source == nil {
		return nil
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	rows := make([]Row, len(source.rows))
	for index, row := range source.rows {
		rows[index] = cloneRow(row)
	}
	return rows
}

func cloneRow(row Row) Row {
	copy := make(Row, len(row))
	for key, value := range row {
		copy[key] = value
	}
	return copy
}
