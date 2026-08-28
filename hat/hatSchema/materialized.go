package hatSchema

import (
	"fmt"
	"hatrie_cache/hat/hatSql"
	"strings"
	"sync"
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

type MaterializedSource struct {
	mu      sync.RWMutex
	columns []DerivedColumn
	nextID  map[string]int64
	rows    []Row
	indexes map[string]map[string][]int
}

func NewMaterializedSource(columns []DerivedColumn) *MaterializedSource {
	return &MaterializedSource{columns: append([]DerivedColumn(nil), columns...), nextID: map[string]int64{}, indexes: map[string]map[string][]int{}}
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
	for _, column := range source.columns {
		if !column.Indexed {
			continue
		}
		if source.indexes[column.Name] == nil {
			source.indexes[column.Name] = map[string][]int{}
		}
		key := fmt.Sprintf("%T:%v", materialized[column.Name], materialized[column.Name])
		source.indexes[column.Name][key] = append(source.indexes[column.Name][key], position)
	}
	return cloneRow(materialized), nil
}

func (source *MaterializedSource) Lookup(field string, value interface{}) []Row {
	if source == nil {
		return nil
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	positions := source.indexes[field][fmt.Sprintf("%T:%v", value, value)]
	rows := make([]Row, 0, len(positions))
	for _, position := range positions {
		rows = append(rows, cloneRow(source.rows[position]))
	}
	return rows
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
