// Package hatSchema defines versioned, reversible schemas for SQL sources.
package hatSchema

import (
	"errors"
	"fmt"
	"strings"
)

// Type is the declared logical type of one source column.
type Type string

const (
	TypeText      Type = "TEXT"
	TypeNumber    Type = "NUMBER"
	TypeInteger   Type = "INTEGER"
	TypeDecimal   Type = "DECIMAL"
	TypeBoolean   Type = "BOOLEAN"
	TypeDate      Type = "DATE"
	TypeTimestamp Type = "TIMESTAMP"
	TypeUUID      Type = "UUID"
	TypeDuration  Type = "DURATION"
	TypeBinary    Type = "BINARY"
	TypeJSON      Type = "JSON"
)

// Column describes one ordered source field. NotNull defaults to false.
type Column struct {
	Name    string `json:"name"`
	Type    Type   `json:"type"`
	NotNull bool   `json:"not_null,omitempty"`
}

// Source is one named SQL source and its ordered schema.
type Source struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

// Schema is a versioned collection of named sources.
type Schema struct {
	Version uint64            `json:"version"`
	Sources map[string]Source `json:"sources"`
}

// Clone returns a deep independent schema copy.
func (schema Schema) Clone() Schema {
	out := Schema{Version: schema.Version, Sources: make(map[string]Source, len(schema.Sources))}
	for name, source := range schema.Sources {
		source.Columns = append([]Column(nil), source.Columns...)
		out.Sources[name] = source
	}
	return out
}

// ChangeKind identifies a reversible schema operation.
type ChangeKind string

const (
	ChangeCreateSource ChangeKind = "create_source"
	ChangeDropSource   ChangeKind = "drop_source"
	ChangeAddColumn    ChangeKind = "add_column"
	ChangeDropColumn   ChangeKind = "drop_column"
)

// Change modifies one source. CreateSource uses Source; column changes use
// SourceName and Column; DropSource uses SourceName.
type Change struct {
	Kind       ChangeKind `json:"kind"`
	SourceName string     `json:"source_name,omitempty"`
	Source     Source     `json:"source,omitempty"`
	Column     Column     `json:"column,omitempty"`
}

// Migration is an ordered forward change set with its explicit reverse.
type Migration struct {
	Version uint64   `json:"version"`
	Name    string   `json:"name"`
	Up      []Change `json:"up"`
	Down    []Change `json:"down"`
}

// Apply atomically applies the next migration version to schema.
func Apply(schema *Schema, migration Migration) error {
	if schema == nil {
		return errors.New("hatSchema: schema is nil")
	}
	if err := migration.Validate(); err != nil {
		return err
	}
	if migration.Version != schema.Version+1 {
		return fmt.Errorf("hatSchema: migration version %d requires schema version %d", migration.Version, schema.Version+1)
	}
	updated := schema.Clone()
	if err := applyChanges(&updated, migration.Up); err != nil {
		return err
	}
	updated.Version = migration.Version
	*schema = updated
	return nil
}

// Revert atomically applies a migration's declared reverse change set.
func Revert(schema *Schema, migration Migration) error {
	if schema == nil {
		return errors.New("hatSchema: schema is nil")
	}
	if err := migration.Validate(); err != nil {
		return err
	}
	if migration.Version != schema.Version {
		return fmt.Errorf("hatSchema: migration version %d cannot revert schema version %d", migration.Version, schema.Version)
	}
	updated := schema.Clone()
	if err := applyChanges(&updated, migration.Down); err != nil {
		return err
	}
	updated.Version--
	*schema = updated
	return nil
}

// Validate verifies that a migration has a sequentially applicable shape.
func (migration Migration) Validate() error {
	if migration.Version == 0 {
		return errors.New("hatSchema: migration version must be positive")
	}
	if strings.TrimSpace(migration.Name) == "" {
		return errors.New("hatSchema: migration name is required")
	}
	if len(migration.Up) == 0 || len(migration.Down) == 0 {
		return errors.New("hatSchema: migration must define both up and down changes")
	}
	return nil
}

func applyChanges(schema *Schema, changes []Change) error {
	if schema.Sources == nil {
		schema.Sources = make(map[string]Source)
	}
	for _, change := range changes {
		if err := applyChange(schema, change); err != nil {
			return err
		}
	}
	return nil
}

func applyChange(schema *Schema, change Change) error {
	sourceName := strings.TrimSpace(change.SourceName)
	switch change.Kind {
	case ChangeCreateSource:
		source := change.Source
		source.Name = strings.TrimSpace(source.Name)
		for index := range source.Columns {
			source.Columns[index] = normalizeColumn(source.Columns[index])
		}
		if err := validateSource(source); err != nil {
			return err
		}
		if _, exists := schema.Sources[source.Name]; exists {
			return fmt.Errorf("hatSchema: source %q already exists", source.Name)
		}
		source.Columns = append([]Column(nil), source.Columns...)
		schema.Sources[source.Name] = source
		return nil
	case ChangeDropSource:
		if sourceName == "" {
			return errors.New("hatSchema: source name is required")
		}
		if _, exists := schema.Sources[sourceName]; !exists {
			return fmt.Errorf("hatSchema: source %q does not exist", sourceName)
		}
		delete(schema.Sources, sourceName)
		return nil
	case ChangeAddColumn:
		source, exists := schema.Sources[sourceName]
		if !exists {
			return fmt.Errorf("hatSchema: source %q does not exist", sourceName)
		}
		column := normalizeColumn(change.Column)
		if err := validateColumn(column); err != nil {
			return err
		}
		if sourceColumnIndex(source, column.Name) >= 0 {
			return fmt.Errorf("hatSchema: column %q already exists in source %q", column.Name, sourceName)
		}
		source.Columns = append(source.Columns, column)
		schema.Sources[sourceName] = source
		return nil
	case ChangeDropColumn:
		source, exists := schema.Sources[sourceName]
		if !exists {
			return fmt.Errorf("hatSchema: source %q does not exist", sourceName)
		}
		columnName := strings.TrimSpace(change.Column.Name)
		index := sourceColumnIndex(source, columnName)
		if index < 0 {
			return fmt.Errorf("hatSchema: column %q does not exist in source %q", columnName, sourceName)
		}
		source.Columns = append(source.Columns[:index:index], source.Columns[index+1:]...)
		schema.Sources[sourceName] = source
		return nil
	default:
		return fmt.Errorf("hatSchema: unsupported change kind %q", change.Kind)
	}
}

func validateSource(source Source) error {
	if source.Name == "" {
		return errors.New("hatSchema: source name is required")
	}
	if len(source.Columns) == 0 {
		return fmt.Errorf("hatSchema: source %q must have columns", source.Name)
	}
	seen := make(map[string]struct{}, len(source.Columns))
	for _, column := range source.Columns {
		if err := validateColumn(column); err != nil {
			return err
		}
		if _, exists := seen[column.Name]; exists {
			return fmt.Errorf("hatSchema: duplicate column %q in source %q", column.Name, source.Name)
		}
		seen[column.Name] = struct{}{}
	}
	return nil
}

func validateColumn(column Column) error {
	if strings.TrimSpace(column.Name) == "" {
		return errors.New("hatSchema: column name is required")
	}
	switch column.Type {
	case TypeText, TypeNumber, TypeInteger, TypeDecimal, TypeBoolean, TypeDate, TypeTimestamp, TypeUUID, TypeDuration, TypeBinary, TypeJSON:
		return nil
	default:
		return fmt.Errorf("hatSchema: unsupported column type %q", column.Type)
	}
}

func normalizeColumn(column Column) Column {
	column.Name = strings.TrimSpace(column.Name)
	column.Type = Type(strings.ToUpper(strings.TrimSpace(string(column.Type))))
	return column
}

func sourceColumnIndex(source Source, name string) int {
	for index, column := range source.Columns {
		if column.Name == name {
			return index
		}
	}
	return -1
}
