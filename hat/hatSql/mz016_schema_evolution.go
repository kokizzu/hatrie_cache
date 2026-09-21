package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrSQLSchemaEvolutionInvalid identifies a malformed schema or option.
	ErrSQLSchemaEvolutionInvalid = errors.New("hatSql: invalid schema evolution plan")
	// ErrSQLSchemaEvolutionIncompatible identifies a change that cannot be
	// adapted without changing the consumer's row semantics.
	ErrSQLSchemaEvolutionIncompatible = errors.New("hatSql: incompatible schema evolution")
	// ErrSQLSchemaEvolutionRowInvalid identifies a row that violates the
	// current schema while it is being adapted.
	ErrSQLSchemaEvolutionRowInvalid = errors.New("hatSql: invalid schema evolution row")
)

const (
	// DefaultSQLSchemaEvolutionMaxColumns bounds one plan when MaxColumns is
	// zero.
	DefaultSQLSchemaEvolutionMaxColumns = 4096
	maxSQLSchemaEvolutionColumns        = 1 << 16
	maxSQLSchemaEvolutionVersionBytes   = 256
)

// SQLSchemaEvolutionOptions controls an opt-in compatibility plan between a
// live source schema and the schema expected by a consumer. A zero value is
// strict: only identical columns are accepted.
type SQLSchemaEvolutionOptions struct {
	// AllowAddedColumns permits columns present in the current source but not
	// in the expected consumer schema. They are ignored during adaptation.
	AllowAddedColumns bool
	// AllowDroppedColumns permits expected nullable columns that are absent
	// from the current source. They are emitted as NULL.
	AllowDroppedColumns bool
	// CurrentVersion and ExpectedVersion are opaque caller-owned dependency
	// tokens carried by the immutable plan.
	CurrentVersion  string
	ExpectedVersion string
	// MaxColumns bounds both schemas. Zero uses
	// DefaultSQLSchemaEvolutionMaxColumns.
	MaxColumns int
}

func (options SQLSchemaEvolutionOptions) normalize() (SQLSchemaEvolutionOptions, error) {
	if options.MaxColumns == 0 {
		options.MaxColumns = DefaultSQLSchemaEvolutionMaxColumns
	}
	if options.MaxColumns < 1 || options.MaxColumns > maxSQLSchemaEvolutionColumns {
		return SQLSchemaEvolutionOptions{}, fmt.Errorf("%w: MaxColumns must be between 1 and %d", ErrSQLSchemaEvolutionInvalid, maxSQLSchemaEvolutionColumns)
	}
	if len(options.CurrentVersion) > maxSQLSchemaEvolutionVersionBytes || len(options.ExpectedVersion) > maxSQLSchemaEvolutionVersionBytes {
		return SQLSchemaEvolutionOptions{}, fmt.Errorf("%w: schema version exceeds %d bytes", ErrSQLSchemaEvolutionInvalid, maxSQLSchemaEvolutionVersionBytes)
	}
	return options, nil
}

// SQLSchemaEvolutionChangeKind identifies a planned schema difference.
type SQLSchemaEvolutionChangeKind uint8

const (
	SQLSchemaEvolutionAddedColumn SQLSchemaEvolutionChangeKind = iota + 1
	SQLSchemaEvolutionDroppedColumn
	SQLSchemaEvolutionChangedColumn
)

// String returns a stable diagnostic name for a change kind.
func (kind SQLSchemaEvolutionChangeKind) String() string {
	switch kind {
	case SQLSchemaEvolutionAddedColumn:
		return "added"
	case SQLSchemaEvolutionDroppedColumn:
		return "dropped"
	case SQLSchemaEvolutionChangedColumn:
		return "changed"
	default:
		return "unknown"
	}
}

// SQLSchemaEvolutionChange describes one difference between the live source
// schema and the consumer schema. Current and Expected are detached copies.
type SQLSchemaEvolutionChange struct {
	Column   string
	Kind     SQLSchemaEvolutionChangeKind
	Current  SQLRowBinaryColumn
	Expected SQLRowBinaryColumn
}

// SQLSchemaEvolutionPlan maps rows from CurrentSchema to ExpectedSchema. It
// is immutable and safe for concurrent reads after construction.
type SQLSchemaEvolutionPlan struct {
	currentVersion  string
	expectedVersion string
	current         []SQLRowBinaryColumn
	expected        []SQLRowBinaryColumn
	mapping         []int
	changes         []SQLSchemaEvolutionChange
}

// NewSQLSchemaEvolutionPlan creates a reusable compatibility plan. current is
// the live source schema; expected is the schema consumed by an existing
// query, sink, or decoder. The plan does not change default SQL execution.
func NewSQLSchemaEvolutionPlan(current, expected []SQLRowBinaryColumn, options SQLSchemaEvolutionOptions) (*SQLSchemaEvolutionPlan, error) {
	normalized, err := options.normalize()
	if err != nil {
		return nil, err
	}
	if len(current) > normalized.MaxColumns || len(expected) > normalized.MaxColumns {
		return nil, fmt.Errorf("%w: schema column count exceeds %d", ErrSQLSchemaEvolutionInvalid, normalized.MaxColumns)
	}
	if err := validateSQLSchemaEvolutionColumns(current, "current"); err != nil {
		return nil, err
	}
	if err := validateSQLSchemaEvolutionColumns(expected, "expected"); err != nil {
		return nil, err
	}
	current = cloneSQLSchemaEvolutionColumns(current)
	expected = cloneSQLSchemaEvolutionColumns(expected)
	currentIndexes := make(map[string]int, len(current))
	for index, column := range current {
		currentIndexes[column.Name] = index
	}
	expectedIndexes := make(map[string]struct{}, len(expected))
	for _, column := range expected {
		expectedIndexes[column.Name] = struct{}{}
	}

	changes := make([]SQLSchemaEvolutionChange, 0)
	for _, column := range current {
		if _, found := expectedIndexes[column.Name]; found {
			continue
		}
		if !normalized.AllowAddedColumns {
			return nil, fmt.Errorf("%w: current column %q is not in expected schema", ErrSQLSchemaEvolutionIncompatible, column.Name)
		}
		changes = append(changes, SQLSchemaEvolutionChange{
			Column:  column.Name,
			Kind:    SQLSchemaEvolutionAddedColumn,
			Current: cloneSQLSchemaEvolutionColumn(column),
		})
	}

	mapping := make([]int, len(expected))
	for index, column := range expected {
		currentIndex, found := currentIndexes[column.Name]
		if !found {
			if !normalized.AllowDroppedColumns {
				return nil, fmt.Errorf("%w: expected column %q is absent from current schema", ErrSQLSchemaEvolutionIncompatible, column.Name)
			}
			if !column.Nullable {
				return nil, fmt.Errorf("%w: dropped expected column %q is not nullable", ErrSQLSchemaEvolutionIncompatible, column.Name)
			}
			mapping[index] = -1
			changes = append(changes, SQLSchemaEvolutionChange{
				Column:   column.Name,
				Kind:     SQLSchemaEvolutionDroppedColumn,
				Expected: cloneSQLSchemaEvolutionColumn(column),
			})
			continue
		}
		mapping[index] = currentIndex
		currentColumn := current[currentIndex]
		if !sqlSchemaEvolutionTypesCompatible(currentColumn, column) {
			return nil, fmt.Errorf("%w: column %q changed from %s to %s", ErrSQLSchemaEvolutionIncompatible, column.Name, sqlSchemaEvolutionColumnDescription(currentColumn), sqlSchemaEvolutionColumnDescription(column))
		}
	}

	return &SQLSchemaEvolutionPlan{
		currentVersion:  normalized.CurrentVersion,
		expectedVersion: normalized.ExpectedVersion,
		current:         current,
		expected:        expected,
		mapping:         mapping,
		changes:         changes,
	}, nil
}

// CurrentVersion returns the opaque live-source version token.
func (plan *SQLSchemaEvolutionPlan) CurrentVersion() string {
	if plan == nil {
		return ""
	}
	return plan.currentVersion
}

// ExpectedVersion returns the opaque consumer version token.
func (plan *SQLSchemaEvolutionPlan) ExpectedVersion() string {
	if plan == nil {
		return ""
	}
	return plan.expectedVersion
}

// CurrentSchema returns a detached copy of the live source schema.
func (plan *SQLSchemaEvolutionPlan) CurrentSchema() []SQLRowBinaryColumn {
	if plan == nil {
		return nil
	}
	return cloneSQLSchemaEvolutionColumns(plan.current)
}

// ExpectedSchema returns a detached copy of the consumer schema.
func (plan *SQLSchemaEvolutionPlan) ExpectedSchema() []SQLRowBinaryColumn {
	if plan == nil {
		return nil
	}
	return cloneSQLSchemaEvolutionColumns(plan.expected)
}

// Changes returns detached, deterministic change descriptions.
func (plan *SQLSchemaEvolutionPlan) Changes() []SQLSchemaEvolutionChange {
	if plan == nil {
		return nil
	}
	changes := make([]SQLSchemaEvolutionChange, len(plan.changes))
	for index, change := range plan.changes {
		changes[index] = change
		changes[index].Current = cloneSQLSchemaEvolutionColumn(change.Current)
		changes[index].Expected = cloneSQLSchemaEvolutionColumn(change.Expected)
	}
	return changes
}

// AdaptRow projects one live-source row into the expected schema. Added
// source columns are omitted; an explicitly allowed dropped nullable column is
// emitted as nil. The returned map is newly allocated and owned by the caller.
func (plan *SQLSchemaEvolutionPlan) AdaptRow(row Row) (Row, error) {
	if plan == nil || len(plan.expected) == 0 {
		return nil, ErrSQLSchemaEvolutionInvalid
	}
	adapted := make(Row, len(plan.expected))
	for expectedIndex, expectedColumn := range plan.expected {
		currentIndex := plan.mapping[expectedIndex]
		if currentIndex < 0 {
			adapted[expectedColumn.Name] = nil
			continue
		}
		currentColumn := plan.current[currentIndex]
		value, present := row[currentColumn.Name]
		if !present || value == nil {
			if !currentColumn.Nullable || !expectedColumn.Nullable {
				return nil, fmt.Errorf("%w: row is missing required column %q", ErrSQLSchemaEvolutionRowInvalid, expectedColumn.Name)
			}
			adapted[expectedColumn.Name] = nil
			continue
		}
		if err := validateSQLRowBinaryDecodedValue(currentColumn, value, 0); err != nil {
			return nil, fmt.Errorf("%w: column %q: %v", ErrSQLSchemaEvolutionRowInvalid, expectedColumn.Name, err)
		}
		adapted[expectedColumn.Name] = value
	}
	return adapted, nil
}

// AdaptRows adapts rows in input order and stops at the first invalid row.
func (plan *SQLSchemaEvolutionPlan) AdaptRows(rows []Row) ([]Row, error) {
	if plan == nil {
		return nil, ErrSQLSchemaEvolutionInvalid
	}
	adapted := make([]Row, 0, len(rows))
	for index, row := range rows {
		value, err := plan.AdaptRow(row)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", index+1, err)
		}
		adapted = append(adapted, value)
	}
	return adapted, nil
}

func validateSQLSchemaEvolutionColumns(columns []SQLRowBinaryColumn, label string) error {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return fmt.Errorf("%w: %s schema: %v", ErrSQLSchemaEvolutionInvalid, label, err)
	}
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if strings.TrimSpace(column.Name) == "" {
			return fmt.Errorf("%w: %s schema has an empty column name", ErrSQLSchemaEvolutionInvalid, label)
		}
		if _, found := seen[column.Name]; found {
			return fmt.Errorf("%w: %s schema repeats column %q", ErrSQLSchemaEvolutionInvalid, label, column.Name)
		}
		seen[column.Name] = struct{}{}
	}
	return nil
}

func sqlSchemaEvolutionTypesCompatible(current, expected SQLRowBinaryColumn) bool {
	if current.Type != expected.Type || current.Nullable && !expected.Nullable {
		return false
	}
	if current.Type == SQLRowBinaryEnum8 || current.Type == SQLRowBinaryEnum16 {
		if len(current.EnumValues) != len(expected.EnumValues) {
			return false
		}
		for index := range current.EnumValues {
			if current.EnumValues[index] != expected.EnumValues[index] {
				return false
			}
		}
	}
	if current.Type == SQLRowBinaryDecimal128 || current.Type == SQLRowBinaryDecimal256 {
		return current.DecimalScale == expected.DecimalScale && current.DecimalPrecision == expected.DecimalPrecision
	}
	return true
}

func sqlSchemaEvolutionColumnDescription(column SQLRowBinaryColumn) string {
	return fmt.Sprintf("type=%d nullable=%t", column.Type, column.Nullable)
}

func cloneSQLSchemaEvolutionColumns(columns []SQLRowBinaryColumn) []SQLRowBinaryColumn {
	if columns == nil {
		return nil
	}
	cloned := make([]SQLRowBinaryColumn, len(columns))
	for index, column := range columns {
		cloned[index] = cloneSQLSchemaEvolutionColumn(column)
	}
	return cloned
}

func cloneSQLSchemaEvolutionColumn(column SQLRowBinaryColumn) SQLRowBinaryColumn {
	column.EnumValues = append([]string(nil), column.EnumValues...)
	return column
}
