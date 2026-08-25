package hatSchema

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"hatrie_cache/hat/hatSql"
)

// ConstraintKind identifies a schema-level data-integrity rule.
type ConstraintKind string

const (
	ConstraintNotNull    ConstraintKind = "not_null"
	ConstraintUnique     ConstraintKind = "unique"
	ConstraintCheck      ConstraintKind = "check"
	ConstraintForeignKey ConstraintKind = "foreign_key"
)

// Constraint is a named source-level integrity rule. CHECK expressions use
// the project's read-only SQL expression syntax and source column names.
// Foreign-key validation compares Columns to ReferenceColumns in the named
// ReferenceSource. NULL values are allowed by UNIQUE, CHECK, and foreign-key
// rules, matching SQL's three-valued constraint semantics.
type Constraint struct {
	Name             string         `json:"name"`
	Kind             ConstraintKind `json:"kind"`
	Columns          []string       `json:"columns,omitempty"`
	Expression       string         `json:"expression,omitempty"`
	ReferenceSource  string         `json:"reference_source,omitempty"`
	ReferenceColumns []string       `json:"reference_columns,omitempty"`
}

// Row is one source row presented to the schema validator.
type Row map[string]interface{}

// SourceRowsResolver supplies rows for a foreign source when validating one
// source independently with ValidateRows.
type SourceRowsResolver func(source string) ([]Row, error)

// Validate reports invalid source definitions before rows are accepted.
func (schema Schema) Validate() error {
	for sourceName, source := range schema.Sources {
		if source.Name != sourceName {
			return fmt.Errorf("hatSchema: source map key %q does not match source name %q", sourceName, source.Name)
		}
		if err := validateSource(source); err != nil {
			return err
		}
		for _, constraint := range source.Constraints {
			if constraint.Kind == ConstraintForeignKey {
				if _, exists := schema.Sources[constraint.ReferenceSource]; !exists {
					return fmt.Errorf("hatSchema: constraint %q in source %q references unknown source %q", constraint.Name, source.Name, constraint.ReferenceSource)
				}
				reference := schema.Sources[constraint.ReferenceSource]
				for _, column := range constraint.ReferenceColumns {
					if sourceColumnIndex(reference, column) < 0 {
						return fmt.Errorf("hatSchema: constraint %q in source %q references unknown column %q in source %q", constraint.Name, source.Name, column, reference.Name)
					}
				}
			}
		}
	}
	return nil
}

// ValidateDataset validates every declared source against an immutable view of
// all supplied rows. It never modifies row maps or the schema.
func ValidateDataset(schema Schema, rowsBySource map[string][]Row) error {
	if err := schema.Validate(); err != nil {
		return err
	}
	names := make([]string, 0, len(schema.Sources))
	for name := range schema.Sources {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		source := schema.Sources[name]
		if err := validateRows(source, rowsBySource[name], func(reference string) ([]Row, error) {
			return rowsBySource[reference], nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// ValidateRows validates one source. The resolver is required only when a
// foreign-key constraint references a different source.
func ValidateRows(schema Schema, sourceName string, rows []Row, resolver SourceRowsResolver) error {
	if err := schema.Validate(); err != nil {
		return err
	}
	source, exists := schema.Sources[sourceName]
	if !exists {
		return fmt.Errorf("hatSchema: source %q does not exist", sourceName)
	}
	return validateRows(source, rows, func(reference string) ([]Row, error) {
		if reference == sourceName {
			return rows, nil
		}
		if resolver == nil {
			return nil, fmt.Errorf("hatSchema: source rows resolver is required for foreign-key constraint")
		}
		return resolver(reference)
	})
}

func validateRows(source Source, rows []Row, resolve SourceRowsResolver) error {
	for rowIndex, row := range rows {
		for _, column := range source.Columns {
			if column.NotNull && row[column.Name] == nil {
				return constraintViolation(source, "column "+column.Name+" NOT NULL", rowIndex)
			}
		}
		for _, constraint := range source.Constraints {
			switch constraint.Kind {
			case ConstraintNotNull:
				if row[constraint.Columns[0]] == nil {
					return constraintViolation(source, constraint.Name, rowIndex)
				}
			case ConstraintCheck:
				valid, err := evaluateCheck(source, row, constraint.Expression)
				if err != nil {
					return fmt.Errorf("hatSchema: CHECK constraint %q in source %q row %d: %w", constraint.Name, source.Name, rowIndex+1, err)
				}
				if !valid {
					return constraintViolation(source, constraint.Name, rowIndex)
				}
			}
		}
	}
	for _, constraint := range source.Constraints {
		switch constraint.Kind {
		case ConstraintUnique:
			seen := make(map[string]int, len(rows))
			for rowIndex, row := range rows {
				key, present, err := constraintRowKey(row, constraint.Columns)
				if err != nil {
					return fmt.Errorf("hatSchema: UNIQUE constraint %q in source %q row %d: %w", constraint.Name, source.Name, rowIndex+1, err)
				}
				if !present {
					continue
				}
				if previous, exists := seen[key]; exists {
					return fmt.Errorf("hatSchema: UNIQUE constraint %q in source %q duplicates rows %d and %d", constraint.Name, source.Name, previous+1, rowIndex+1)
				}
				seen[key] = rowIndex
			}
		case ConstraintForeignKey:
			references, err := resolve(constraint.ReferenceSource)
			if err != nil {
				return fmt.Errorf("hatSchema: foreign-key constraint %q in source %q: %w", constraint.Name, source.Name, err)
			}
			referenceKeys := make(map[string]struct{}, len(references))
			for rowIndex, reference := range references {
				key, present, err := constraintRowKey(reference, constraint.ReferenceColumns)
				if err != nil {
					return fmt.Errorf("hatSchema: foreign-key constraint %q reference row %d: %w", constraint.Name, rowIndex+1, err)
				}
				if present {
					referenceKeys[key] = struct{}{}
				}
			}
			for rowIndex, row := range rows {
				key, present, err := constraintRowKey(row, constraint.Columns)
				if err != nil {
					return fmt.Errorf("hatSchema: foreign-key constraint %q in source %q row %d: %w", constraint.Name, source.Name, rowIndex+1, err)
				}
				if present {
					if _, exists := referenceKeys[key]; !exists {
						return constraintViolation(source, constraint.Name, rowIndex)
					}
				}
			}
		}
	}
	return nil
}

func evaluateCheck(source Source, row Row, expression string) (bool, error) {
	parameters := make([]interface{}, len(source.Columns))
	placeholders := make([]string, len(source.Columns))
	columns := make([]string, len(source.Columns))
	for index, column := range source.Columns {
		parameters[index] = row[column.Name]
		placeholders[index] = fmt.Sprintf("$%d", index+1)
		columns[index] = column.Name
	}
	query := "FROM VALUES (" + strings.Join(placeholders, ", ") + ") AS values(" + strings.Join(columns, ", ") + ") SELECT (" + expression + ") AS constraint_valid"
	result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, nil, parameters, hatSql.QueryOptions{})
	if err != nil {
		return false, err
	}
	if len(result.Rows) != 1 {
		return false, fmt.Errorf("CHECK expression produced %d rows", len(result.Rows))
	}
	value := result.Rows[0]["constraint_valid"]
	if value == nil {
		return true, nil
	}
	valid, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("CHECK expression must evaluate to BOOLEAN, got %T", value)
	}
	return valid, nil
}

func validateConstraint(source Source, constraint Constraint) error {
	if constraint.Name == "" {
		return errorsNew("constraint name is required")
	}
	for _, column := range constraint.Columns {
		if sourceColumnIndex(source, column) < 0 {
			return fmt.Errorf("hatSchema: constraint %q in source %q uses unknown column %q", constraint.Name, source.Name, column)
		}
	}
	switch constraint.Kind {
	case ConstraintNotNull:
		if len(constraint.Columns) != 1 {
			return fmt.Errorf("hatSchema: NOT NULL constraint %q in source %q requires exactly one column", constraint.Name, source.Name)
		}
	case ConstraintUnique:
		if len(constraint.Columns) == 0 {
			return fmt.Errorf("hatSchema: UNIQUE constraint %q in source %q requires columns", constraint.Name, source.Name)
		}
	case ConstraintCheck:
		if constraint.Expression == "" {
			return fmt.Errorf("hatSchema: CHECK constraint %q in source %q requires an expression", constraint.Name, source.Name)
		}
	case ConstraintForeignKey:
		if len(constraint.Columns) == 0 || len(constraint.Columns) != len(constraint.ReferenceColumns) || constraint.ReferenceSource == "" {
			return fmt.Errorf("hatSchema: foreign-key constraint %q in source %q requires equally sized source and reference columns", constraint.Name, source.Name)
		}
	default:
		return fmt.Errorf("hatSchema: unsupported constraint kind %q", constraint.Kind)
	}
	return nil
}

func normalizeConstraint(constraint Constraint) Constraint {
	constraint.Name = strings.TrimSpace(constraint.Name)
	constraint.Kind = ConstraintKind(strings.ToLower(strings.TrimSpace(string(constraint.Kind))))
	constraint.Expression = strings.TrimSpace(constraint.Expression)
	constraint.ReferenceSource = strings.TrimSpace(constraint.ReferenceSource)
	for index := range constraint.Columns {
		constraint.Columns[index] = strings.TrimSpace(constraint.Columns[index])
	}
	for index := range constraint.ReferenceColumns {
		constraint.ReferenceColumns[index] = strings.TrimSpace(constraint.ReferenceColumns[index])
	}
	return constraint
}

func cloneConstraint(constraint Constraint) Constraint {
	constraint.Columns = append([]string(nil), constraint.Columns...)
	constraint.ReferenceColumns = append([]string(nil), constraint.ReferenceColumns...)
	return constraint
}

func cloneConstraints(constraints []Constraint) []Constraint {
	out := make([]Constraint, len(constraints))
	for index, constraint := range constraints {
		out[index] = cloneConstraint(constraint)
	}
	return out
}

func constraintRowKey(row Row, columns []string) (string, bool, error) {
	parts := make([]string, len(columns))
	for index, column := range columns {
		value := row[column]
		if value == nil {
			return "", false, nil
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", false, err
		}
		parts[index] = fmt.Sprintf("%T:%s", value, encoded)
	}
	return strings.Join(parts, "\x1f"), true, nil
}

func constraintViolation(source Source, constraint string, rowIndex int) error {
	return fmt.Errorf("hatSchema: constraint %q violated in source %q row %d", constraint, source.Name, rowIndex+1)
}

func errorsNew(message string) error { return fmt.Errorf("hatSchema: %s", message) }
