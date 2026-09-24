package hatSchema

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrMaterializedSourceGeneratedDependencyUnknown indicates that a checked
	// generated column references a column absent from the schema.
	ErrMaterializedSourceGeneratedDependencyUnknown = errors.New("hatSchema: generated column dependency is unknown")
	// ErrMaterializedSourceGeneratedDependencyDuplicate indicates repeated
	// dependency metadata on one generated column.
	ErrMaterializedSourceGeneratedDependencyDuplicate = errors.New("hatSchema: generated column dependency is duplicated")
	// ErrMaterializedSourceGeneratedDependencyCycle indicates a cyclic
	// dependency graph that cannot be evaluated in a finite order.
	ErrMaterializedSourceGeneratedDependencyCycle = errors.New("hatSchema: generated column dependency cycle")
	// ErrMaterializedSourceGeneratedEvaluatorRequired indicates dependency
	// metadata was supplied for a column without a generated evaluator.
	ErrMaterializedSourceGeneratedEvaluatorRequired = errors.New("hatSchema: generated column evaluator is required")
)

// NewValidatedMaterializedSource creates a materialized source whose generated
// callbacks are evaluated in a cached dependency order. It rejects empty or
// duplicate column names, unknown or duplicate dependencies, dependency
// metadata on ordinary columns, and cycles before the source is published.
// NewMaterializedSource remains the compatibility constructor and preserves
// declaration-order callback evaluation.
func NewValidatedMaterializedSource(columns []DerivedColumn) (*MaterializedSource, error) {
	validatedColumns := cloneValidatedDerivedColumns(columns)
	order, err := buildGeneratedColumnOrder(validatedColumns)
	if err != nil {
		return nil, err
	}
	source := NewMaterializedSource(validatedColumns)
	source.generatedOrder = order
	return source, nil
}

func cloneValidatedDerivedColumns(columns []DerivedColumn) []DerivedColumn {
	cloned := append([]DerivedColumn(nil), columns...)
	for index := range cloned {
		cloned[index].GeneratedDependencies = append([]string(nil), columns[index].GeneratedDependencies...)
	}
	return cloned
}

func buildGeneratedColumnOrder(columns []DerivedColumn) ([]int, error) {
	columnIndexes := make(map[string]int, len(columns))
	for index, column := range columns {
		if strings.TrimSpace(column.Name) == "" {
			return nil, fmt.Errorf("hatSchema: materialized source column %d has an empty name", index)
		}
		if _, exists := columnIndexes[column.Name]; exists {
			return nil, fmt.Errorf("hatSchema: materialized source column %q is duplicated", column.Name)
		}
		columnIndexes[column.Name] = index
	}

	for index := range columns {
		column := &columns[index]
		if len(column.GeneratedDependencies) == 0 {
			continue
		}
		if column.Generated == nil {
			return nil, fmt.Errorf("%w: %q", ErrMaterializedSourceGeneratedEvaluatorRequired, column.Name)
		}
		seen := make(map[string]struct{}, len(column.GeneratedDependencies))
		for dependencyIndex, dependency := range column.GeneratedDependencies {
			dependency = strings.TrimSpace(dependency)
			if dependency == "" {
				return nil, fmt.Errorf("%w: %q has an empty dependency", ErrMaterializedSourceGeneratedDependencyUnknown, column.Name)
			}
			column.GeneratedDependencies[dependencyIndex] = dependency
			if _, exists := seen[dependency]; exists {
				return nil, fmt.Errorf("%w: %q depends on %q", ErrMaterializedSourceGeneratedDependencyDuplicate, column.Name, dependency)
			}
			seen[dependency] = struct{}{}
			if _, exists := columnIndexes[dependency]; !exists {
				return nil, fmt.Errorf("%w: %q depends on %q", ErrMaterializedSourceGeneratedDependencyUnknown, column.Name, dependency)
			}
		}
	}

	order := make([]int, 0, len(columns))
	state := make([]uint8, len(columns))
	var visit func(int) error
	visit = func(index int) error {
		switch state[index] {
		case 1:
			return fmt.Errorf("%w at %q", ErrMaterializedSourceGeneratedDependencyCycle, columns[index].Name)
		case 2:
			return nil
		}
		state[index] = 1
		for _, dependency := range columns[index].GeneratedDependencies {
			dependencyIndex := columnIndexes[dependency]
			if columns[dependencyIndex].Generated == nil {
				continue
			}
			if err := visit(dependencyIndex); err != nil {
				return err
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
