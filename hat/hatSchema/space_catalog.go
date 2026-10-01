package hatSchema

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrSpaceCatalogInvalid      = errors.New("hatSchema: space catalog definition is invalid")
	ErrSpaceCatalogNameRequired = errors.New("hatSchema: space catalog space name is required")
	ErrSpaceCatalogNil          = errors.New("hatSchema: space catalog is nil")
	ErrSpaceCatalogLimit        = errors.New("hatSchema: space catalog limit exceeded")
)

const (
	MaxSpaceCatalogSpaces  = 4096
	MaxSpaceCatalogIndexes = 256
)

// IndexKind identifies the access structure declared for a named space.
type IndexKind string

const (
	IndexKindHash       IndexKind = "hash"
	IndexKindTree       IndexKind = "tree"
	IndexKindRTree      IndexKind = "rtree"
	IndexKindFunctional IndexKind = "functional"
)

// IndexDefinition describes one named-space index. Columns are ordered and
// refer to fields in SpaceDefinition.Source. Predicate and PredicateColumns
// make an index conditional for planner metadata; predicate evaluation and
// runtime index maintenance remain owned by the execution layer.
type IndexDefinition struct {
	Name             string    `json:"name"`
	Kind             IndexKind `json:"kind"`
	Columns          []string  `json:"columns,omitempty"`
	Expression       string    `json:"expression,omitempty"`
	Predicate        string    `json:"predicate,omitempty"`
	PredicateColumns []string  `json:"predicate_columns,omitempty"`
	Unique           bool      `json:"unique,omitempty"`
}

// SpaceDefinition combines a versioned source schema, constraints, and named
// index declarations in one catalog entry.
type SpaceDefinition struct {
	Name    string            `json:"name"`
	Version uint64            `json:"version"`
	Source  Source            `json:"source"`
	Indexes []IndexDefinition `json:"indexes,omitempty"`
}

// SpaceCatalog stores optional named-space definitions with concurrent lookup
// and deterministic clone-on-read listing.
type SpaceCatalog struct {
	mu     sync.RWMutex
	spaces map[string]SpaceDefinition
}

// NewSpaceCatalog validates and installs the initial definitions. Later
// Upsert calls can replace an existing definition atomically.
func NewSpaceCatalog(definitions []SpaceDefinition) (*SpaceCatalog, error) {
	if len(definitions) > MaxSpaceCatalogSpaces {
		return nil, fmt.Errorf("%w: maximum spaces %d exceeded", ErrSpaceCatalogLimit, MaxSpaceCatalogSpaces)
	}
	catalog := &SpaceCatalog{spaces: make(map[string]SpaceDefinition, len(definitions))}
	for _, definition := range definitions {
		normalized, err := normalizeSpaceDefinition(definition)
		if err != nil {
			return nil, err
		}
		if _, exists := catalog.spaces[normalized.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate space %q", ErrSpaceCatalogInvalid, normalized.Name)
		}
		catalog.spaces[normalized.Name] = normalized
	}
	return catalog, nil
}

// Upsert validates and replaces one named-space definition.
func (catalog *SpaceCatalog) Upsert(definition SpaceDefinition) error {
	if catalog == nil {
		return ErrSpaceCatalogNil
	}
	normalized, err := normalizeSpaceDefinition(definition)
	if err != nil {
		return err
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if catalog.spaces == nil {
		catalog.spaces = make(map[string]SpaceDefinition)
	}
	if _, exists := catalog.spaces[normalized.Name]; !exists && len(catalog.spaces) >= MaxSpaceCatalogSpaces {
		return fmt.Errorf("%w: maximum spaces %d exceeded", ErrSpaceCatalogLimit, MaxSpaceCatalogSpaces)
	}
	catalog.spaces[normalized.Name] = normalized
	return nil
}

// Lookup returns an independent definition copy for name.
func (catalog *SpaceCatalog) Lookup(name string) (SpaceDefinition, bool) {
	if catalog == nil {
		return SpaceDefinition{}, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return SpaceDefinition{}, false
	}
	catalog.mu.RLock()
	definition, ok := catalog.spaces[name]
	catalog.mu.RUnlock()
	if !ok {
		return SpaceDefinition{}, false
	}
	return cloneSpaceDefinition(definition), true
}

// ConditionalIndexes returns clone-safe conditional index metadata for one
// named space. The boolean reports whether the space exists, so an existing
// space with no conditional indexes is distinguishable from an unknown space.
func (catalog *SpaceCatalog) ConditionalIndexes(name string) ([]IndexDefinition, bool) {
	if catalog == nil {
		return nil, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, false
	}
	catalog.mu.RLock()
	definition, ok := catalog.spaces[name]
	if !ok {
		catalog.mu.RUnlock()
		return nil, false
	}
	indexes := make([]IndexDefinition, 0, len(definition.Indexes))
	for _, index := range definition.Indexes {
		if index.Predicate != "" {
			indexes = append(indexes, cloneIndexDefinition(index))
		}
	}
	catalog.mu.RUnlock()
	return indexes, true
}

// List returns independent definitions sorted by normalized space name.
func (catalog *SpaceCatalog) List() []SpaceDefinition {
	if catalog == nil {
		return nil
	}
	catalog.mu.RLock()
	definitions := make([]SpaceDefinition, 0, len(catalog.spaces))
	for _, definition := range catalog.spaces {
		definitions = append(definitions, cloneSpaceDefinition(definition))
	}
	catalog.mu.RUnlock()
	sort.Slice(definitions, func(left, right int) bool { return definitions[left].Name < definitions[right].Name })
	return definitions
}

// Delete removes one named-space definition and reports whether it existed.
func (catalog *SpaceCatalog) Delete(name string) bool {
	if catalog == nil {
		return false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if _, ok := catalog.spaces[name]; !ok {
		return false
	}
	delete(catalog.spaces, name)
	return true
}

func normalizeSpaceDefinition(definition SpaceDefinition) (SpaceDefinition, error) {
	definition.Name = strings.TrimSpace(definition.Name)
	if definition.Name == "" {
		return SpaceDefinition{}, ErrSpaceCatalogNameRequired
	}
	if len(definition.Indexes) > MaxSpaceCatalogIndexes {
		return SpaceDefinition{}, fmt.Errorf("%w: space %q has more than %d indexes", ErrSpaceCatalogLimit, definition.Name, MaxSpaceCatalogIndexes)
	}

	source := cloneSource(definition.Source)
	source.Name = strings.TrimSpace(source.Name)
	if source.Name == "" {
		source.Name = definition.Name
	}
	if source.Name != definition.Name {
		return SpaceDefinition{}, fmt.Errorf("%w: space %q source name is %q", ErrSpaceCatalogInvalid, definition.Name, source.Name)
	}
	columnNames := make(map[string]struct{}, len(source.Columns))
	for index := range source.Columns {
		column := &source.Columns[index]
		column.Name = strings.TrimSpace(column.Name)
		column.Type = Type(strings.ToLower(strings.TrimSpace(string(column.Type))))
		if column.Name == "" || column.Type == "" {
			return SpaceDefinition{}, fmt.Errorf("%w: space %q has an invalid column", ErrSpaceCatalogInvalid, definition.Name)
		}
		if _, exists := columnNames[column.Name]; exists {
			return SpaceDefinition{}, fmt.Errorf("%w: space %q duplicates column %q", ErrSpaceCatalogInvalid, definition.Name, column.Name)
		}
		columnNames[column.Name] = struct{}{}
	}
	constraintNames := make(map[string]struct{}, len(source.Constraints))
	for index := range source.Constraints {
		constraint := &source.Constraints[index]
		*constraint = normalizeConstraint(*constraint)
		if constraint.Name == "" {
			return SpaceDefinition{}, fmt.Errorf("%w: space %q has an unnamed constraint", ErrSpaceCatalogInvalid, definition.Name)
		}
		if _, exists := constraintNames[constraint.Name]; exists {
			return SpaceDefinition{}, fmt.Errorf("%w: space %q duplicates constraint %q", ErrSpaceCatalogInvalid, definition.Name, constraint.Name)
		}
		constraintNames[constraint.Name] = struct{}{}
	}

	indexNames := make(map[string]struct{}, len(definition.Indexes))
	indexes := make([]IndexDefinition, len(definition.Indexes))
	for index, declared := range definition.Indexes {
		declared.Name = strings.TrimSpace(declared.Name)
		declared.Kind = IndexKind(strings.ToLower(strings.TrimSpace(string(declared.Kind))))
		declared.Predicate = strings.TrimSpace(declared.Predicate)
		declared.PredicateColumns = normalizeIndexColumns(declared.PredicateColumns)
		if declared.Name == "" || !validIndexKind(declared.Kind) {
			return SpaceDefinition{}, fmt.Errorf("%w: space %q has an invalid index", ErrSpaceCatalogInvalid, definition.Name)
		}
		if _, exists := indexNames[declared.Name]; exists {
			return SpaceDefinition{}, fmt.Errorf("%w: space %q duplicates index %q", ErrSpaceCatalogInvalid, definition.Name, declared.Name)
		}
		indexNames[declared.Name] = struct{}{}
		if declared.Kind == IndexKindFunctional {
			declared.Expression = strings.TrimSpace(declared.Expression)
			if declared.Expression == "" {
				return SpaceDefinition{}, fmt.Errorf("%w: functional index %q requires an expression", ErrSpaceCatalogInvalid, declared.Name)
			}
		}
		if declared.Predicate == "" && len(declared.PredicateColumns) > 0 {
			return SpaceDefinition{}, fmt.Errorf("%w: index %q predicate columns require a predicate", ErrSpaceCatalogInvalid, declared.Name)
		}
		if declared.Predicate != "" && len(declared.PredicateColumns) == 0 {
			return SpaceDefinition{}, fmt.Errorf("%w: conditional index %q requires predicate columns", ErrSpaceCatalogInvalid, declared.Name)
		}
		declared.Columns = normalizeIndexColumns(declared.Columns)
		if declared.Kind != IndexKindFunctional && len(declared.Columns) == 0 {
			return SpaceDefinition{}, fmt.Errorf("%w: index %q requires columns", ErrSpaceCatalogInvalid, declared.Name)
		}
		seenColumns := make(map[string]struct{}, len(declared.Columns))
		for _, column := range declared.Columns {
			if _, exists := columnNames[column]; !exists {
				return SpaceDefinition{}, fmt.Errorf("%w: index %q references unknown column %q", ErrSpaceCatalogInvalid, declared.Name, column)
			}
			if _, exists := seenColumns[column]; exists {
				return SpaceDefinition{}, fmt.Errorf("%w: index %q duplicates column %q", ErrSpaceCatalogInvalid, declared.Name, column)
			}
			seenColumns[column] = struct{}{}
		}
		seenPredicateColumns := make(map[string]struct{}, len(declared.PredicateColumns))
		for _, column := range declared.PredicateColumns {
			if _, exists := columnNames[column]; !exists {
				return SpaceDefinition{}, fmt.Errorf("%w: index %q predicate references unknown column %q", ErrSpaceCatalogInvalid, declared.Name, column)
			}
			if _, exists := seenPredicateColumns[column]; exists {
				return SpaceDefinition{}, fmt.Errorf("%w: index %q duplicates predicate column %q", ErrSpaceCatalogInvalid, declared.Name, column)
			}
			seenPredicateColumns[column] = struct{}{}
		}
		indexes[index] = declared
	}
	definition.Source = source
	definition.Indexes = indexes
	return definition, nil
}

func normalizeIndexColumns(columns []string) []string {
	if len(columns) == 0 {
		return nil
	}
	normalized := make([]string, len(columns))
	for index, column := range columns {
		normalized[index] = strings.TrimSpace(column)
	}
	return normalized
}

func validIndexKind(kind IndexKind) bool {
	switch kind {
	case IndexKindHash, IndexKindTree, IndexKindRTree, IndexKindFunctional:
		return true
	default:
		return false
	}
}

func cloneSpaceDefinition(definition SpaceDefinition) SpaceDefinition {
	definition.Source = cloneSource(definition.Source)
	indexes := make([]IndexDefinition, len(definition.Indexes))
	for index, declared := range definition.Indexes {
		indexes[index] = cloneIndexDefinition(declared)
	}
	definition.Indexes = indexes
	return definition
}

func cloneIndexDefinition(definition IndexDefinition) IndexDefinition {
	definition.Columns = append([]string(nil), definition.Columns...)
	definition.PredicateColumns = append([]string(nil), definition.PredicateColumns...)
	return definition
}
