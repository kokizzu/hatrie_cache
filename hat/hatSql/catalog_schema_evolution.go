package hatSql

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

var (
	ErrCatalogSchemaInvalid            = errors.New("hatSql: catalog schema is invalid")
	ErrCatalogSchemaIncompatible       = errors.New("hatSql: catalog schema evolution is incompatible")
	ErrCatalogSchemaDependency         = errors.New("hatSql: catalog schema evolution has dependent objects")
	ErrCatalogSchemaStale              = errors.New("hatSql: catalog schema evolution plan is stale")
	ErrCatalogSchemaGenerationConflict = errors.New("hatSql: catalog schema generation conflict")
	ErrCatalogSchemaValueIncompatible  = errors.New("hatSql: catalog schema value cannot be projected")
)

// CatalogSchemaEvolutionOptions controls compatibility checks for a catalog
// replacement. All options are opt-in so an omitted policy cannot silently
// discard existing data or alter a typed value.
type CatalogSchemaEvolutionOptions struct {
	AllowFieldRemoval    bool
	AllowNumericWidening bool
}

// CatalogSchemaChangeKind identifies one deterministic catalog change.
type CatalogSchemaChangeKind uint8

const (
	CatalogSchemaSourceAdded CatalogSchemaChangeKind = iota
	CatalogSchemaSourceRemoved
	CatalogSchemaSourceKindChanged
	CatalogSchemaFieldAdded
	CatalogSchemaFieldRemoved
	CatalogSchemaFieldTypeChanged
	CatalogSchemaFieldNullabilityChanged
	CatalogSchemaIndexAdded
	CatalogSchemaIndexRemoved
)

// CatalogSchemaChange describes one source, field, or index change.
type CatalogSchemaChange struct {
	Kind                     CatalogSchemaChangeKind
	Namespace                string
	Source                   string
	Field                    string
	Index                    string
	FromType, ToType         string
	FromNullable, ToNullable bool
}

// CatalogSchemaEvolutionPlan is an immutable, deterministic replacement
// proposal. Before and After are normalized copies and can be published by a
// caller after the plan has passed its compatibility checks.
type CatalogSchemaEvolutionPlan struct {
	Before          Catalog
	After           Catalog
	FromFingerprint [32]byte
	ToFingerprint   [32]byte
	Changes         []CatalogSchemaChange
}

// PlanCatalogSchemaEvolution validates and compares two catalog versions.
// Existing index dependencies are checked before the target catalog is
// accepted, so dropping an indexed field reports a dependency error rather
// than leaving a partially valid catalog.
func PlanCatalogSchemaEvolution(before, after Catalog, options CatalogSchemaEvolutionOptions) (CatalogSchemaEvolutionPlan, error) {
	if err := validateCatalogSchema(before); err != nil {
		return CatalogSchemaEvolutionPlan{}, err
	}
	if err := catalogSchemaDependencyError(before, after); err != nil {
		return CatalogSchemaEvolutionPlan{}, err
	}
	normalizedBefore, err := normalizeCatalogSchema(before)
	if err != nil {
		return CatalogSchemaEvolutionPlan{}, err
	}
	normalizedAfter, err := normalizeCatalogSchema(after)
	if err != nil {
		return CatalogSchemaEvolutionPlan{}, err
	}
	plan := CatalogSchemaEvolutionPlan{
		Before:          normalizedBefore,
		After:           normalizedAfter,
		FromFingerprint: fingerprintCatalogSchema(normalizedBefore),
		ToFingerprint:   fingerprintCatalogSchema(normalizedAfter),
	}
	plan.Changes = catalogSchemaChanges(normalizedBefore, normalizedAfter)
	if err := validateCatalogSchemaChanges(normalizedBefore, normalizedAfter, plan.Changes, options); err != nil {
		return CatalogSchemaEvolutionPlan{}, err
	}
	return plan, nil
}

// ProjectRows maps rows from the Before source shape to the After source
// shape. Added nullable fields become nil, removed fields are omitted, and an
// explicitly allowed integer-to-number widening converts scalar values.
func (plan CatalogSchemaEvolutionPlan) ProjectRows(namespace, source string, rows []Row) ([]Row, error) {
	before, err := normalizeCatalogSchema(plan.Before)
	if err != nil {
		return nil, ErrCatalogSchemaStale
	}
	after, err := normalizeCatalogSchema(plan.After)
	if err != nil || fingerprintCatalogSchema(before) != plan.FromFingerprint || fingerprintCatalogSchema(after) != plan.ToFingerprint {
		return nil, ErrCatalogSchemaStale
	}
	beforeSource, beforeOK := findCatalogSource(before, namespace, source)
	afterSource, afterOK := findCatalogSource(after, namespace, source)
	if !beforeOK || !afterOK {
		return nil, ErrCatalogSchemaIncompatible
	}
	beforeFields := catalogFieldsByName(beforeSource.Fields)
	projected := make([]Row, len(rows))
	for rowIndex, row := range rows {
		output := make(Row, len(afterSource.Fields))
		for _, field := range afterSource.Fields {
			value, present := row[field.Name]
			if !present {
				output[field.Name] = nil
				continue
			}
			previous, existed := beforeFields[field.Name]
			if !existed {
				return nil, fmt.Errorf("%w: row %d field %q", ErrCatalogSchemaValueIncompatible, rowIndex+1, field.Name)
			}
			if previous.Type != field.Type && value != nil {
				value, err = widenCatalogSchemaValue(value, previous.Type, field.Type)
				if err != nil {
					return nil, fmt.Errorf("%w: row %d field %q: %v", ErrCatalogSchemaValueIncompatible, rowIndex+1, field.Name, err)
				}
			}
			output[field.Name] = value
		}
		projected[rowIndex] = output
	}
	return projected, nil
}

// CatalogSchemaRegistry publishes one catalog generation atomically. It does
// not retain query locks or perform source I/O; callers obtain a snapshot and
// build a CatalogResolver for each desired read boundary.
type CatalogSchemaRegistry struct {
	mu         sync.RWMutex
	catalog    Catalog
	generation uint64
}

// NewCatalogSchemaRegistry validates and installs an initial catalog at
// generation one.
func NewCatalogSchemaRegistry(catalog Catalog) (*CatalogSchemaRegistry, error) {
	normalized, err := normalizeCatalogSchema(catalog)
	if err != nil {
		return nil, err
	}
	return &CatalogSchemaRegistry{catalog: normalized, generation: 1}, nil
}

// Snapshot returns an independent catalog copy and its generation.
func (registry *CatalogSchemaRegistry) Snapshot() (Catalog, uint64) {
	if registry == nil {
		return Catalog{}, 0
	}
	registry.mu.RLock()
	catalog := cloneCatalogSchema(registry.catalog)
	generation := registry.generation
	registry.mu.RUnlock()
	return catalog, generation
}

// Resolver returns a query resolver over one atomically captured catalog.
func (registry *CatalogSchemaRegistry) Resolver(source SourceResolver) CatalogResolver {
	catalog, _ := registry.Snapshot()
	return CatalogResolver{Source: source, Catalog: catalog}
}

// Replace validates and publishes next as one new generation. A failed plan
// leaves both catalog and generation unchanged.
func (registry *CatalogSchemaRegistry) Replace(next Catalog, options CatalogSchemaEvolutionOptions) (CatalogSchemaEvolutionPlan, uint64, error) {
	if registry == nil {
		return CatalogSchemaEvolutionPlan{}, 0, ErrCatalogSchemaInvalid
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	plan, err := PlanCatalogSchemaEvolution(registry.catalog, next, options)
	if err != nil {
		return CatalogSchemaEvolutionPlan{}, registry.generation, err
	}
	if plan.FromFingerprint == plan.ToFingerprint {
		return plan, registry.generation, nil
	}
	registry.catalog = cloneCatalogSchema(plan.After)
	registry.generation++
	return plan, registry.generation, nil
}

// Rollback publishes previous only when expectedGeneration is still current.
// It uses the same compatibility and dependency checks as a forward replace.
func (registry *CatalogSchemaRegistry) Rollback(expectedGeneration uint64, previous Catalog, options CatalogSchemaEvolutionOptions) (uint64, error) {
	if registry == nil {
		return 0, ErrCatalogSchemaInvalid
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.generation != expectedGeneration {
		return registry.generation, ErrCatalogSchemaGenerationConflict
	}
	plan, err := PlanCatalogSchemaEvolution(registry.catalog, previous, options)
	if err != nil {
		return registry.generation, err
	}
	if plan.FromFingerprint != plan.ToFingerprint {
		registry.catalog = cloneCatalogSchema(plan.After)
		registry.generation++
	}
	return registry.generation, nil
}

func validateCatalogSchema(catalog Catalog) error {
	_, err := normalizeCatalogSchema(catalog)
	return err
}

func normalizeCatalogSchema(catalog Catalog) (Catalog, error) {
	normalized := cloneCatalogSchema(catalog)
	sort.Strings(normalized.Namespaces)
	for index := 1; index < len(normalized.Namespaces); index++ {
		if normalized.Namespaces[index] == normalized.Namespaces[index-1] || normalized.Namespaces[index] == "" {
			return Catalog{}, ErrCatalogSchemaInvalid
		}
	}
	if len(normalized.Namespaces) > 0 && normalized.Namespaces[0] == "" {
		return Catalog{}, ErrCatalogSchemaInvalid
	}
	sources := make(map[string]struct{}, len(normalized.Sources))
	for sourceIndex := range normalized.Sources {
		source := &normalized.Sources[sourceIndex]
		if source.Namespace == "" || source.Name == "" {
			return Catalog{}, ErrCatalogSchemaInvalid
		}
		key := catalogSchemaSourceKey(source.Namespace, source.Name)
		if _, exists := sources[key]; exists {
			return Catalog{}, ErrCatalogSchemaInvalid
		}
		sources[key] = struct{}{}
		fields := make(map[string]struct{}, len(source.Fields))
		for fieldIndex := range source.Fields {
			field := &source.Fields[fieldIndex]
			if field.Name == "" || field.Type == "" {
				return Catalog{}, ErrCatalogSchemaInvalid
			}
			if _, exists := fields[field.Name]; exists {
				return Catalog{}, ErrCatalogSchemaInvalid
			}
			fields[field.Name] = struct{}{}
		}
		sort.Slice(source.Fields, func(i, j int) bool { return source.Fields[i].Name < source.Fields[j].Name })
	}
	sort.Slice(normalized.Sources, func(i, j int) bool {
		return catalogSchemaSourceKey(normalized.Sources[i].Namespace, normalized.Sources[i].Name) < catalogSchemaSourceKey(normalized.Sources[j].Namespace, normalized.Sources[j].Name)
	})
	indexes := make(map[string]struct{}, len(normalized.Indexes))
	for index := range normalized.Indexes {
		catalogIndex := &normalized.Indexes[index]
		if catalogIndex.Namespace == "" || catalogIndex.Source == "" || catalogIndex.Name == "" || len(catalogIndex.Columns) == 0 {
			return Catalog{}, ErrCatalogSchemaInvalid
		}
		indexKey := catalogSchemaIndexKey(catalogIndex.Namespace, catalogIndex.Source, catalogIndex.Name)
		if _, exists := indexes[indexKey]; exists {
			return Catalog{}, ErrCatalogSchemaInvalid
		}
		indexes[indexKey] = struct{}{}
		source, exists := findCatalogSource(normalized, catalogIndex.Namespace, catalogIndex.Source)
		if !exists {
			return Catalog{}, ErrCatalogSchemaInvalid
		}
		fields := catalogFieldsByName(source.Fields)
		seenColumns := make(map[string]struct{}, len(catalogIndex.Columns))
		for _, column := range catalogIndex.Columns {
			if column == "" {
				return Catalog{}, ErrCatalogSchemaInvalid
			}
			if _, duplicate := seenColumns[column]; duplicate {
				return Catalog{}, ErrCatalogSchemaInvalid
			}
			seenColumns[column] = struct{}{}
			if _, exists := fields[column]; !exists {
				return Catalog{}, ErrCatalogSchemaInvalid
			}
		}
		sort.Strings(catalogIndex.Columns)
	}
	sort.Slice(normalized.Indexes, func(i, j int) bool {
		return catalogSchemaIndexKey(normalized.Indexes[i].Namespace, normalized.Indexes[i].Source, normalized.Indexes[i].Name) < catalogSchemaIndexKey(normalized.Indexes[j].Namespace, normalized.Indexes[j].Source, normalized.Indexes[j].Name)
	})
	return normalized, nil
}

func catalogSchemaDependencyError(before, after Catalog) error {
	beforeSources := catalogSourceMap(before.Sources)
	afterSources := catalogSourceMap(after.Sources)
	afterIndexes := catalogIndexMap(after.Indexes)
	for sourceKey, previous := range beforeSources {
		next, sourceExists := afterSources[sourceKey]
		if !sourceExists {
			for _, index := range before.Indexes {
				if catalogSchemaSourceKey(index.Namespace, index.Source) == sourceKey {
					if _, retained := afterIndexes[catalogSchemaIndexKey(index.Namespace, index.Source, index.Name)]; retained {
						return ErrCatalogSchemaDependency
					}
				}
			}
			continue
		}
		nextFields := catalogFieldsByName(next.Fields)
		for _, field := range previous.Fields {
			if _, retained := nextFields[field.Name]; retained {
				continue
			}
			for _, index := range before.Indexes {
				if catalogSchemaSourceKey(index.Namespace, index.Source) != sourceKey {
					continue
				}
				retainedIndex, retained := afterIndexes[catalogSchemaIndexKey(index.Namespace, index.Source, index.Name)]
				if retained && containsCatalogSchemaString(retainedIndex.Columns, field.Name) {
					return ErrCatalogSchemaDependency
				}
			}
		}
	}
	return nil
}

func catalogSchemaChanges(before, after Catalog) []CatalogSchemaChange {
	changes := make([]CatalogSchemaChange, 0)
	beforeSources := catalogSourceMap(before.Sources)
	afterSources := catalogSourceMap(after.Sources)
	for _, key := range sortedCatalogSchemaMapKeys(afterSources) {
		if _, exists := beforeSources[key]; !exists {
			source := afterSources[key]
			changes = append(changes, CatalogSchemaChange{Kind: CatalogSchemaSourceAdded, Namespace: source.Namespace, Source: source.Name})
		}
	}
	for _, key := range sortedCatalogSchemaMapKeys(beforeSources) {
		if _, exists := afterSources[key]; !exists {
			source := beforeSources[key]
			changes = append(changes, CatalogSchemaChange{Kind: CatalogSchemaSourceRemoved, Namespace: source.Namespace, Source: source.Name})
			continue
		}
		previous, next := beforeSources[key], afterSources[key]
		if previous.Kind != next.Kind {
			changes = append(changes, CatalogSchemaChange{Kind: CatalogSchemaSourceKindChanged, Namespace: next.Namespace, Source: next.Name, FromType: previous.Kind, ToType: next.Kind})
		}
		beforeFields, afterFields := catalogFieldsByName(previous.Fields), catalogFieldsByName(next.Fields)
		for _, fieldName := range sortedCatalogSchemaMapKeys(afterFields) {
			if _, exists := beforeFields[fieldName]; !exists {
				field := afterFields[fieldName]
				changes = append(changes, CatalogSchemaChange{Kind: CatalogSchemaFieldAdded, Namespace: next.Namespace, Source: next.Name, Field: field.Name, ToType: field.Type, ToNullable: field.Nullable})
			}
		}
		for _, fieldName := range sortedCatalogSchemaMapKeys(beforeFields) {
			previousField, exists := beforeFields[fieldName]
			if !exists {
				continue
			}
			nextField, retained := afterFields[fieldName]
			if !retained {
				changes = append(changes, CatalogSchemaChange{Kind: CatalogSchemaFieldRemoved, Namespace: previous.Namespace, Source: next.Name, Field: previousField.Name, FromType: previousField.Type, FromNullable: previousField.Nullable})
				continue
			}
			if previousField.Type != nextField.Type {
				changes = append(changes, CatalogSchemaChange{Kind: CatalogSchemaFieldTypeChanged, Namespace: next.Namespace, Source: next.Name, Field: nextField.Name, FromType: previousField.Type, ToType: nextField.Type, FromNullable: previousField.Nullable, ToNullable: nextField.Nullable})
			}
			if previousField.Nullable != nextField.Nullable {
				changes = append(changes, CatalogSchemaChange{Kind: CatalogSchemaFieldNullabilityChanged, Namespace: next.Namespace, Source: next.Name, Field: nextField.Name, FromType: previousField.Type, ToType: nextField.Type, FromNullable: previousField.Nullable, ToNullable: nextField.Nullable})
			}
		}
	}
	beforeIndexes, afterIndexes := catalogIndexMap(before.Indexes), catalogIndexMap(after.Indexes)
	for _, key := range sortedCatalogSchemaMapKeys(afterIndexes) {
		if _, exists := beforeIndexes[key]; !exists {
			index := afterIndexes[key]
			changes = append(changes, CatalogSchemaChange{Kind: CatalogSchemaIndexAdded, Namespace: index.Namespace, Source: index.Source, Index: index.Name})
		}
	}
	for _, key := range sortedCatalogSchemaMapKeys(beforeIndexes) {
		if _, exists := afterIndexes[key]; !exists {
			index := beforeIndexes[key]
			changes = append(changes, CatalogSchemaChange{Kind: CatalogSchemaIndexRemoved, Namespace: index.Namespace, Source: index.Source, Index: index.Name})
		}
	}
	return changes
}

func validateCatalogSchemaChanges(before, after Catalog, changes []CatalogSchemaChange, options CatalogSchemaEvolutionOptions) error {
	for _, change := range changes {
		switch change.Kind {
		case CatalogSchemaSourceKindChanged:
			return ErrCatalogSchemaIncompatible
		case CatalogSchemaSourceRemoved, CatalogSchemaFieldRemoved:
			if !options.AllowFieldRemoval {
				return ErrCatalogSchemaIncompatible
			}
		case CatalogSchemaFieldAdded:
			if !change.ToNullable {
				return ErrCatalogSchemaIncompatible
			}
		case CatalogSchemaFieldTypeChanged:
			if !options.AllowNumericWidening || !isNumericCatalogSchemaWidening(change.FromType, change.ToType) {
				return ErrCatalogSchemaIncompatible
			}
		case CatalogSchemaFieldNullabilityChanged:
			if change.FromNullable && !change.ToNullable {
				return ErrCatalogSchemaIncompatible
			}
		}
	}
	_ = before
	_ = after
	return nil
}

func widenCatalogSchemaValue(value interface{}, fromType, toType string) (interface{}, error) {
	if !isNumericCatalogSchemaWidening(fromType, toType) {
		return nil, ErrCatalogSchemaValueIncompatible
	}
	switch typed := value.(type) {
	case int:
		converted := float64(typed)
		if int(converted) != typed {
			return nil, ErrCatalogSchemaValueIncompatible
		}
		return converted, nil
	case int8:
		converted := float64(typed)
		if int8(converted) != typed {
			return nil, ErrCatalogSchemaValueIncompatible
		}
		return converted, nil
	case int16:
		converted := float64(typed)
		if int16(converted) != typed {
			return nil, ErrCatalogSchemaValueIncompatible
		}
		return converted, nil
	case int32:
		converted := float64(typed)
		if int32(converted) != typed {
			return nil, ErrCatalogSchemaValueIncompatible
		}
		return converted, nil
	case int64:
		converted := float64(typed)
		if int64(converted) != typed {
			return nil, ErrCatalogSchemaValueIncompatible
		}
		return converted, nil
	case uint:
		converted := float64(typed)
		if uint(converted) != typed {
			return nil, ErrCatalogSchemaValueIncompatible
		}
		return converted, nil
	case uint8:
		converted := float64(typed)
		if uint8(converted) != typed {
			return nil, ErrCatalogSchemaValueIncompatible
		}
		return converted, nil
	case uint16:
		converted := float64(typed)
		if uint16(converted) != typed {
			return nil, ErrCatalogSchemaValueIncompatible
		}
		return converted, nil
	case uint32:
		converted := float64(typed)
		if uint32(converted) != typed {
			return nil, ErrCatalogSchemaValueIncompatible
		}
		return converted, nil
	case uint64:
		if typed > math.MaxInt64 {
			return nil, ErrCatalogSchemaValueIncompatible
		}
		converted := float64(typed)
		if uint64(converted) != typed {
			return nil, ErrCatalogSchemaValueIncompatible
		}
		return converted, nil
	default:
		return nil, ErrCatalogSchemaValueIncompatible
	}
}

func isNumericCatalogSchemaWidening(fromType, toType string) bool {
	fromType, toType = strings.ToLower(strings.TrimSpace(fromType)), strings.ToLower(strings.TrimSpace(toType))
	return (fromType == "int" || fromType == "integer" || fromType == "int64") && (toType == "number" || toType == "float64" || toType == "double")
}

func cloneCatalogSchema(catalog Catalog) Catalog {
	clone := Catalog{Namespaces: append([]string(nil), catalog.Namespaces...)}
	clone.Sources = make([]CatalogSource, len(catalog.Sources))
	for index, source := range catalog.Sources {
		clone.Sources[index] = CatalogSource{Namespace: source.Namespace, Name: source.Name, Kind: source.Kind, Fields: append([]CatalogField(nil), source.Fields...)}
	}
	clone.Indexes = make([]CatalogIndex, len(catalog.Indexes))
	for index, catalogIndex := range catalog.Indexes {
		clone.Indexes[index] = CatalogIndex{Namespace: catalogIndex.Namespace, Source: catalogIndex.Source, Name: catalogIndex.Name, Kind: catalogIndex.Kind, Columns: append([]string(nil), catalogIndex.Columns...)}
	}
	return clone
}

func fingerprintCatalogSchema(catalog Catalog) [32]byte {
	hash := sha256.New()
	writeCatalogSchemaString := func(value string) {
		var length [binary.MaxVarintLen64]byte
		size := binary.PutUvarint(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:size])
		_, _ = hash.Write([]byte(value))
	}
	for _, namespace := range catalog.Namespaces {
		writeCatalogSchemaString("namespace")
		writeCatalogSchemaString(namespace)
	}
	for _, source := range catalog.Sources {
		writeCatalogSchemaString("source")
		writeCatalogSchemaString(source.Namespace)
		writeCatalogSchemaString(source.Name)
		writeCatalogSchemaString(source.Kind)
		for _, field := range source.Fields {
			writeCatalogSchemaString("field")
			writeCatalogSchemaString(field.Name)
			writeCatalogSchemaString(field.Type)
			if field.Nullable {
				writeCatalogSchemaString("nullable")
			} else {
				writeCatalogSchemaString("required")
			}
		}
	}
	for _, index := range catalog.Indexes {
		writeCatalogSchemaString("index")
		writeCatalogSchemaString(index.Namespace)
		writeCatalogSchemaString(index.Source)
		writeCatalogSchemaString(index.Name)
		writeCatalogSchemaString(index.Kind)
		for _, column := range index.Columns {
			writeCatalogSchemaString(column)
		}
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func catalogSourceMap(sources []CatalogSource) map[string]CatalogSource {
	result := make(map[string]CatalogSource, len(sources))
	for _, source := range sources {
		result[catalogSchemaSourceKey(source.Namespace, source.Name)] = source
	}
	return result
}

func catalogIndexMap(indexes []CatalogIndex) map[string]CatalogIndex {
	result := make(map[string]CatalogIndex, len(indexes))
	for _, index := range indexes {
		result[catalogSchemaIndexKey(index.Namespace, index.Source, index.Name)] = index
	}
	return result
}

func catalogFieldsByName(fields []CatalogField) map[string]CatalogField {
	result := make(map[string]CatalogField, len(fields))
	for _, field := range fields {
		result[field.Name] = field
	}
	return result
}

func findCatalogSource(catalog Catalog, namespace, name string) (CatalogSource, bool) {
	for _, source := range catalog.Sources {
		if source.Namespace == namespace && source.Name == name {
			return source, true
		}
	}
	return CatalogSource{}, false
}

func sortedCatalogSchemaMapKeys[Value any](values map[string]Value) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func catalogSchemaSourceKey(namespace, source string) string {
	return namespace + "\x00" + source
}

func catalogSchemaIndexKey(namespace, source, index string) string {
	return namespace + "\x00" + source + "\x00" + index
}

func containsCatalogSchemaString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
