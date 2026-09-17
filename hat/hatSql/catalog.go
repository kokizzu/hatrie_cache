package hatSql

import (
	"fmt"
	"strings"
)

// Catalog declares virtual information_schema metadata for one SQL resolver.
type Catalog struct {
	Version      uint64
	Namespaces   []string
	Sources      []CatalogSource
	Indexes      []CatalogIndex
	Objects      []CatalogObject
	Dependencies []CatalogDependency
}

type CatalogSource struct {
	Namespace string
	Name      string
	Kind      string
	Fields    []CatalogField
}

type CatalogField struct {
	Name     string
	Type     string
	Nullable bool
}

type CatalogIndex struct {
	Namespace string
	Source    string
	Name      string
	Kind      string
	Columns   []string
}

// CatalogResolver exposes immutable information_schema virtual sources while
// delegating ordinary source resolution to Source.
type CatalogResolver struct {
	Source  SourceResolver
	Catalog Catalog
}

// CompileSQLShortcut lowers catalog shortcuts to ordinary information_schema
// queries. Non-shortcut SQL is returned unchanged.
func CompileSQLShortcut(source string) (string, error) {
	trimmed := strings.TrimSpace(source)
	parts := strings.Fields(trimmed)
	if len(parts) == 0 {
		return source, nil
	}
	switch strings.ToUpper(parts[0]) {
	case "SHOW":
		if len(parts) != 2 {
			return "", fmt.Errorf("SHOW expects one of NAMESPACES, SOURCES, INDEXES, OBJECTS, or DEPENDENCIES")
		}
		switch strings.ToUpper(parts[1]) {
		case "NAMESPACES":
			return "FROM CACHE('information_schema.namespaces') SELECT namespace", nil
		case "SOURCES":
			return "FROM CACHE('information_schema.sources') SELECT namespace, name, kind", nil
		case "INDEXES":
			return "FROM CACHE('information_schema.indexes') SELECT namespace, source, name, kind, column, ordinal_position", nil
		case "OBJECTS":
			return "FROM CACHE('information_schema.objects') SELECT catalog_version, namespace, name, kind, type, object_version, state ORDER BY namespace, name, kind", nil
		case "DEPENDENCIES":
			return "FROM CACHE('information_schema.dependencies') SELECT catalog_version, namespace, object, object_kind, depends_on_namespace, depends_on, depends_on_kind, ordinal_position ORDER BY namespace, object, object_kind", nil
		default:
			return "", fmt.Errorf("SHOW expects one of NAMESPACES, SOURCES, INDEXES, OBJECTS, or DEPENDENCIES")
		}
	case "DESCRIBE":
		if len(parts) != 2 || !catalogIdentifier(parts[1]) {
			return "", fmt.Errorf("DESCRIBE expects one simple source name")
		}
		return "FROM CACHE('information_schema.fields') WHERE source = '" + parts[1] + "' SELECT namespace, source, name, type, nullable, ordinal_position ORDER BY ordinal_position", nil
	}
	return source, nil
}

func catalogIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' || character == '.' || character == '-') {
			return false
		}
	}
	return true
}

func (resolver CatalogResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if strings.EqualFold(name, "CACHE") {
		switch strings.ToLower(key) {
		case "information_schema.namespaces":
			rows := make([]Row, len(resolver.Catalog.Namespaces))
			for index, namespace := range resolver.Catalog.Namespaces {
				rows[index] = Row{"namespace": namespace}
			}
			return rows, nil
		case "information_schema.sources":
			rows := make([]Row, len(resolver.Catalog.Sources))
			for index, source := range resolver.Catalog.Sources {
				rows[index] = Row{"namespace": source.Namespace, "name": source.Name, "kind": source.Kind}
			}
			return rows, nil
		case "information_schema.fields":
			rows := []Row{}
			for _, source := range resolver.Catalog.Sources {
				for position, field := range source.Fields {
					rows = append(rows, Row{"namespace": source.Namespace, "source": source.Name, "name": field.Name, "type": field.Type, "nullable": field.Nullable, "ordinal_position": int64(position + 1)})
				}
			}
			return rows, nil
		case "information_schema.indexes":
			rows := []Row{}
			for _, index := range resolver.Catalog.Indexes {
				for position, column := range index.Columns {
					rows = append(rows, Row{"namespace": index.Namespace, "source": index.Source, "name": index.Name, "kind": index.Kind, "column": column, "ordinal_position": int64(position + 1)})
				}
			}
			return rows, nil
		case "information_schema.objects":
			return catalogObjectRows(resolver.Catalog)
		case "information_schema.dependencies":
			return catalogDependencyRows(resolver.Catalog)
		}
	}
	if resolver.Source == nil {
		return nil, nil
	}
	return resolver.Source.ResolveSQLSource(name, key)
}

func catalogOwnsVirtualSource(name, key string) bool {
	if !strings.EqualFold(name, "CACHE") {
		return false
	}
	switch strings.ToLower(key) {
	case "information_schema.namespaces", "information_schema.sources", "information_schema.fields", "information_schema.indexes", "information_schema.objects", "information_schema.dependencies":
		return true
	default:
		return false
	}
}

// ResolveSQLColumnarSource forwards the optional columnar contract for
// application sources while keeping information-schema sources local.
func (resolver CatalogResolver) ResolveSQLColumnarSource(name, key string, fields []string) (ColumnarBatch, bool, error) {
	if catalogOwnsVirtualSource(name, key) || resolver.Source == nil {
		return ColumnarBatch{}, false, nil
	}
	columnar, ok := resolver.Source.(ColumnarSourceResolver)
	if !ok {
		return ColumnarBatch{}, false, nil
	}
	return columnar.ResolveSQLColumnarSource(name, key, fields)
}

// ResolveSQLColumnarMapSubcolumns forwards the optional map-subcolumn
// contract for application sources while keeping information-schema sources
// local.
func (resolver CatalogResolver) ResolveSQLColumnarMapSubcolumns(name, key string, fields []string, paths []ColumnarMapSubcolumn) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	if catalogOwnsVirtualSource(name, key) || resolver.Source == nil {
		return ColumnarBatch{}, nil, false, nil
	}
	mapSource, ok := resolver.Source.(ColumnarMapSubcolumnSourceResolver)
	if !ok {
		return ColumnarBatch{}, nil, false, nil
	}
	return mapSource.ResolveSQLColumnarMapSubcolumns(name, key, fields, paths)
}

// BorrowSQLColumnarSource forwards the optional immutable columnar contract
// for application sources while keeping information-schema sources local.
func (resolver CatalogResolver) BorrowSQLColumnarSource(name, key string, fields []string) (ColumnarBatch, bool, error) {
	if catalogOwnsVirtualSource(name, key) || resolver.Source == nil {
		return ColumnarBatch{}, false, nil
	}
	borrowed, ok := resolver.Source.(BorrowedColumnarSourceResolver)
	if !ok {
		return ColumnarBatch{}, false, nil
	}
	return borrowed.BorrowSQLColumnarSource(name, key, fields)
}

// BorrowSQLColumnarSourceSegments forwards the optional immutable segmented
// columnar contract for application sources while keeping virtual sources local.
func (resolver CatalogResolver) BorrowSQLColumnarSourceSegments(name, key string, fields []string) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	if catalogOwnsVirtualSource(name, key) || resolver.Source == nil {
		return ColumnarBatch{}, nil, false, nil
	}
	segmented, ok := resolver.Source.(SegmentedColumnarSourceResolver)
	if !ok {
		return ColumnarBatch{}, nil, false, nil
	}
	return segmented.BorrowSQLColumnarSourceSegments(name, key, fields)
}

// BorrowSQLColumnarSourceOrder forwards the optional sorted ordinal contract
// for application sources while keeping virtual sources local.
func (resolver CatalogResolver) BorrowSQLColumnarSourceOrder(name, key string, fields []string, orderField string) ([]uint32, bool, error) {
	if catalogOwnsVirtualSource(name, key) || resolver.Source == nil {
		return nil, false, nil
	}
	sorted, ok := resolver.Source.(SortedColumnarSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return sorted.BorrowSQLColumnarSourceOrder(name, key, fields, orderField)
}

// BorrowSQLColumnarSourceOrderFields forwards the optional composite sorted
// ordinal contract for application sources while keeping virtual sources local.
func (resolver CatalogResolver) BorrowSQLColumnarSourceOrderFields(name, key string, fields, orderFields []string) ([]uint32, bool, error) {
	if catalogOwnsVirtualSource(name, key) || resolver.Source == nil {
		return nil, false, nil
	}
	sorted, ok := resolver.Source.(CompositeSortedColumnarSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return sorted.BorrowSQLColumnarSourceOrderFields(name, key, fields, orderFields)
}

// BorrowSQLColumnarSourceOrderBy forwards the optional directed sorted ordinal
// contract for application sources while keeping virtual sources local.
func (resolver CatalogResolver) BorrowSQLColumnarSourceOrderBy(name, key string, fields, orderFields []string, descending []bool) ([]uint32, bool, error) {
	if catalogOwnsVirtualSource(name, key) || resolver.Source == nil {
		return nil, false, nil
	}
	sorted, ok := resolver.Source.(DirectedCompositeSortedColumnarSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return sorted.BorrowSQLColumnarSourceOrderBy(name, key, fields, orderFields, descending)
}

// PreferSQLColumnarSource forwards the optional columnar preference contract
// for application sources while keeping virtual sources local.
func (resolver CatalogResolver) PreferSQLColumnarSource(name, key string, fields []string) bool {
	if catalogOwnsVirtualSource(name, key) || resolver.Source == nil {
		return false
	}
	preferred, ok := resolver.Source.(ColumnarSourcePreferenceResolver)
	return ok && preferred.PreferSQLColumnarSource(name, key, fields)
}

// SQLSourceCardinality forwards optional source row-count metadata. Catalog
// pseudo-sources intentionally remain unavailable so the SQL planner falls
// back to its established exact source resolution path for mixed queries.
func (resolver CatalogResolver) SQLSourceCardinality(name, key string) (int, bool, bool, error) {
	if resolver.Source == nil {
		return 0, false, false, nil
	}
	cardinality, ok := resolver.Source.(SourceCardinalityResolver)
	if !ok {
		return 0, false, false, nil
	}
	return cardinality.SQLSourceCardinality(name, key)
}

// ResolveSQLSourcePartitions forwards partitioned application sources while
// leaving information-schema sources owned by the catalog resolver.
func (resolver CatalogResolver) ResolveSQLSourcePartitions(name, key string) ([]SQLSourcePartition, bool, error) {
	if strings.EqualFold(name, "CACHE") {
		switch strings.ToLower(key) {
		case "information_schema.namespaces", "information_schema.sources", "information_schema.fields", "information_schema.indexes", "information_schema.objects", "information_schema.dependencies":
			return nil, false, nil
		}
	}
	if resolver.Source == nil {
		return nil, false, nil
	}
	partitioned, ok := resolver.Source.(PartitionedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return partitioned.ResolveSQLSourcePartitions(name, key)
}

// ResolveSQLIndexDiagnostics forwards optional application index diagnostics
// while leaving information-schema sources owned by the catalog resolver.
func (resolver CatalogResolver) ResolveSQLIndexDiagnostics(name, key, field string, value interface{}) (SQLIndexDiagnostics, bool, error) {
	if strings.EqualFold(name, "CACHE") {
		switch strings.ToLower(key) {
		case "information_schema.namespaces", "information_schema.sources", "information_schema.fields", "information_schema.indexes", "information_schema.objects", "information_schema.dependencies":
			return SQLIndexDiagnostics{}, false, nil
		}
	}
	if resolver.Source == nil {
		return SQLIndexDiagnostics{}, false, nil
	}
	diagnostics, ok := resolver.Source.(SQLIndexDiagnosticsResolver)
	if !ok {
		return SQLIndexDiagnostics{}, false, nil
	}
	return diagnostics.ResolveSQLIndexDiagnostics(name, key, field, value)
}

// ResolveSQLArrangementMetadata forwards optional application arrangement
// metadata while leaving information-schema sources owned by the catalog
// resolver.
func (resolver CatalogResolver) ResolveSQLArrangementMetadata(name, key string) ([]SQLArrangementMetadata, error) {
	if catalogOwnsVirtualSource(name, key) || resolver.Source == nil {
		return nil, nil
	}
	arrangements, ok := resolver.Source.(SQLArrangementMetadataResolver)
	if !ok {
		return nil, nil
	}
	return arrangements.ResolveSQLArrangementMetadata(name, key)
}

// ResolveSQLOrderedSourcePartitions forwards ordered application partitions
// while leaving information-schema sources owned by the catalog resolver.
func (resolver CatalogResolver) ResolveSQLOrderedSourcePartitions(name, key, field string, desc, nullsFirst, nullsLast bool) ([]SQLSourcePartition, bool, error) {
	if strings.EqualFold(name, "CACHE") {
		switch strings.ToLower(key) {
		case "information_schema.namespaces", "information_schema.sources", "information_schema.fields", "information_schema.indexes", "information_schema.objects", "information_schema.dependencies":
			return nil, false, nil
		}
	}
	if resolver.Source == nil {
		return nil, false, nil
	}
	ordered, ok := resolver.Source.(PartitionedOrderedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return ordered.ResolveSQLOrderedSourcePartitions(name, key, field, desc, nullsFirst, nullsLast)
}

// ResolveSQLSourcePartitionsForPredicate forwards predicate pruning for
// application sources while leaving information-schema sources to the
// catalog resolver.
func (resolver CatalogResolver) ResolveSQLSourcePartitionsForPredicate(name, key string, predicate SQLPartitionPredicate) ([]SQLSourcePartition, bool, error) {
	if strings.EqualFold(name, "CACHE") {
		switch strings.ToLower(key) {
		case "information_schema.namespaces", "information_schema.sources", "information_schema.fields", "information_schema.indexes":
			return nil, false, nil
		}
	}
	if resolver.Source == nil {
		return nil, false, nil
	}
	pruning, ok := resolver.Source.(PartitionPruningSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return pruning.ResolveSQLSourcePartitionsForPredicate(name, key, predicate)
}
