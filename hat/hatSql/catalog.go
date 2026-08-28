package hatSql

import (
	"fmt"
	"strings"
)

// Catalog declares virtual information_schema metadata for one SQL resolver.
type Catalog struct {
	Namespaces []string
	Sources    []CatalogSource
	Indexes    []CatalogIndex
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
			return "", fmt.Errorf("SHOW expects one of NAMESPACES, SOURCES, or INDEXES")
		}
		switch strings.ToUpper(parts[1]) {
		case "NAMESPACES":
			return "FROM CACHE('information_schema.namespaces') SELECT namespace", nil
		case "SOURCES":
			return "FROM CACHE('information_schema.sources') SELECT namespace, name, kind", nil
		case "INDEXES":
			return "FROM CACHE('information_schema.indexes') SELECT namespace, source, name, kind, column, ordinal_position", nil
		default:
			return "", fmt.Errorf("SHOW expects one of NAMESPACES, SOURCES, or INDEXES")
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
		}
	}
	if resolver.Source == nil {
		return nil, nil
	}
	return resolver.Source.ResolveSQLSource(name, key)
}
