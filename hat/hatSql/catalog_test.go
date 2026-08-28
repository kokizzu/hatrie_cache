package hatSql

import (
	"context"
	"strings"
	"testing"
)

func TestCatalogResolverExposesInformationSchemaSources(t *testing.T) {
	resolver := CatalogResolver{Catalog: Catalog{
		Namespaces: []string{"public"},
		Sources:    []CatalogSource{{Namespace: "public", Name: "people", Kind: "CACHE", Fields: []CatalogField{{Name: "id", Type: "INTEGER"}}}},
		Indexes:    []CatalogIndex{{Namespace: "public", Source: "people", Name: "people_id", Kind: "hash", Columns: []string{"id"}}},
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), `FROM CACHE('information_schema.fields') WHERE source = 'people' SELECT name, type`, resolver, nil, SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "id" || result.Rows[0]["type"] != "INTEGER" {
		t.Fatalf("catalog query = %#v, %v", result, err)
	}
}

func TestCatalogResolverCompilesAndExecutesShortcuts(t *testing.T) {
	resolver := CatalogResolver{Catalog: Catalog{Sources: []CatalogSource{{Namespace: "public", Name: "people", Kind: "CACHE", Fields: []CatalogField{{Name: "id", Type: "INTEGER"}}}}}}
	compiled, err := CompileSQLShortcut("DESCRIBE people")
	if err != nil || !strings.Contains(compiled, "information_schema.fields") || !strings.Contains(compiled, "source = 'people'") {
		t.Fatalf("CompileSQLShortcut() = %q, %v", compiled, err)
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SHOW SOURCES", resolver, nil, SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "people" {
		t.Fatalf("SHOW SOURCES = %#v, %v", result, err)
	}
}
