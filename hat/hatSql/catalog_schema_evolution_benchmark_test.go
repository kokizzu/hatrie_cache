package hatSql_test

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkCatalogSchemaEvolutionControl(b *testing.B) {
	catalog := catalogSchemaEvolutionBenchmarkCatalog(false)
	resolver := hatSql.CatalogResolver{Catalog: catalog}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := resolver.ResolveSQLSource("CACHE", "information_schema.fields"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCatalogSchemaEvolutionPlan(b *testing.B) {
	before := catalogSchemaEvolutionBenchmarkCatalog(false)
	after := catalogSchemaEvolutionBenchmarkCatalog(true)
	options := hatSql.CatalogSchemaEvolutionOptions{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := hatSql.PlanCatalogSchemaEvolution(before, after, options); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCatalogSchemaEvolutionProjectRows(b *testing.B) {
	before := catalogSchemaEvolutionBenchmarkCatalog(false)
	after := catalogSchemaEvolutionBenchmarkCatalog(true)
	plan, err := hatSql.PlanCatalogSchemaEvolution(before, after, hatSql.CatalogSchemaEvolutionOptions{})
	if err != nil {
		b.Fatal(err)
	}
	rows := make([]hatSql.Row, 128)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":     int64(index),
			"name":   fmt.Sprintf("name-%d", index),
			"amount": float64(index) / 10,
			"active": index%2 == 0,
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := plan.ProjectRows("default", "source-00", rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCatalogSchemaEvolutionRegistryReplace(b *testing.B) {
	before := catalogSchemaEvolutionBenchmarkCatalog(false)
	after := catalogSchemaEvolutionBenchmarkCatalog(true)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		registry, err := hatSql.NewCatalogSchemaRegistry(before)
		if err != nil {
			b.Fatal(err)
		}
		if _, _, err := registry.Replace(after, hatSql.CatalogSchemaEvolutionOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func catalogSchemaEvolutionBenchmarkCatalog(addRegion bool) hatSql.Catalog {
	catalog := hatSql.Catalog{Namespaces: []string{"default"}, Sources: make([]hatSql.CatalogSource, 64), Indexes: make([]hatSql.CatalogIndex, 64)}
	for sourceIndex := range catalog.Sources {
		sourceName := fmt.Sprintf("source-%02d", sourceIndex)
		fields := []hatSql.CatalogField{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
			{Name: "amount", Type: "number", Nullable: true},
			{Name: "active", Type: "boolean"},
		}
		if addRegion {
			fields = append(fields, hatSql.CatalogField{Name: "region", Type: "string", Nullable: true})
		}
		catalog.Sources[sourceIndex] = hatSql.CatalogSource{Namespace: "default", Name: sourceName, Kind: "table", Fields: fields}
		catalog.Indexes[sourceIndex] = hatSql.CatalogIndex{Namespace: "default", Source: sourceName, Name: sourceName + "_id", Kind: "primary", Columns: []string{"id"}}
	}
	return catalog
}
