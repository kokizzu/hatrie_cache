package hatSql

import (
	"context"
	"testing"
)

var mu014CatalogBaselineSink QueryResult

func BenchmarkMU014CatalogSourcesBaseline(b *testing.B) {
	resolver := CatalogResolver{Catalog: Catalog{
		Namespaces: []string{"public"},
		Sources: []CatalogSource{
			{
				Namespace: "public",
				Name:      "people",
				Kind:      "CACHE",
				Fields: []CatalogField{
					{Name: "id", Type: "INTEGER"},
					{Name: "name", Type: "TEXT"},
				},
			},
		},
		Indexes: []CatalogIndex{{
			Namespace: "public",
			Source:    "people",
			Name:      "people_id",
			Kind:      "hash",
			Columns:   []string{"id"},
		}},
	}}
	query := "FROM CACHE('information_schema.sources') SELECT namespace, name, kind ORDER BY name"

	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		mu014CatalogBaselineSink = result
	}
}
