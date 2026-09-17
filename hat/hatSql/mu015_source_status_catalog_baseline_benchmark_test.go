package hatSql

import (
	"context"
	"testing"
)

var mu015SourceStatusBaselineSink QueryResult

func BenchmarkMU015CatalogSourcesBaseline(b *testing.B) {
	resolver := CatalogResolver{Catalog: Catalog{
		Sources: []CatalogSource{
			{Namespace: "public", Name: "orders", Kind: "CACHE"},
			{Namespace: "public", Name: "people", Kind: "CACHE"},
			{Namespace: "public", Name: "regions", Kind: "CACHE"},
		},
	}}
	query := "FROM CACHE('information_schema.sources') SELECT namespace, name, kind ORDER BY namespace, name"

	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		mu015SourceStatusBaselineSink = result
	}
}
