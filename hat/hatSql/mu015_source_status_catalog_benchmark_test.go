package hatSql

import (
	"context"
	"testing"
)

var mu015SourceStatusSink QueryResult

func BenchmarkMU015CatalogSourceStatus(b *testing.B) {
	resolver := CatalogResolver{
		Catalog: Catalog{
			Version: 3,
			Sources: []CatalogSource{
				{Namespace: "public", Name: "orders", Kind: "CACHE"},
				{Namespace: "public", Name: "people", Kind: "CACHE"},
				{Namespace: "public", Name: "regions", Kind: "CACHE"},
			},
		},
		SourceStatus: CatalogSourceStatusResolverFunc(func() ([]CatalogSourceStatus, error) {
			return []CatalogSourceStatus{
				{Namespace: "public", Source: "orders", Kind: "CACHE", State: CatalogSourceStateReady, Available: true, Ready: true, Frontier: 10, Observed: 10},
				{Namespace: "public", Source: "people", Kind: "CACHE", State: CatalogSourceStateRunning, Available: true, Frontier: 8, Observed: 10},
				{Namespace: "public", Source: "regions", Kind: "CACHE", State: CatalogSourceStateStopped},
			}, nil
		}),
	}
	query := "FROM CACHE('information_schema.source_status') SELECT catalog_version, namespace, source, kind, state, available, ready, frontier, observed, lag, error_code ORDER BY namespace, source"

	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		mu015SourceStatusSink = result
	}
}
