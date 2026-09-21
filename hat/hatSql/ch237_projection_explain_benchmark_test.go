package hatSql

import (
	"context"
	"testing"
)

var ch237ProjectionExplainBenchmarkResult SQLQueryResult

func newCH237ProjectionExplainBenchmarkFixture(b *testing.B) (*ch237VersionedResolver, *MaterializedViews, string) {
	b.Helper()
	rows := make([]Row, 128)
	for index := range rows {
		rows[index] = Row{"id": int64(index), "unused": "source-only-payload"}
	}
	resolver := &ch237VersionedResolver{
		rows:    map[string][]Row{"orders": rows},
		version: map[string]string{"orders": "v1"},
	}
	views := NewMaterializedViews()
	query := "FROM CACHE('orders') SELECT id"
	if _, err := views.Create(context.Background(), MaterializedViewDefinition{
		Name:         "orders_by_id",
		Query:        query,
		Dependencies: []string{"orders"},
	}, resolver, QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	return resolver, views, query
}

func BenchmarkCH237ExplainWithoutProjectionCatalog(b *testing.B) {
	resolver, _, query := newCH237ProjectionExplainBenchmarkFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN "+query, resolver, QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch237ProjectionExplainBenchmarkResult = result
	}
}

func BenchmarkCH237ExplainWithProjectionDiagnostics(b *testing.B) {
	resolver, views, query := newCH237ProjectionExplainBenchmarkFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN "+query, resolver, QueryOptions{ProjectionCatalog: views})
		if err != nil {
			b.Fatal(err)
		}
		ch237ProjectionExplainBenchmarkResult = result
	}
}
