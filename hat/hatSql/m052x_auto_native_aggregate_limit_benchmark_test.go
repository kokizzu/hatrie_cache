package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCompiledSQLAutomaticNativeAggregateLimit(b *testing.B) {
	query, resolver := m052xAggregateLimitBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := query.Execute(context.Background(), resolver, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompiledSQLAutomaticNativeAggregateLimitFallback(b *testing.B) {
	query, resolver := m052xAggregateLimitBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := query.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func m052xAggregateLimitBenchmarkSetup(b *testing.B) (*CompiledSQLQuery, SQLSourceResolver) {
	b.Helper()
	rows := make([]SQLRow, 20000)
	for index := range rows {
		rows[index] = SQLRow{"value": int64(index % 97)}
	}
	query, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT COUNT(*) AS total, SUM(src.value) AS total_value LIMIT 1")
	if err != nil {
		b.Fatal(err)
	}
	return query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
}
