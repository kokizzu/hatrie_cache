package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCompiledSQLAutomaticNativeScalarLimit(b *testing.B) {
	query, resolver := m052vScalarLimitBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := query.Execute(context.Background(), resolver, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompiledSQLAutomaticNativeScalarLimitFallback(b *testing.B) {
	query, resolver := m052vScalarLimitBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := query.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func m052vScalarLimitBenchmarkSetup(b *testing.B) (*CompiledSQLQuery, SQLSourceResolver) {
	b.Helper()
	rows := make([]SQLRow, 20000)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    int64(index),
			"value": int64(index % 97),
		}
	}
	query, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 0 LIMIT 16 OFFSET 32")
	if err != nil {
		b.Fatal(err)
	}
	return query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
}
