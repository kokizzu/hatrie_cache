package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCompiledSQLAutomaticNativeOrdered(b *testing.B) {
	compiled, resolver := m052rOrderedBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompiledSQLAutomaticNativeOrderedFallback(b *testing.B) {
	compiled, resolver := m052rOrderedBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func m052rOrderedBenchmarkSetup(b *testing.B) (*CompiledSQLQuery, SQLSourceResolver) {
	b.Helper()
	rows := make([]SQLRow, 4096)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    int64(index),
			"score": int64((index * 7919) % 100000),
		}
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.score ORDER BY src.score DESC LIMIT 32 OFFSET 512")
	if err != nil {
		b.Fatal(err)
	}
	return compiled, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
}
