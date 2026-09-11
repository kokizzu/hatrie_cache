package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCompiledSQLAutomaticNativeAggregate(b *testing.B) {
	compiled, resolver := m052qAggregateBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompiledSQLAutomaticNativeAggregateFallback(b *testing.B) {
	compiled, resolver := m052qAggregateBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompiledSQLAutomaticNativeDistinct(b *testing.B) {
	compiled, resolver := m052qDistinctBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompiledSQLAutomaticNativeDistinctFallback(b *testing.B) {
	compiled, resolver := m052qDistinctBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func m052qAggregateBenchmarkSetup(b *testing.B) (*CompiledSQLQuery, SQLSourceResolver) {
	b.Helper()
	rows := make([]SQLRow, 4096)
	for index := range rows {
		rows[index] = SQLRow{"value": int64(index % 97)}
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT COUNT(*) AS total, SUM(src.value) AS total_value")
	if err != nil {
		b.Fatal(err)
	}
	return compiled, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
}

func m052qDistinctBenchmarkSetup(b *testing.B) (*CompiledSQLQuery, SQLSourceResolver) {
	b.Helper()
	rows := make([]SQLRow, 4096)
	for index := range rows {
		rows[index] = SQLRow{"value": int64(index % 257)}
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT DISTINCT src.value")
	if err != nil {
		b.Fatal(err)
	}
	return compiled, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
}
