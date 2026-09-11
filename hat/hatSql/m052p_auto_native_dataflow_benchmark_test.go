package hatSql

import (
	"context"
	"testing"
)

var compiledSQLAutomaticNativeDataflowSink SQLQueryResult

func BenchmarkCompiledSQLAutomaticNativeDataflow(b *testing.B) {
	rows := makeNativeDataflowBenchmarkRows(4096)
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 2048")
	if err != nil {
		b.Fatalf("compile SQL: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatalf("execute automatic SQL: %v", err)
		}
		compiledSQLAutomaticNativeDataflowSink = result
	}
}

func BenchmarkCompiledSQLAutomaticNativeDataflowFallback(b *testing.B) {
	rows := makeNativeDataflowBenchmarkRows(4096)
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 2048")
	if err != nil {
		b.Fatalf("compile SQL: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true})
		if err != nil {
			b.Fatalf("execute fallback SQL: %v", err)
		}
		compiledSQLAutomaticNativeDataflowSink = result
	}
}
