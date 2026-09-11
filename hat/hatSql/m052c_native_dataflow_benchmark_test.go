package hatSql

import (
	"context"
	"testing"
)

var compiledSQLDataflowBaselineSink SQLQueryResult
var compiledSQLNativeDataflowSink []SQLRow

func BenchmarkCompiledSQLDataflowBaseline(b *testing.B) {
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
			b.Fatalf("execute SQL: %v", err)
		}
		compiledSQLDataflowBaselineSink = result
	}
}

func BenchmarkCompiledSQLDataflowNative(b *testing.B) {
	rows := makeNativeDataflowBenchmarkRows(4096)
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 2048")
	if err != nil {
		b.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		b.Fatalf("compile native dataflow: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := native.Execute(context.Background(), rows)
		if err != nil {
			b.Fatalf("execute native dataflow: %v", err)
		}
		compiledSQLNativeDataflowSink = result
	}
}

func makeNativeDataflowBenchmarkRows(count int) []SQLRow {
	rows := make([]SQLRow, count)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    int64(index),
			"value": int64(index),
		}
	}
	return rows
}
