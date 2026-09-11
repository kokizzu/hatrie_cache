package hatSql

import (
	"context"
	"testing"
)

var (
	compiledSQLLimitBaselineSink SQLQueryResult
	compiledSQLLimitNativeSink   []SQLRow
)

const nativeDataflowLimitBenchmarkQuery = "FROM CACHE('items') AS src SELECT src.id AS id, src.value AS value WHERE src.value >= 0 LIMIT 32 OFFSET 512"

func BenchmarkCompiledSQLLimitBaseline(b *testing.B) {
	compiled, err := CompileSQLQuery(nativeDataflowLimitBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	rows := makeNativeDataflowLimitBenchmarkRows(4096)
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLLimitBaselineSink = result
	}
}

func BenchmarkCompiledSQLLimitNative(b *testing.B) {
	compiled, err := CompileSQLQuery(nativeDataflowLimitBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		b.Fatal(err)
	}
	rows := makeNativeDataflowLimitBenchmarkRows(4096)
	b.ReportAllocs()
	for range b.N {
		result, err := native.Execute(context.Background(), rows)
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLLimitNativeSink = result
	}
}

func makeNativeDataflowLimitBenchmarkRows(count int) []SQLRow {
	rows := make([]SQLRow, count)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    int64(index),
			"value": int64(index % 4),
		}
	}
	return rows
}
