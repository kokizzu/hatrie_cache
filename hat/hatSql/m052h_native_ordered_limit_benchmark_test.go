package hatSql

import (
	"context"
	"testing"
)

var (
	compiledSQLOrderedLimitBaselineSink SQLQueryResult
	compiledSQLOrderedLimitNativeSink   []SQLRow
)

const nativeDataflowOrderedLimitBenchmarkQuery = "FROM CACHE('items') AS src SELECT src.id AS id, src.value AS value WHERE src.value >= 0 ORDER BY src.value DESC LIMIT 32 OFFSET 512"

func BenchmarkCompiledSQLOrderedLimitBaseline(b *testing.B) {
	compiled, err := CompileSQLQuery(nativeDataflowOrderedLimitBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	rows := makeNativeDataflowOrderedLimitBenchmarkRows(4096)
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLOrderedLimitBaselineSink = result
	}
}

func BenchmarkCompiledSQLOrderedLimitNative(b *testing.B) {
	compiled, err := CompileSQLQuery(nativeDataflowOrderedLimitBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		b.Fatal(err)
	}
	rows := makeNativeDataflowOrderedLimitBenchmarkRows(4096)
	b.ReportAllocs()
	for range b.N {
		result, err := native.Execute(context.Background(), rows)
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLOrderedLimitNativeSink = result
	}
}

func makeNativeDataflowOrderedLimitBenchmarkRows(count int) []SQLRow {
	rows := make([]SQLRow, count)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    int64(index),
			"value": int64(index % 1000),
		}
	}
	return rows
}
