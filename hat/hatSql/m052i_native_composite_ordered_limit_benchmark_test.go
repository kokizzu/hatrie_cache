package hatSql

import (
	"context"
	"testing"
)

var (
	compiledSQLCompositeOrderedLimitBaselineSink SQLQueryResult
	compiledSQLCompositeOrderedLimitNativeSink   []SQLRow
)

const nativeDataflowCompositeOrderedLimitBenchmarkQuery = "FROM CACHE('items') AS src SELECT src.id AS id, src.region AS region, src.value AS value WHERE src.value >= 0 ORDER BY src.region ASC, src.value DESC LIMIT 32 OFFSET 512"

func BenchmarkCompiledSQLCompositeOrderedLimitBaseline(b *testing.B) {
	compiled, err := CompileSQLQuery(nativeDataflowCompositeOrderedLimitBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	rows := makeNativeDataflowCompositeOrderedLimitBenchmarkRows(4096)
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLCompositeOrderedLimitBaselineSink = result
	}
}

func BenchmarkCompiledSQLCompositeOrderedLimitNative(b *testing.B) {
	compiled, err := CompileSQLQuery(nativeDataflowCompositeOrderedLimitBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		b.Fatal(err)
	}
	rows := makeNativeDataflowCompositeOrderedLimitBenchmarkRows(4096)
	b.ReportAllocs()
	for range b.N {
		result, err := native.Execute(context.Background(), rows)
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLCompositeOrderedLimitNativeSink = result
	}
}

func makeNativeDataflowCompositeOrderedLimitBenchmarkRows(count int) []SQLRow {
	rows := make([]SQLRow, count)
	for index := range rows {
		rows[index] = SQLRow{
			"id":     int64(index),
			"region": int64(index % 64),
			"value":  int64(index % 1000),
		}
	}
	return rows
}
