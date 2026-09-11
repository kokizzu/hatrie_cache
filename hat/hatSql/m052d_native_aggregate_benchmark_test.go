package hatSql

import (
	"context"
	"testing"
)

var (
	compiledSQLAggregateBaselineSink SQLQueryResult
	compiledSQLAggregateNativeSink   []SQLRow
)

func BenchmarkCompiledSQLAggregateBaseline(b *testing.B) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT COUNT(*) AS total, SUM(src.value) AS sum, AVG(src.value) AS average, MIN(src.value) AS minimum, MAX(src.value) AS maximum WHERE src.value >= 2048")
	if err != nil {
		b.Fatal(err)
	}
	rows := makeNativeDataflowAggregateBenchmarkRows(4096)
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLAggregateBaselineSink = result
	}
}

func BenchmarkCompiledSQLAggregateNative(b *testing.B) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT COUNT(*) AS total, SUM(src.value) AS sum, AVG(src.value) AS average, MIN(src.value) AS minimum, MAX(src.value) AS maximum WHERE src.value >= 2048")
	if err != nil {
		b.Fatal(err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		b.Fatal(err)
	}
	rows := makeNativeDataflowAggregateBenchmarkRows(4096)
	b.ReportAllocs()
	for range b.N {
		result, err := native.Execute(context.Background(), rows)
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLAggregateNativeSink = result
	}
}

func makeNativeDataflowAggregateBenchmarkRows(count int) []SQLRow {
	rows := make([]SQLRow, count)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    int64(index),
			"value": int64(index % 4096),
		}
	}
	return rows
}
