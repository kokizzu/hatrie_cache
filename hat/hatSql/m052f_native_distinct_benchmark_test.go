package hatSql

import (
	"context"
	"testing"
)

var (
	compiledSQLDistinctBaselineSink SQLQueryResult
	compiledSQLDistinctNativeSink   []SQLRow
)

const nativeDataflowDistinctBenchmarkQuery = "FROM CACHE('items') AS src SELECT DISTINCT src.group AS bucket WHERE src.value >= 0"

func BenchmarkCompiledSQLDistinctBaseline(b *testing.B) {
	compiled, err := CompileSQLQuery(nativeDataflowDistinctBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	rows := makeNativeDataflowDistinctBenchmarkRows(4096)
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLDistinctBaselineSink = result
	}
}

func BenchmarkCompiledSQLDistinctNative(b *testing.B) {
	compiled, err := CompileSQLQuery(nativeDataflowDistinctBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		b.Fatal(err)
	}
	rows := makeNativeDataflowDistinctBenchmarkRows(4096)
	b.ReportAllocs()
	for range b.N {
		result, err := native.Execute(context.Background(), rows)
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLDistinctNativeSink = result
	}
}

func makeNativeDataflowDistinctBenchmarkRows(count int) []SQLRow {
	rows := make([]SQLRow, count)
	for index := range rows {
		rows[index] = SQLRow{
			"group": int64(index % 257),
			"value": int64(index%1000 - 500),
		}
	}
	return rows
}
