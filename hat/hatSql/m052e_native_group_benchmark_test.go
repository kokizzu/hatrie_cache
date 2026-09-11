package hatSql

import (
	"context"
	"testing"
)

var (
	compiledSQLGroupBaselineSink SQLQueryResult
	compiledSQLGroupNativeSink   []SQLRow
)

const nativeDataflowGroupBenchmarkQuery = "FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total, COUNT(src.value) AS present, SUM(src.value) AS sum, AVG(src.value) AS average, MIN(src.value) AS minimum, MAX(src.value) AS maximum WHERE src.value >= -100 GROUP BY src.group"

func BenchmarkCompiledSQLGroupBaseline(b *testing.B) {
	compiled, err := CompileSQLQuery(nativeDataflowGroupBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	rows := makeNativeDataflowGroupBenchmarkRows(4096)
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLGroupBaselineSink = result
	}
}

func BenchmarkCompiledSQLGroupNative(b *testing.B) {
	compiled, err := CompileSQLQuery(nativeDataflowGroupBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		b.Fatal(err)
	}
	rows := makeNativeDataflowGroupBenchmarkRows(4096)
	b.ReportAllocs()
	for range b.N {
		result, err := native.Execute(context.Background(), rows)
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLGroupNativeSink = result
	}
}

func makeNativeDataflowGroupBenchmarkRows(count int) []SQLRow {
	rows := make([]SQLRow, count)
	for index := range rows {
		rows[index] = SQLRow{
			"group": int64(index % 257),
			"value": int64(index%1000 - 500),
		}
	}
	return rows
}
