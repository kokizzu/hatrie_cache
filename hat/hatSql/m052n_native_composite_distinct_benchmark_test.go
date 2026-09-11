package hatSql

import (
	"context"
	"strconv"
	"testing"
)

var m052nNativeCompositeDistinctSink []SQLRow

func m052nNativeCompositeDistinctRows() []SQLRow {
	regions := make([]string, 512)
	for index := range regions {
		regions[index] = "region-" + strconv.Itoa(index)
	}
	rows := make([]SQLRow, 20_000)
	for index := range rows {
		rows[index] = SQLRow{
			"region": regions[index%len(regions)],
			"tier":   int64((index / len(regions)) % 8),
			"value":  int64(index%1000 - 500),
		}
	}
	return rows
}

func BenchmarkCompiledSQLNativeCompositeDistinctBaseline(b *testing.B) {
	rows := m052nNativeCompositeDistinctRows()
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT DISTINCT src.region AS region, src.tier AS tier WHERE src.value >= 0")
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
			b.Fatalf("execute ordinary composite DISTINCT query: %v", err)
		}
		m052nNativeCompositeDistinctSink = result.Rows
	}
}

func BenchmarkCompiledSQLNativeCompositeDistinctNative(b *testing.B) {
	rows := m052nNativeCompositeDistinctRows()
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT DISTINCT src.region AS region, src.tier AS tier WHERE src.value >= 0")
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
			b.Fatalf("execute native composite DISTINCT query: %v", err)
		}
		m052nNativeCompositeDistinctSink = result
	}
}
