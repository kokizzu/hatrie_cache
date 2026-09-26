package hatSql

import (
	"context"
	"strconv"
	"testing"
)

var m052aeNativeTripleGroupSink []SQLRow

func m052aeNativeTripleGroupBenchmarkRows() []SQLRow {
	regions := make([]string, 128)
	for index := range regions {
		regions[index] = "region-" + strconv.Itoa(index)
	}
	channels := []string{"web", "store", "mobile"}
	rows := make([]SQLRow, 20_000)
	for index := range rows {
		rows[index] = SQLRow{
			"region":  regions[index%len(regions)],
			"tier":    int64((index / len(regions)) % 4),
			"channel": channels[index%len(channels)],
			"value":   int64(index % 1_000),
		}
	}
	return rows
}

func BenchmarkCompiledSQLNativeTripleGroupBaseline(b *testing.B) {
	rows := m052aeNativeTripleGroupBenchmarkRows()
	compiled, err := CompileSQLQuery(m052aeNativeTripleGroupQuery())
	if err != nil {
		b.Fatalf("compile SQL: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{
			DisableNativeDataflow: true,
		})
		if err != nil {
			b.Fatalf("execute fallback triple GROUP BY query: %v", err)
		}
		m052aeNativeTripleGroupSink = result.Rows
	}
}

func BenchmarkCompiledSQLNativeTripleGroupNative(b *testing.B) {
	rows := m052aeNativeTripleGroupBenchmarkRows()
	compiled, err := CompileSQLQuery(m052aeNativeTripleGroupQuery())
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
			b.Fatalf("execute native triple GROUP BY query: %v", err)
		}
		m052aeNativeTripleGroupSink = result
	}
}
