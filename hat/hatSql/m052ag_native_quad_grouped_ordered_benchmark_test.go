package hatSql

import (
	"context"
	"strconv"
	"testing"
)

var m052agNativeQuadGroupedOrderedSink []SQLRow

func m052agNativeQuadGroupedOrderedBenchmarkRows() []SQLRow {
	regions := make([]string, 128)
	for index := range regions {
		regions[index] = "region-" + strconv.Itoa(index)
	}
	channels := []string{"web", "store", "mobile"}
	segments := []interface{}{"new", "returning", nil, "vip"}
	rows := make([]SQLRow, 20_000)
	for index := range rows {
		rows[index] = SQLRow{
			"region":  regions[index%len(regions)],
			"tier":    int64((index / len(regions)) % 4),
			"channel": channels[index%len(channels)],
			"segment": segments[index%len(segments)],
			"value":   int64((index * 17) % 1_000),
		}
	}
	return rows
}

func m052agNativeQuadGroupedOrderedBenchmarkQuery() string {
	return "FROM CACHE('items') AS src SELECT src.region AS region, src.tier AS tier, src.channel AS channel, src.segment AS segment, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.region, src.tier, src.channel, src.segment ORDER BY total_value DESC, region ASC LIMIT 100 OFFSET 25"
}

func BenchmarkCompiledSQLNativeQuadGroupedOrderedBaseline(b *testing.B) {
	rows := m052agNativeQuadGroupedOrderedBenchmarkRows()
	compiled, err := CompileSQLQuery(m052agNativeQuadGroupedOrderedBenchmarkQuery())
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
			b.Fatalf("execute fallback quad grouped ordered query: %v", err)
		}
		m052agNativeQuadGroupedOrderedSink = result.Rows
	}
}

func BenchmarkCompiledSQLNativeQuadGroupedOrderedNative(b *testing.B) {
	rows := m052agNativeQuadGroupedOrderedBenchmarkRows()
	compiled, err := CompileSQLQuery(m052agNativeQuadGroupedOrderedBenchmarkQuery())
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
			b.Fatalf("execute native quad grouped ordered query: %v", err)
		}
		m052agNativeQuadGroupedOrderedSink = result
	}
}
