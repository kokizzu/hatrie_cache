package hatSql

import (
	"context"
	"fmt"
	"testing"
)

var ch039GroupedApproxSink SQLQueryResult

func ch039GroupedApproxRows() []SQLRow {
	rows := make([]SQLRow, 20000)
	for index := range rows {
		group := index % 64
		rows[index] = SQLRow{
			"region":  fmt.Sprintf("region-%02d", group),
			"visitor": fmt.Sprintf("visitor-%04d", index%512),
			"latency": float64((index*37)%10000) / 10,
		}
	}
	return rows
}

func benchmarkCH039GroupedApprox(b *testing.B, options SQLQueryOptions) {
	rows := ch039GroupedApproxRows()
	query := "FROM CACHE('events') AS src SELECT src.region, APPROX_COUNT_DISTINCT(src.visitor, 10) AS visitors, APPROX_PERCENTILE(src.latency, 0.95, 0.01) AS p95 GROUP BY src.region"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, options)
		if err != nil {
			b.Fatalf("execute grouped approximate query: %v", err)
		}
		ch039GroupedApproxSink = result
	}
}

func BenchmarkCH039GroupedApproxFallback(b *testing.B) {
	benchmarkCH039GroupedApprox(b, SQLQueryOptions{DisableNativeDataflow: true})
}

func BenchmarkCH039GroupedApproxAutomatic(b *testing.B) {
	benchmarkCH039GroupedApprox(b, SQLQueryOptions{})
}
