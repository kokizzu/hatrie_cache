package hatSql

import (
	"context"
	"fmt"
	"testing"
)

var ch039GroupedTopKSink SQLQueryResult

func ch039GroupedTopKRows() []SQLRow {
	rows := make([]SQLRow, 20000)
	for index := range rows {
		rows[index] = SQLRow{
			"region": fmt.Sprintf("region-%02d", index%64),
			"state":  []string{"queued", "running", "failed", "done", "retry"}[(index*17)%5],
		}
	}
	return rows
}

func benchmarkCH039GroupedTopK(b *testing.B, options SQLQueryOptions) {
	rows := ch039GroupedTopKRows()
	query := "FROM CACHE('events') AS src SELECT src.region, APPROX_TOP_K(src.state, 8) AS states GROUP BY src.region"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, options)
		if err != nil {
			b.Fatalf("execute grouped top-k: %v", err)
		}
		ch039GroupedTopKSink = result
	}
}

func BenchmarkCH039GroupedTopKFallback(b *testing.B) {
	benchmarkCH039GroupedTopK(b, SQLQueryOptions{DisableNativeDataflow: true})
}

func BenchmarkCH039GroupedTopKAutomatic(b *testing.B) {
	benchmarkCH039GroupedTopK(b, SQLQueryOptions{})
}
