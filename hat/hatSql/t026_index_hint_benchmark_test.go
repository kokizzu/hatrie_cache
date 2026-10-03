package hatSql

import (
	"context"
	"testing"
)

func BenchmarkT026NamedHint(b *testing.B) {
	resolver := newT026BenchmarkResolver()
	options := SQLQueryOptions{IndexHint: SQLIndexHint{Source: "person", Field: "region", Index: "region_copy", Mode: SQLIndexHintForce}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), t026BenchmarkQuery, resolver, options)
		if err != nil || len(result.Rows) != 32 {
			b.Fatalf("named hint result rows=%d err=%v", len(result.Rows), err)
		}
	}
}
