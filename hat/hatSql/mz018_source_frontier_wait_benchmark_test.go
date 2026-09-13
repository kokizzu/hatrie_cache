package hatSql

import (
	"context"
	"testing"
	"time"
)

var mz018SourceFrontierBenchmarkSink SQLQueryResult

func BenchmarkMZ018SourceFrontierDefault(b *testing.B) {
	benchmarkMZ018SourceFrontier(b, SQLQueryOptions{
		RequireSourceFrontier:  true,
		RequiredSourceFrontier: 5,
	})
}

func BenchmarkMZ018SourceFrontierWaitEnabled(b *testing.B) {
	benchmarkMZ018SourceFrontier(b, SQLQueryOptions{
		RequireSourceFrontier:      true,
		RequiredSourceFrontier:     5,
		SourceFrontierWaitTimeout:  time.Second,
		SourceFrontierWaitInterval: time.Millisecond,
	})
}

func benchmarkMZ018SourceFrontier(b *testing.B, options SQLQueryOptions) {
	resolver := &mz018SourceFrontierResolver{frontier: 5, succeedAt: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQueryContext(context.Background(), "SELECT id FROM CACHE('users')", resolver, options)
		if err != nil {
			b.Fatal(err)
		}
		mz018SourceFrontierBenchmarkSink = result
	}
}
