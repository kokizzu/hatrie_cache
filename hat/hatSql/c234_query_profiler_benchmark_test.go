package hatSql

import (
	"context"
	"testing"
)

func BenchmarkC234QueryDefault(b *testing.B) {
	benchmarkC234Query(b, SQLQueryOptions{})
}

func BenchmarkC234QueryObserver(b *testing.B) {
	benchmarkC234Query(b, SQLQueryOptions{Observer: QueryObserverFunc(func(QueryEvent) {})})
}

func BenchmarkC234QueryProfiler(b *testing.B) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxSamplesPerQuery: 64})
	if err != nil {
		b.Fatal(err)
	}
	benchmarkC234Query(b, SQLQueryOptions{QueryProfiler: profiler})
}

func BenchmarkC234QueryProfilerAllocations(b *testing.B) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxSamplesPerQuery: 64, CaptureAllocations: true})
	if err != nil {
		b.Fatal(err)
	}
	benchmarkC234Query(b, SQLQueryOptions{QueryProfiler: profiler})
}

func benchmarkC234Query(b *testing.B, options SQLQueryOptions) {
	b.Helper()
	options.QueryID = "c234-benchmark"
	query := "FROM CACHE('events') AS event SELECT event.kind"
	resolver := c234RowsResolver{rows: []Row{{"kind": "queued"}, {"kind": "done"}}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
			b.Fatal(err)
		}
	}
}
