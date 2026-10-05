package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCHU26SQLQueryProfilerAutomaticMemory(b *testing.B) {
	b.Run("default", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result, err := ExecuteSQLQueryContext(context.Background(), chu26AutomaticMemoryProfileQuery, nil, SQLQueryOptions{})
			if err != nil || len(result.Rows) != 2 {
				b.Fatalf("default query rows=%d err=%v", len(result.Rows), err)
			}
		}
	})
	b.Run("profiler", func(b *testing.B) {
		profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for range b.N {
			result, err := ExecuteSQLQueryContext(context.Background(), chu26AutomaticMemoryProfileQuery, nil, SQLQueryOptions{QueryID: "profiler", QueryProfiler: profiler})
			if err != nil || len(result.Rows) != 2 {
				b.Fatalf("profiler query rows=%d err=%v", len(result.Rows), err)
			}
		}
	})
	b.Run("automatic-memory", func(b *testing.B) {
		profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{CaptureOperatorMemory: true})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for range b.N {
			result, err := ExecuteSQLQueryContext(context.Background(), chu26AutomaticMemoryProfileQuery, nil, SQLQueryOptions{QueryID: "automatic", QueryProfiler: profiler})
			if err != nil || len(result.Rows) != 2 {
				b.Fatalf("automatic query rows=%d err=%v", len(result.Rows), err)
			}
		}
	})
}
