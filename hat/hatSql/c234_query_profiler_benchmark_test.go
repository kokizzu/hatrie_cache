package hatSql

import (
	"context"
	"testing"
)

var c234QueryProfilerBenchmarkSink SQLQueryEvent

func BenchmarkC234QueryObservation(b *testing.B) {
	query := "FROM VALUES (1) AS values(id) SELECT id"
	for _, benchmark := range []struct {
		name               string
		profileAllocations bool
	}{
		{name: "default"},
		{name: "allocations", profileAllocations: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{
					Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
						c234QueryProfilerBenchmarkSink = event
					}),
					ProfileAllocations: benchmark.profileAllocations,
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
