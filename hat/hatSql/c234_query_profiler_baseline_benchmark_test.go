package hatSql

import (
	"context"
	"testing"
)

var c234QueryProfilerBaselineBenchmarkSink SQLQueryEvent

func BenchmarkC234QueryObservationBaseline(b *testing.B) {
	const query = "FROM VALUES (1) AS values(id) SELECT id"
	b.ReportAllocs()
	for range b.N {
		_, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{
			Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
				c234QueryProfilerBaselineBenchmarkSink = event
			}),
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
