package hatSql

import (
	"context"
	"testing"
)

var mz050PlanSnapshotBenchmarkSink QueryResult

func BenchmarkMZ050PlanSnapshotDefault(b *testing.B) {
	resolver := mz050PlanSnapshotResolver{}
	options := SQLQueryOptions{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN SELECT id FROM CACHE('users')", resolver, options)
		if err != nil {
			b.Fatal(err)
		}
		mz050PlanSnapshotBenchmarkSink = result
	}
}

func BenchmarkMZ050PlanSnapshotEnabled(b *testing.B) {
	resolver := mz050PlanSnapshotResolver{}
	options := SQLQueryOptions{PlanSnapshot: &SQLPlanSnapshotOptions{}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN SELECT id FROM CACHE('users')", resolver, options)
		if err != nil {
			b.Fatal(err)
		}
		mz050PlanSnapshotBenchmarkSink = result
	}
}
