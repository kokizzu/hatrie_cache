package hatSql

import (
	"context"
	"testing"
)

type mz008BenchmarkResolver struct {
	rows []Row
}

func (resolver *mz008BenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver *mz008BenchmarkResolver) BeginSQLSnapshotAt(_ context.Context, _ uint64) (SQLSourceResolver, func(), error) {
	return resolver, nil, nil
}

var mz008AsOfBenchmarkSink SQLQueryResult

func BenchmarkSQLAsOfLiveBaseline(b *testing.B) {
	resolver := &mz008BenchmarkResolver{rows: []Row{{"id": int64(1), "value": "live"}}}
	query := "FROM CACHE('items') SELECT id, value"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		mz008AsOfBenchmarkSink = result
	}
}

func BenchmarkSQLAsOfHistorical(b *testing.B) {
	resolver := &mz008BenchmarkResolver{rows: []Row{{"id": int64(1), "value": "historical"}}}
	frontier := uint64(42)
	options := SQLQueryOptions{AsOfFrontier: &frontier}
	query := "FROM CACHE('items') SELECT id, value"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, options)
		if err != nil {
			b.Fatal(err)
		}
		mz008AsOfBenchmarkSink = result
	}
}
