package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var mz047SessionComputeRoutingBenchmarkResult hatSql.QueryResult

func BenchmarkMZ047ExistingManagedComputePool(b *testing.B) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
		ComputeWorkers:       1,
		ComputeQueueCapacity: 8,
	})
	b.Cleanup(func() { _ = manager.Close() })
	for b.Loop() {
		result, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		mz047SessionComputeRoutingBenchmarkResult = result
	}
}
