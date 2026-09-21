package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkMZ047NamedComputeCluster(b *testing.B) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
		ComputeClusters: map[string]hatSql.SQLComputeClusterOptions{
			"analytics": {Workers: 1, QueueCapacity: 8},
		},
	})
	b.Cleanup(func() { _ = manager.Close() })
	for b.Loop() {
		result, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{
			ComputeCluster: "analytics",
		})
		if err != nil {
			b.Fatal(err)
		}
		mz047SessionComputeRoutingBenchmarkResult = result
	}
}
