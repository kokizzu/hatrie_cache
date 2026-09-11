package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkMZ018SQLQueryManagerComputePool(b *testing.B) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	for _, workers := range []int{1, 2, 4} {
		b.Run("workers-"+benchmarkIntString(workers), func(b *testing.B) {
			manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
				ComputeWorkers: workers,
			})
			defer func() {
				if err := manager.Close(); err != nil {
					b.Fatal(err)
				}
			}()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 {
					b.Fatalf("rows = %d, want 1", len(result.Rows))
				}
			}
		})
	}
}

func BenchmarkMZ018SQLQueryManagerComputePoolConcurrent(b *testing.B) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
		ComputeWorkers:       4,
		ComputeQueueCapacity: 64,
	})
	defer func() {
		if err := manager.Close(); err != nil {
			b.Fatal(err)
		}
	}()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{})
			if err != nil {
				b.Errorf("execute query: %v", err)
				return
			}
			if len(result.Rows) != 1 {
				b.Errorf("rows = %d, want 1", len(result.Rows))
				return
			}
		}
	})
}

func BenchmarkMZ018SQLQueryManagerLegacyConcurrent(b *testing.B) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	manager := hatSql.NewSQLQueryManager(256)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{})
			if err != nil {
				b.Errorf("execute query: %v", err)
				return
			}
			if len(result.Rows) != 1 {
				b.Errorf("rows = %d, want 1", len(result.Rows))
				return
			}
		}
	})
}

func benchmarkIntString(value int) string {
	if value == 1 {
		return "1"
	}
	if value == 2 {
		return "2"
	}
	return "4"
}
