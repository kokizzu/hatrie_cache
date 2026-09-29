package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkM241OptimizerTrace(b *testing.B) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	optimizer := hatSql.NewSQLQueryOptimizer(func(*hatSql.SQLQueryOptimizationContext) error {
		return nil
	})
	query := "FROM CACHE('items') AS o SELECT o.id"
	run := func(b *testing.B, options hatSql.SQLQueryOptions) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, options)
			if err != nil {
				b.Fatal(err)
			}
			if len(result.Rows) != 1 {
				b.Fatalf("rows = %d, want 1", len(result.Rows))
			}
		}
	}
	b.Run("default", func(b *testing.B) {
		run(b, hatSql.SQLQueryOptions{})
	})
	b.Run("optimizer_no_trace", func(b *testing.B) {
		run(b, hatSql.SQLQueryOptions{Optimizer: optimizer})
	})
	b.Run("optimizer_trace", func(b *testing.B) {
		run(b, hatSql.SQLQueryOptions{
			Optimizer:      optimizer,
			OptimizerTrace: &hatSql.SQLOptimizerTraceOptions{MaxEntries: 64},
		})
	})
}
