package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var m241OptimizerBenchmarkSink int

func BenchmarkM241Optimizer(b *testing.B) {
	query := "EXPLAIN FROM VALUES (1) AS values(id) SELECT id"
	b.Run("default", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, nil, hatSql.SQLQueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			m241OptimizerBenchmarkSink += len(result.Plan)
		}
	})
	b.Run("optimizer_no_trace", func(b *testing.B) {
		options := hatSql.SQLQueryOptions{
			Optimizer: hatSql.NewSQLQueryOptimizer(func(*hatSql.SQLQueryOptimizationContext) error { return nil }),
		}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, nil, options)
			if err != nil {
				b.Fatal(err)
			}
			m241OptimizerBenchmarkSink += len(result.Plan)
		}
	})
}
