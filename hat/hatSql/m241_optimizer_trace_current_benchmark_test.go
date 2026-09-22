package hatSql_test

import (
	"context"
	"encoding/json"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkM241OptimizerTrace(b *testing.B) {
	query := "EXPLAIN FROM VALUES (1) AS values(id) SELECT id"
	options := hatSql.SQLQueryOptions{
		OptimizerTrace: true,
		Optimizer: hatSql.NewSQLQueryOptimizer(func(plan *hatSql.SQLQueryOptimizationContext) error {
			plan.RejectAlternative("full scan", "an index strategy is available")
			return nil
		}),
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, nil, options)
		if err != nil {
			b.Fatal(err)
		}
		m241OptimizerBenchmarkSink += len(result.OptimizerTrace.Events)
	}
}

func BenchmarkM241OptimizerTraceJSON(b *testing.B) {
	query := "EXPLAIN FROM VALUES (1) AS values(id) SELECT id"
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, nil, hatSql.SQLQueryOptions{
		OptimizerTrace: true,
		Optimizer: hatSql.NewSQLQueryOptimizer(func(plan *hatSql.SQLQueryOptimizationContext) error {
			plan.RejectAlternative("full scan", "an index strategy is available")
			return nil
		}),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	encoded, err := json.Marshal(result)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(encoded)), "wire_bytes")
	b.StartTimer()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		encoded, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			b.Fatal(marshalErr)
		}
		m241OptimizerBenchmarkSink += len(encoded)
	}
}
