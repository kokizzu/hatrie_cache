package hatSql

import "testing"

func BenchmarkSQLQueryOptimizerRules(b *testing.B) {
	query := "FROM VALUES (1) AS values(id) SELECT id"
	b.Run("default", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := ExecuteSQLQuery(query, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("one_noop_rule", func(b *testing.B) {
		options := SQLQueryOptions{Optimizer: NewSQLQueryOptimizer(func(*SQLQueryOptimizationContext) error { return nil })}
		b.ReportAllocs()
		for range b.N {
			if _, err := ExecuteSQLQueryContext(b.Context(), query, nil, options); err != nil {
				b.Fatal(err)
			}
		}
	})
}
