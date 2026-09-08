package hatSql

import (
	"context"
	"testing"
)

type compiledPlanBenchmarkResolver struct{}

func (compiledPlanBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"name": "Ada", "score": int64(7)}}, nil
}

var compiledPlanBenchmarkSink int

func BenchmarkSQLCompiledPlanExecution(b *testing.B) {
	const source = "SELECT name FROM CACHE('users') WHERE score >= $1"
	resolver := compiledPlanBenchmarkResolver{}
	compiled, err := CompileSQLQuery(source)
	if err != nil {
		b.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		compiled bool
	}{
		{name: "cached_template", compiled: false},
		{name: "compiled_handle", compiled: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			cache := NewSQLPreparedQueryCache(1)
			if _, err := ExecuteSQLQueryParameters(context.Background(), source, resolver, []interface{}{int64(7)}, SQLQueryOptions{PreparedCache: cache}); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			b.ReportAllocs()
			rows := 0
			for iteration := 0; iteration < b.N; iteration++ {
				var result SQLQueryResult
				var err error
				if test.compiled {
					result, err = compiled.Execute(context.Background(), resolver, []interface{}{int64(7)}, SQLQueryOptions{})
				} else {
					result, err = ExecuteSQLQueryParameters(context.Background(), source, resolver, []interface{}{int64(7)}, SQLQueryOptions{PreparedCache: cache})
				}
				if err != nil {
					b.Fatal(err)
				}
				rows += len(result.Rows)
			}
			compiledPlanBenchmarkSink = rows
		})
	}
}
