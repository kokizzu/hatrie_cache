//go:build !mz044baseline

package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

var mz044ExplainBenchmarkSink hatSql.QueryResult

func BenchmarkCostedExplainMZ044Regular(b *testing.B) {
	benchmarkMZ044Explain(b, "EXPLAIN FROM VALUES (1), (2), (3), (4) AS values(id) WHERE id > 0 SELECT id")
}

func BenchmarkCostedExplainMZ044Costed(b *testing.B) {
	benchmarkMZ044Explain(b, "EXPLAIN COST FROM VALUES (1), (2), (3), (4) AS values(id) WHERE id > 0 SELECT id")
}

func benchmarkMZ044Explain(b *testing.B, query string) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result, err := hatSql.ExecuteSQLQuery(query, nil)
		if err != nil {
			b.Fatal(err)
		}
		mz044ExplainBenchmarkSink = result
	}
}
