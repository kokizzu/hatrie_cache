//go:build mz044baseline

package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

var mz044BaselineExplainBenchmarkSink hatSql.QueryResult

func BenchmarkMZ044BaselineRegularExplain(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM VALUES (1), (2), (3), (4) AS values(id) WHERE id > 0 SELECT id", nil)
		if err != nil {
			b.Fatal(err)
		}
		mz044BaselineExplainBenchmarkSink = result
	}
}
