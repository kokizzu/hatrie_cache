package hatSql

import "testing"

var ch045ExplainEstimateSink SQLQueryResult

func BenchmarkCH045ExplainEstimate(b *testing.B) {
	benchmarkCH045Explain(b, "EXPLAIN ESTIMATE FROM VALUES (1), (2), (3) AS values(id) WHERE id > 0 SELECT id")
}

func BenchmarkCH045ExplainCost(b *testing.B) {
	benchmarkCH045Explain(b, "EXPLAIN COST FROM VALUES (1), (2), (3) AS values(id) WHERE id > 0 SELECT id")
}

func benchmarkCH045Explain(b *testing.B, query string) {
	b.ReportAllocs()
	for range b.N {
		result, err := ExecuteSQLQuery(query, nil)
		if err != nil {
			b.Fatal(err)
		}
		ch045ExplainEstimateSink = result
	}
}
