package hatSql

import "testing"

var sqlExplainPipelineBenchmarkSink SQLQueryResult

func BenchmarkSQLExplainPipeline(b *testing.B) {
	const query = "FROM VALUES (1), (2), (1) AS values(id) SELECT id, COUNT(*) AS total GROUP BY id ORDER BY id"
	for _, benchmark := range []struct {
		name   string
		prefix string
	}{
		{name: "regular_explain", prefix: "EXPLAIN "},
		{name: "pipeline_explain", prefix: "EXPLAIN PIPELINE "},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				result, err := ExecuteSQLQuery(benchmark.prefix+query, nil)
				if err != nil {
					b.Fatal(err)
				}
				sqlExplainPipelineBenchmarkSink = result
			}
		})
	}
}
