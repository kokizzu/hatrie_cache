package hatSql

import "testing"

func BenchmarkSQLApproximateAggregateStream(b *testing.B) {
	rows := make(approximateAggregateSource, 10000)
	for index := range rows {
		rows[index] = SQLRow{
			"visitor": int64(index % 4000),
			"latency": float64((index * 37) % 1000),
		}
	}
	source := approximateAggregateStreamSource{rows: rows}
	query := `SELECT APPROX_COUNT_DISTINCT(visitor, 10) AS visitors, APPROX_PERCENTILE(latency, 0.95, 0.01) AS p95 FROM CACHE('events')`
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(query, source)
		if err != nil {
			b.Fatalf("execute approximate aggregate query: %v", err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("result rows = %d, want 1", len(result.Rows))
		}
	}
}
