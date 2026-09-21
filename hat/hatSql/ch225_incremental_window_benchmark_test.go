package hatSql

import "testing"

func BenchmarkC225WindowAggregateBaseline(b *testing.B) {
	rows := make(approximateAggregateSource, 2000)
	for index := range rows {
		rows[index] = SQLRow{
			"id":     int64(index),
			"bucket": int64(index % 8),
			"value":  float64(index % 100),
		}
	}
	query := `
		SELECT id,
			SUM(value) OVER (PARTITION BY bucket ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum,
			AVG(value) OVER (PARTITION BY bucket ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg,
			MIN(value) OVER (PARTITION BY bucket ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min,
			MAX(value) OVER (PARTITION BY bucket ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max
		FROM CACHE('events')`
	b.ReportAllocs()
	for range b.N {
		result, err := ExecuteSQLQuery(query, rows)
		if err != nil || len(result.Rows) != len(rows) {
			b.Fatalf("execute window query: rows=%d err=%v", len(result.Rows), err)
		}
	}
}
