package hatSql

import "testing"

func TestSQLTDigestPercentileSupportsTailAndFilter(t *testing.T) {
	rows := make(approximateAggregateSource, 10000)
	for index := range rows {
		rows[index] = SQLRow{
			"region":  "east",
			"latency": float64(index + 1),
			"state":   "ok",
		}
	}
	result, err := ExecuteSQLQuery(`
		SELECT APPROX_TDIGEST_PERCENTILE(latency, 0.99, 100) FILTER (WHERE state = 'ok') AS p99
		FROM CACHE('events')
	`, rows)
	if err != nil {
		t.Fatalf("execute t-digest percentile: %v", err)
	}
	value, ok := result.Rows[0]["p99"].(float64)
	if !ok || value < 9700 || value > 10000 {
		t.Fatalf("p99 = %#v, want a value in [9700, 10000]", result.Rows[0]["p99"])
	}
}

func TestSQLTDigestPercentileValidatesArguments(t *testing.T) {
	rows := approximateAggregateSource{{"latency": 10.0}}
	for _, query := range []string{
		`SELECT APPROX_TDIGEST_PERCENTILE(latency) FROM CACHE('events')`,
		`SELECT APPROX_TDIGEST_PERCENTILE(latency, 1.1) FROM CACHE('events')`,
		`SELECT APPROX_TDIGEST_PERCENTILE(latency, 0.5, 10) FROM CACHE('events')`,
	} {
		if _, err := ExecuteSQLQuery(query, rows); err == nil {
			t.Fatalf("query %q succeeded, want validation error", query)
		}
	}
}

func BenchmarkSQLTDigestPercentile(b *testing.B) {
	rows := make(approximateAggregateSource, 10000)
	for index := range rows {
		latency := float64((index * 37) % 1000)
		if index >= 9900 {
			latency = float64(1000 + (index-9900)*100)
		}
		rows[index] = SQLRow{"latency": latency}
	}
	benchmarks := []struct {
		name  string
		query string
	}{
		{
			name:  "gk-control",
			query: `SELECT APPROX_PERCENTILE(latency, 0.99, 0.01) AS p99 FROM CACHE('events')`,
		},
		{
			name:  "tdigest",
			query: `SELECT APPROX_TDIGEST_PERCENTILE(latency, 0.99, 100) AS p99 FROM CACHE('events')`,
		},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err := ExecuteSQLQuery(benchmark.query, rows)
				if err != nil || len(result.Rows) != 1 {
					b.Fatalf("execute percentile: result=%#v err=%v", result, err)
				}
				if _, ok := result.Rows[0]["p99"].(float64); !ok {
					b.Fatalf("p99 = %#v, want float64", result.Rows[0]["p99"])
				}
			}
		})
	}
}
