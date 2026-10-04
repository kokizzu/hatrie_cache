package hatSql

import "testing"

func TestSQLApproximatePercentileInfo(t *testing.T) {
	rows := approximateAggregateSource{
		{"region": "east", "latency": 10.0},
		{"region": "east", "latency": 20.0},
		{"region": "east", "latency": 30.0},
		{"region": "east", "latency": 40.0},
		{"region": "east", "latency": nil},
	}

	result, err := ExecuteSQLQuery(`
		SELECT APPROX_PERCENTILE_INFO(latency, 0.5, 0.1) AS p50
		FROM CACHE('events')
	`, rows)
	if err != nil {
		t.Fatalf("execute percentile info: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("row count = %d, want 1", len(result.Rows))
	}
	info, ok := result.Rows[0]["p50"].(SQLApproxPercentileInfo)
	if !ok {
		t.Fatalf("p50 = %#v, want SQLApproxPercentileInfo", result.Rows[0]["p50"])
	}
	want := SQLApproxPercentileInfo{
		Value:     20,
		Quantile:  0.5,
		Count:     4,
		Epsilon:   0.1,
		RankError: 1,
	}
	if info != want {
		t.Fatalf("p50 = %#v, want %#v", info, want)
	}
}

func TestSQLApproximatePercentileInfoEmptyGroup(t *testing.T) {
	rows := approximateAggregateSource{{"latency": nil}}
	result, err := ExecuteSQLQuery(`
		SELECT APPROX_PERCENTILE_INFO(latency, 0.5) AS p50
		FROM CACHE('events')
	`, rows)
	if err != nil {
		t.Fatalf("execute empty percentile info: %v", err)
	}
	if result.Rows[0]["p50"] != nil {
		t.Fatalf("empty p50 = %#v, want nil", result.Rows[0]["p50"])
	}
}

func BenchmarkSQLApproximatePercentileScalar(b *testing.B) {
	benchmarkSQLApproximatePercentile(b, "APPROX_PERCENTILE(latency, 0.95)")
}

func BenchmarkSQLApproximatePercentileInfo(b *testing.B) {
	benchmarkSQLApproximatePercentile(b, "APPROX_PERCENTILE_INFO(latency, 0.95)")
}

func benchmarkSQLApproximatePercentile(b *testing.B, expression string) {
	rows := make(approximateAggregateSource, 10000)
	for index := range rows {
		rows[index] = SQLRow{"latency": float64(index % 1000)}
	}
	query := "SELECT " + expression + " AS percentile FROM CACHE('events')"
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(query, rows)
		if err != nil || len(result.Rows) != 1 {
			b.Fatalf("execute percentile benchmark: result=%#v err=%v", result, err)
		}
	}
}
