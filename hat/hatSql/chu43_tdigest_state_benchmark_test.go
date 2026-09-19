package hatSql

import "testing"

func benchmarkCHU43TDigestRows(size int) approximateAggregateSource {
	rows := make(approximateAggregateSource, size)
	for index := range rows {
		rows[index] = SQLRow{"latency": float64((index % 1000) + 1)}
	}
	return rows
}

func BenchmarkCHU43TDigestBaseline(b *testing.B) {
	rows := benchmarkCHU43TDigestRows(10000)
	query := `SELECT APPROX_TDIGEST_PERCENTILE(latency, 0.99, 100) AS p99 FROM CACHE('events')`
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := ExecuteSQLQuery(query, rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU43TDigestState(b *testing.B) {
	rows := benchmarkCHU43TDigestRows(10000)
	query := `SELECT APPROX_TDIGEST_PERCENTILE_STATE(latency, 100) AS state FROM CACHE('events')`
	result, err := ExecuteSQLQuery(query, rows)
	if err != nil {
		b.Fatal(err)
	}
	wire, ok := result.Rows[0]["state"].([]byte)
	if !ok {
		b.Fatalf("state result = %#v, want bytes", result.Rows[0]["state"])
	}
	b.ReportMetric(float64(len(wire)), "wire-bytes/op")
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(query, rows)
		if err != nil {
			b.Fatal(err)
		}
		if result.Rows[0]["state"] == nil {
			b.Fatal("state result is nil")
		}
	}
}

func BenchmarkCHU43TDigestMerge(b *testing.B) {
	left := benchmarkCHU43TDigestRows(5000)
	right := benchmarkCHU43TDigestRows(5000)
	leftState, err := ExecuteSQLQuery(`SELECT APPROX_TDIGEST_PERCENTILE_STATE(latency, 100) AS state FROM CACHE('events')`, left)
	if err != nil {
		b.Fatal(err)
	}
	rightState, err := ExecuteSQLQuery(`SELECT APPROX_TDIGEST_PERCENTILE_STATE(latency, 100) AS state FROM CACHE('events')`, right)
	if err != nil {
		b.Fatal(err)
	}
	rows := approximateAggregateSource{
		{"state": leftState.Rows[0]["state"]},
		{"state": rightState.Rows[0]["state"]},
	}
	query := `SELECT APPROX_TDIGEST_PERCENTILE_MERGE(state, 0.99) AS p99 FROM CACHE('states')`
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := ExecuteSQLQuery(query, rows); err != nil {
			b.Fatal(err)
		}
	}
}
