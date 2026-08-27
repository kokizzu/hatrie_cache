package hatSql

import (
	"strings"
	"testing"
)

type approximateAggregateSource []SQLRow

func (rows approximateAggregateSource) ResolveSQLSource(name string, key string) ([]SQLRow, error) {
	return rows, nil
}

func TestSQLApproximateAggregates(t *testing.T) {
	rows := approximateAggregateSource{
		{"region": "east", "visitor": "a", "latency": 10.0, "state": "queued"},
		{"region": "east", "visitor": "b", "latency": 20.0, "state": "queued"},
		{"region": "east", "visitor": "a", "latency": 30.0, "state": "running"},
		{"region": "east", "visitor": "c", "latency": 40.0, "state": "queued"},
		{"region": "west", "visitor": "d", "latency": 50.0, "state": "failed"},
		{"region": "west", "visitor": "e", "latency": 60.0, "state": "failed"},
	}

	result, err := ExecuteSQLQuery(`
		SELECT region,
			APPROX_COUNT_DISTINCT(visitor, 10) AS visitors,
			APPROX_PERCENTILE(latency, 0.5, 0.01) AS p50,
			APPROX_TOP_K(state, 2) AS states
		FROM CACHE('events')
		GROUP BY region
		ORDER BY region
	`, rows)
	if err != nil {
		t.Fatalf("execute approximate aggregates: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("row count = %d, want 2", len(result.Rows))
	}
	east := result.Rows[0]
	if got, ok := east["visitors"].(uint64); !ok || got != 3 {
		t.Fatalf("east visitors = %#v, want uint64(3)", east["visitors"])
	}
	if got, ok := east["p50"].(float64); !ok || got != 20 {
		t.Fatalf("east p50 = %#v, want float64(20)", east["p50"])
	}
	states, ok := east["states"].([]SQLApproxTopKItem)
	if !ok {
		t.Fatalf("east states = %#v, want []SQLApproxTopKItem", east["states"])
	}
	if len(states) != 2 || states[0] != (SQLApproxTopKItem{Value: "queued", Estimate: 3}) || states[1] != (SQLApproxTopKItem{Value: "running", Estimate: 1}) {
		t.Fatalf("east states = %#v, want queued then running", states)
	}
	west := result.Rows[1]
	if got, ok := west["visitors"].(uint64); !ok || got != 2 {
		t.Fatalf("west visitors = %#v, want uint64(2)", west["visitors"])
	}
	if got, ok := west["p50"].(float64); !ok || got != 50 {
		t.Fatalf("west p50 = %#v, want float64(50)", west["p50"])
	}
}

func TestSQLApproximateAggregateValidation(t *testing.T) {
	rows := approximateAggregateSource{{"value": "a", "latency": 10.0}}
	for _, query := range []string{
		`SELECT APPROX_COUNT_DISTINCT(value, 1) FROM CACHE('events')`,
		`SELECT APPROX_PERCENTILE(latency, 1.1) FROM CACHE('events')`,
		`SELECT APPROX_TOP_K(value, 0) FROM CACHE('events')`,
	} {
		if _, err := ExecuteSQLQuery(query, rows); err == nil {
			t.Fatalf("query %q succeeded, want validation error", query)
		} else if strings.Contains(err.Error(), "unknown SQL function") {
			t.Fatalf("query %q returned unknown function error: %v", query, err)
		}
	}
}

func BenchmarkSQLApproximateAggregates(b *testing.B) {
	rows := make(approximateAggregateSource, 10000)
	for index := range rows {
		rows[index] = SQLRow{
			"visitor": "visitor-" + string(rune('a'+index%26)) + string(rune('a'+index/26%26)),
			"latency": float64(index % 1000),
			"state":   []string{"queued", "running", "failed", "done"}[index%4],
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(`
			SELECT APPROX_COUNT_DISTINCT(visitor) AS visitors,
				APPROX_PERCENTILE(latency, 0.95) AS p95,
				APPROX_TOP_K(state, 4) AS states
			FROM CACHE('events')
		`, rows)
		if err != nil || len(result.Rows) != 1 {
			b.Fatalf("execute approximate aggregates: result=%#v err=%v", result, err)
		}
	}
}
