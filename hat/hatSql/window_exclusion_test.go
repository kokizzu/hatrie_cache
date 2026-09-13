package hatSql

import "testing"

func TestSQLWindowExcludeCurrentRow(t *testing.T) {
	rows := approximateAggregateSource{
		{"id": int64(1), "value": 1.0},
		{"id": int64(2), "value": 2.0},
		{"id": int64(3), "value": 3.0},
	}
	result, err := ExecuteSQLQuery(`
		SELECT id,
			SUM(value) OVER (
				ORDER BY id
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
				EXCLUDE CURRENT ROW
			) AS prior_sum
		FROM CACHE('events')
		ORDER BY id
	`, rows)
	if err != nil {
		t.Fatalf("execute EXCLUDE CURRENT ROW: %v", err)
	}
	want := []interface{}{nil, float64(1), float64(3)}
	for index, expected := range want {
		if got := result.Rows[index]["prior_sum"]; got != expected {
			t.Fatalf("row %d prior_sum = %#v, want %#v", index, got, expected)
		}
	}
}

func TestSQLWindowExcludeGroupAndTies(t *testing.T) {
	rows := approximateAggregateSource{
		{"id": int64(1), "peer": "a", "value": 1.0},
		{"id": int64(2), "peer": "a", "value": 2.0},
		{"id": int64(3), "peer": "b", "value": 4.0},
		{"id": int64(4), "peer": "b", "value": 8.0},
		{"id": int64(5), "peer": "c", "value": 16.0},
	}
	result, err := ExecuteSQLQuery(`
		SELECT id,
			SUM(value) OVER (
				ORDER BY peer
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
				EXCLUDE GROUP
			) AS without_group,
			SUM(value) OVER (
				ORDER BY peer
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
				EXCLUDE TIES
			) AS without_ties
		FROM CACHE('events')
		ORDER BY id
	`, rows)
	if err != nil {
		t.Fatalf("execute peer exclusions: %v", err)
	}
	wantGroup := []interface{}{nil, nil, float64(3), float64(3), float64(15)}
	wantTies := []interface{}{float64(1), float64(2), float64(7), float64(11), float64(31)}
	for index := range rows {
		if got := result.Rows[index]["without_group"]; got != wantGroup[index] {
			t.Fatalf("row %d without_group = %#v, want %#v", index, got, wantGroup[index])
		}
		if got := result.Rows[index]["without_ties"]; got != wantTies[index] {
			t.Fatalf("row %d without_ties = %#v, want %#v", index, got, wantTies[index])
		}
	}
}

func TestSQLWindowExcludeNoOthersAndRejectsUnknownMode(t *testing.T) {
	rows := approximateAggregateSource{{"id": int64(1), "value": 1.0}}
	result, err := ExecuteSQLQuery(`
		SELECT SUM(value) OVER (
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS
		) AS total
		FROM CACHE('events')
	`, rows)
	if err != nil {
		t.Fatalf("EXCLUDE NO OTHERS: %v", err)
	}
	if got := result.Rows[0]["total"]; got != float64(1) {
		t.Fatalf("EXCLUDE NO OTHERS total = %#v, want 1", got)
	}
	if _, err := ExecuteSQLQuery(`
		SELECT SUM(value) OVER (
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE INVALID
		) AS total
		FROM CACHE('events')
	`, rows); err == nil {
		t.Fatal("unknown EXCLUDE mode succeeded, want error")
	}
}

func TestSQLWindowExcludeAppliesToArgExtreme(t *testing.T) {
	rows := approximateAggregateSource{
		{"id": int64(1), "payload": "first", "score": 10.0},
		{"id": int64(2), "payload": "second", "score": 20.0},
		{"id": int64(3), "payload": "third", "score": 30.0},
	}
	result, err := ExecuteSQLQuery(`
		SELECT id,
			ARGMAX(payload, score) OVER (
				ORDER BY id
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
				EXCLUDE CURRENT ROW
			) AS previous_best
		FROM CACHE('events')
		ORDER BY id
	`, rows)
	if err != nil {
		t.Fatalf("execute ARGMAX exclusion: %v", err)
	}
	want := []interface{}{nil, "first", "second"}
	for index, expected := range want {
		if got := result.Rows[index]["previous_best"]; got != expected {
			t.Fatalf("row %d previous_best = %#v, want %#v", index, got, expected)
		}
	}
}

func BenchmarkSQLWindowFrameExclusion(b *testing.B) {
	rows := make(approximateAggregateSource, 2000)
	for index := range rows {
		rows[index] = SQLRow{"id": int64(index), "value": float64(index % 100)}
	}
	benchmarks := []struct {
		name  string
		query string
	}{
		{
			name:  "control",
			query: `SELECT SUM(value) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS rolling FROM CACHE('events')`,
		},
		{
			name:  "exclude-current",
			query: `SELECT SUM(value) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS rolling FROM CACHE('events')`,
		},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err := ExecuteSQLQuery(benchmark.query, rows)
				if err != nil || len(result.Rows) != len(rows) {
					b.Fatalf("execute window: rows=%d err=%v", len(result.Rows), err)
				}
			}
		})
	}
}
