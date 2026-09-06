package hatSql

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestSQLWithFillAddsMissingTimeBuckets(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []Row{
		{"ts": start, "value": int64(1)},
		{"ts": start.Add(2 * time.Minute), "value": int64(2)},
	}
	result, err := ExecuteSQLQuery(`
		SELECT ts, SUM(value) AS total
		FROM CACHE('events')
		GROUP BY ts
		ORDER BY ts WITH FILL
		FROM TIMESTAMP '2026-01-01T00:00:00Z'
		TO TIMESTAMP '2026-01-01T00:04:00Z'
		STEP DURATION '1m'`, SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil }))
	if err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"ts": start, "total": float64(1)},
		{"ts": start.Add(time.Minute), "total": nil},
		{"ts": start.Add(2 * time.Minute), "total": float64(2)},
		{"ts": start.Add(3 * time.Minute), "total": nil},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("filled rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLWithFillEnforcesResultLimit(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []Row{{"ts": start, "value": int64(1)}}
	_, err := ExecuteSQLQueryContext(nil, `
		SELECT ts, SUM(value) AS total
		FROM CACHE('events')
		GROUP BY ts
		ORDER BY ts WITH FILL
		FROM TIMESTAMP '2026-01-01T00:00:00Z'
		TO TIMESTAMP '2026-01-01T00:10:00Z'
		STEP DURATION '1m'`, SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil }), SQLQueryOptions{MaxRows: 3})
	if err == nil {
		t.Fatal("WITH FILL exceeded MaxRows without an error")
	}
}

func TestSQLWithFillResolvesAliasBeforeApplyingLimit(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []Row{
		{"ts": start, "value": int64(1)},
		{"ts": start.Add(2 * time.Minute), "value": int64(2)},
	}
	result, err := ExecuteSQLQuery(`
		SELECT ts AS bucket, SUM(value) AS total
		FROM CACHE('events')
		GROUP BY ts
		ORDER BY bucket WITH FILL
		FROM TIMESTAMP '2026-01-01T00:00:00Z'
		TO TIMESTAMP '2026-01-01T00:04:00Z'
		STEP DURATION '1m'
		LIMIT 2 OFFSET 1`, SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil }))
	if err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"bucket": start.Add(time.Minute), "total": nil},
		{"bucket": start.Add(2 * time.Minute), "total": float64(2)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("limited filled rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLWithFillPopulatesEmptyInput(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := ExecuteSQLQuery(`
		SELECT ts, value
		FROM CACHE('events')
		ORDER BY ts WITH FILL
		FROM TIMESTAMP '2026-01-01T00:00:00Z'
		TO TIMESTAMP '2026-01-01T00:03:00Z'
		STEP DURATION '1m'`, SourceResolverFunc(func(string, string) ([]Row, error) { return nil, nil }))
	if err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"ts": start, "value": nil},
		{"ts": start.Add(time.Minute), "value": nil},
		{"ts": start.Add(2 * time.Minute), "value": nil},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("empty-input filled rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLWithFillUsesMaterializedRowsForStreamingAPI(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []Row{
		{"ts": start, "value": int64(1)},
		{"ts": start.Add(2 * time.Minute), "value": int64(2)},
	}
	query := `
		SELECT ts, SUM(value) AS total
		FROM CACHE('events')
		GROUP BY ts
		ORDER BY ts WITH FILL
		FROM TIMESTAMP '2026-01-01T00:00:00Z'
		TO TIMESTAMP '2026-01-01T00:04:00Z'
		STEP DURATION '1m'`
	var got []Row
	err := ExecuteSQLQueryRows(context.Background(), query, SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil }), nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		got = append(got, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[1]["total"] != nil || got[3]["total"] != nil {
		t.Fatalf("streamed fill rows = %#v, want four rows with two NULL buckets", got)
	}
}

func TestSQLWithFillRejectsUnsupportedOrderForms(t *testing.T) {
	queries := []string{
		`SELECT ts FROM CACHE('events') ORDER BY ts DESC WITH FILL FROM TIMESTAMP '2026-01-01T00:00:00Z' TO TIMESTAMP '2026-01-01T00:03:00Z' STEP DURATION '1m'`,
		`SELECT ts, value FROM CACHE('events') ORDER BY ts WITH FILL FROM TIMESTAMP '2026-01-01T00:00:00Z' TO TIMESTAMP '2026-01-01T00:03:00Z' STEP DURATION '1m', value`,
	}
	for _, query := range queries {
		if _, err := ExecuteSQLQuery(query, SourceResolverFunc(func(string, string) ([]Row, error) { return nil, nil })); err == nil {
			t.Fatalf("query unexpectedly accepted: %s", query)
		}
	}
}

func BenchmarkSQLWithFill(b *testing.B) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]Row, 0, 50)
	for index := 0; index < 50; index += 2 {
		rows = append(rows, Row{"ts": start.Add(time.Duration(index) * time.Minute), "value": int64(index)})
	}
	query := `SELECT ts, SUM(value) AS total FROM CACHE('events') GROUP BY ts ORDER BY ts WITH FILL FROM TIMESTAMP '2026-01-01T00:00:00Z' TO TIMESTAMP '2026-01-01T00:50:00Z' STEP DURATION '1m'`
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil })
	b.ReportAllocs()
	for range b.N {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 50 {
			b.Fatalf("filled rows = %d, want 50", len(result.Rows))
		}
	}
}

func BenchmarkSQLWithoutFill(b *testing.B) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]Row, 0, 50)
	for index := 0; index < 50; index += 2 {
		rows = append(rows, Row{"ts": start.Add(time.Duration(index) * time.Minute), "value": int64(index)})
	}
	query := `SELECT ts, SUM(value) AS total FROM CACHE('events') GROUP BY ts ORDER BY ts`
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil })
	b.ReportAllocs()
	for range b.N {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 25 {
			b.Fatalf("unfilled rows = %d, want 25", len(result.Rows))
		}
	}
}
