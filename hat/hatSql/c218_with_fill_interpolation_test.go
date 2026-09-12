package hatSql_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestC218FillSQLRowsInterpolatesPreviousNextAndLinearValues(t *testing.T) {
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	rows := []hatSql.Row{
		{"at": start.Add(2 * time.Hour), "linear": int64(20), "previous": "late", "next": "late"},
		{"at": start.Add(4 * time.Hour), "linear": int64(40), "previous": "latest", "next": "latest"},
	}
	got, err := hatSql.FillSQLRows(rows, hatSql.SQLWithFillSpec{
		Column: "at",
		From:   start,
		To:     start.Add(6 * time.Hour),
		Step:   time.Hour,
		Interpolation: map[string]hatSql.SQLWithFillInterpolation{
			"linear":   hatSql.SQLWithFillInterpolationLinear,
			"previous": hatSql.SQLWithFillInterpolationPrevious,
			"next":     hatSql.SQLWithFillInterpolationNext,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.Row{
		{"at": start, "linear": nil, "previous": nil, "next": "late"},
		{"at": start.Add(time.Hour), "linear": nil, "previous": nil, "next": "late"},
		rows[0],
		{"at": start.Add(3 * time.Hour), "linear": float64(30), "previous": "late", "next": "latest"},
		rows[1],
		{"at": start.Add(5 * time.Hour), "linear": nil, "previous": "latest", "next": nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("interpolated rows = %#v, want %#v", got, want)
	}
}

func TestC218WithFillSQLSyntaxInterpolatesProjectedValues(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []hatSql.Row{
		{"ts": start, "value": int64(10)},
		{"ts": start.Add(2 * time.Minute), "value": int64(30)},
	}
	result, err := hatSql.ExecuteSQLQuery(`
		SELECT ts, value
		FROM CACHE('events')
		ORDER BY ts WITH FILL
		FROM TIMESTAMP '2026-01-01T00:00:00Z'
		TO TIMESTAMP '2026-01-01T00:04:00Z'
		STEP DURATION '1m'
		INTERPOLATE (value LINEAR)`, hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return rows, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.Row{
		{"ts": start, "value": int64(10)},
		{"ts": start.Add(time.Minute), "value": float64(20)},
		{"ts": start.Add(2 * time.Minute), "value": int64(30)},
		{"ts": start.Add(3 * time.Minute), "value": nil},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("SQL interpolated rows = %#v, want %#v", result.Rows, want)
	}
}

func TestC218WithFillSQLDefaultsToPreviousPolicy(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := hatSql.ExecuteSQLQuery(
		"SELECT ts, value FROM CACHE('events') ORDER BY ts WITH FILL FROM TIMESTAMP '2026-01-01T00:00:00Z' TO TIMESTAMP '2026-01-01T00:04:00Z' STEP DURATION '1m' INTERPOLATE (value)",
		hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
			return []hatSql.Row{
				{"ts": start, "value": int64(10)},
				{"ts": start.Add(2 * time.Minute), "value": int64(30)},
			}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.Row{
		{"ts": start, "value": int64(10)},
		{"ts": start.Add(time.Minute), "value": int64(10)},
		{"ts": start.Add(2 * time.Minute), "value": int64(30)},
		{"ts": start.Add(3 * time.Minute), "value": int64(30)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("default interpolation rows = %#v, want %#v", result.Rows, want)
	}
}

func TestC218LinearInterpolationLeavesUnsupportedEndpointsEmpty(t *testing.T) {
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	got, err := hatSql.FillSQLRows([]hatSql.Row{
		{"at": start, "value": "not numeric"},
		{"at": start.Add(2 * time.Hour), "value": "still not numeric"},
	}, hatSql.SQLWithFillSpec{
		Column: "at",
		From:   start,
		To:     start.Add(3 * time.Hour),
		Step:   time.Hour,
		Interpolation: map[string]hatSql.SQLWithFillInterpolation{
			"value": hatSql.SQLWithFillInterpolationLinear,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got[1]["value"] != nil {
		t.Fatalf("unsupported linear value = %#v, want nil", got[1]["value"])
	}
}

func TestC218WithFillRejectsInvalidInterpolationConfiguration(t *testing.T) {
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	for _, interpolations := range []map[string]hatSql.SQLWithFillInterpolation{
		{"value": hatSql.SQLWithFillInterpolation("UNKNOWN")},
		{"": hatSql.SQLWithFillInterpolationPrevious},
	} {
		_, err := hatSql.FillSQLRows([]hatSql.Row{{"at": start}}, hatSql.SQLWithFillSpec{
			Column:        "at",
			From:          start,
			To:            start.Add(time.Hour),
			Step:          time.Hour,
			Interpolation: interpolations,
		})
		if !errors.Is(err, hatSql.ErrSQLWithFillInvalid) {
			t.Fatalf("invalid interpolation error = %v, want WITH FILL error", err)
		}
	}

	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"ts": start, "value": int64(1)}}, nil
	})
	for _, query := range []string{
		`SELECT ts, value FROM CACHE('events') ORDER BY ts WITH FILL FROM TIMESTAMP '2026-09-06T00:00:00Z' TO TIMESTAMP '2026-09-06T00:02:00Z' STEP DURATION '1m' INTERPOLATE ()`,
		`SELECT ts, value FROM CACHE('events') ORDER BY ts WITH FILL FROM TIMESTAMP '2026-09-06T00:00:00Z' TO TIMESTAMP '2026-09-06T00:02:00Z' STEP DURATION '1m' INTERPOLATE (missing LINEAR)`,
		`SELECT ts, value FROM CACHE('events') ORDER BY ts WITH FILL FROM TIMESTAMP '2026-09-06T00:00:00Z' TO TIMESTAMP '2026-09-06T00:02:00Z' STEP DURATION '1m' INTERPOLATE (value, VALUE)`,
	} {
		if _, err := hatSql.ExecuteSQLQuery(query, resolver); err == nil {
			t.Fatalf("invalid interpolation query unexpectedly succeeded: %s", query)
		}
	}
}

func BenchmarkC218FillInterpolationBaseline(b *testing.B) {
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	rows := []hatSql.Row{{"at": start, "value": int64(1)}, {"at": start.Add(2 * time.Hour), "value": int64(3)}}
	spec := hatSql.SQLWithFillSpec{Column: "at", From: start, To: start.Add(100 * time.Hour), Step: time.Hour}
	b.ReportAllocs()
	for range b.N {
		if _, err := hatSql.FillSQLRows(rows, spec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkC218FillInterpolationLinear(b *testing.B) {
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	rows := []hatSql.Row{{"at": start, "value": int64(1)}, {"at": start.Add(2 * time.Hour), "value": int64(3)}}
	spec := hatSql.SQLWithFillSpec{
		Column: "at",
		From:   start,
		To:     start.Add(100 * time.Hour),
		Step:   time.Hour,
		Interpolation: map[string]hatSql.SQLWithFillInterpolation{
			"value": hatSql.SQLWithFillInterpolationLinear,
		},
	}
	b.ReportAllocs()
	for range b.N {
		if _, err := hatSql.FillSQLRows(rows, spec); err != nil {
			b.Fatal(err)
		}
	}
}
