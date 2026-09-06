package hatSql_test

import (
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestFillSQLRowsAddsMissingOrderedTimeBuckets(t *testing.T) {
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	rows := []hatSql.Row{
		{"at": start, "series": "cpu", "value": int64(10)},
		{"at": start.Add(2 * time.Hour), "series": "cpu", "value": int64(30)},
	}
	template := hatSql.Row{"series": "cpu", "value": int64(0)}
	got, err := hatSql.FillSQLRows(rows, hatSql.SQLWithFillSpec{
		Column:   "at",
		From:     start,
		To:       start.Add(4 * time.Hour),
		Step:     time.Hour,
		Template: template,
	})
	if err != nil {
		t.Fatalf("FillSQLRows() error = %v", err)
	}
	want := []hatSql.Row{
		{"at": start, "series": "cpu", "value": int64(10)},
		{"at": start.Add(time.Hour), "series": "cpu", "value": int64(0)},
		{"at": start.Add(2 * time.Hour), "series": "cpu", "value": int64(30)},
		{"at": start.Add(3 * time.Hour), "series": "cpu", "value": int64(0)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FillSQLRows() = %#v, want %#v", got, want)
	}
	got[1]["series"] = "changed"
	if template["series"] != "cpu" || rows[0]["series"] != "cpu" {
		t.Fatal("FillSQLRows() aliased the template or source rows")
	}
}

func TestFillSQLRowsSupportsLeadingAndEmptyGaps(t *testing.T) {
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	got, err := hatSql.FillSQLRows([]hatSql.Row{{"at": start.Add(2 * time.Hour), "value": int64(2)}}, hatSql.SQLWithFillSpec{
		Column: "at",
		From:   start,
		To:     start.Add(4 * time.Hour),
		Step:   time.Hour,
	})
	if err != nil {
		t.Fatalf("FillSQLRows() error = %v", err)
	}
	if len(got) != 4 || !got[0]["at"].(time.Time).Equal(start) || !got[1]["at"].(time.Time).Equal(start.Add(time.Hour)) || !got[3]["at"].(time.Time).Equal(start.Add(3*time.Hour)) {
		t.Fatalf("FillSQLRows() = %#v, want four hourly rows", got)
	}
}

func TestFillSQLRowsKeepsGridAfterOffGridRows(t *testing.T) {
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	got, err := hatSql.FillSQLRows([]hatSql.Row{
		{"at": start.Add(30 * time.Minute)},
		{"at": start.Add(90 * time.Minute)},
	}, hatSql.SQLWithFillSpec{Column: "at", From: start, To: start.Add(3 * time.Hour), Step: time.Hour})
	if err != nil {
		t.Fatalf("FillSQLRows() error = %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("FillSQLRows() returned %d rows, want 5: %#v", len(got), got)
	}
	for index, want := range []time.Time{
		start,
		start.Add(30 * time.Minute),
		start.Add(time.Hour),
		start.Add(90 * time.Minute),
		start.Add(2 * time.Hour),
	} {
		if !got[index]["at"].(time.Time).Equal(want) {
			t.Fatalf("row %d time = %v, want %v", index, got[index]["at"], want)
		}
	}
}

func TestFillSQLRowsRejectsInvalidInput(t *testing.T) {
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	valid := hatSql.SQLWithFillSpec{Column: "at", From: start, To: start.Add(time.Hour), Step: time.Hour}
	tests := []struct {
		name string
		rows []hatSql.Row
		spec hatSql.SQLWithFillSpec
	}{
		{name: "zero step", spec: hatSql.SQLWithFillSpec{Column: "at", From: start, To: start.Add(time.Hour)}},
		{name: "reversed bounds", spec: hatSql.SQLWithFillSpec{Column: "at", From: start.Add(time.Hour), To: start, Step: time.Hour}},
		{name: "missing column", rows: []hatSql.Row{{"value": int64(1)}}, spec: valid},
		{name: "wrong type", rows: []hatSql.Row{{"at": "not-time"}}, spec: valid},
		{name: "unsorted", rows: []hatSql.Row{{"at": start.Add(time.Hour)}, {"at": start}}, spec: hatSql.SQLWithFillSpec{Column: "at", From: start, To: start.Add(2 * time.Hour), Step: time.Hour}},
		{name: "outside bounds", rows: []hatSql.Row{{"at": start.Add(-time.Hour)}}, spec: valid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := hatSql.FillSQLRows(test.rows, test.spec); err == nil {
				t.Fatal("FillSQLRows() error = nil")
			}
		})
	}
}

func BenchmarkFillSQLRows(b *testing.B) {
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	rows := make([]hatSql.Row, 5000)
	for i := range rows {
		rows[i] = hatSql.Row{"at": start.Add(time.Duration(i*2) * time.Minute), "value": int64(i)}
	}
	spec := hatSql.SQLWithFillSpec{Column: "at", From: start, To: start.Add(10000 * time.Minute), Step: time.Minute}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := hatSql.FillSQLRows(rows, spec); err != nil {
			b.Fatal(err)
		}
	}
}
