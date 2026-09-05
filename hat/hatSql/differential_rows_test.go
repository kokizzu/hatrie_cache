package hatSql_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestConsolidateDifferentialRowsPreservesMultisetSemantics(t *testing.T) {
	input := []hatSql.DifferentialRow{
		{Key: "ada", Time: 1, Diff: 2, Row: hatSql.Row{"name": "Ada"}},
		{Key: "lin", Time: 1, Diff: 1, Row: hatSql.Row{"name": "Lin"}},
		{Key: "ada", Time: 1, Diff: -1, Row: hatSql.Row{"name": "Ada"}},
		{Key: "ada", Time: 2, Diff: -3, Row: hatSql.Row{"name": "Ada"}},
		{Key: "lin", Time: 1, Diff: -1, Row: hatSql.Row{"name": "Lin"}},
		{Key: "ada", Time: 1, Diff: 4, Row: hatSql.Row{"name": "Ada"}},
		{Key: "zero", Time: 1, Diff: 0, Row: hatSql.Row{"name": "ignored"}},
	}
	want := []hatSql.DifferentialRow{
		{Key: "ada", Time: 1, Diff: 5, Row: hatSql.Row{"name": "Ada"}},
		{Key: "ada", Time: 2, Diff: -3, Row: hatSql.Row{"name": "Ada"}},
	}
	got, err := hatSql.ConsolidateDifferentialRows(input)
	if err != nil {
		t.Fatalf("ConsolidateDifferentialRows() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("consolidated rows = %#v, want %#v", got, want)
	}
	if input[0].Diff != 2 || input[2].Diff != -1 {
		t.Fatalf("input was mutated: %#v", input)
	}
	got[0].Row["name"] = "changed"
	if input[0].Row["name"] != "Ada" {
		t.Fatal("consolidated row aliases input row map")
	}
}

func TestConsolidateDifferentialRowsRejectsInvalidKeysAndOverflow(t *testing.T) {
	for _, test := range []struct {
		name string
		rows []hatSql.DifferentialRow
	}{
		{name: "empty key", rows: []hatSql.DifferentialRow{{Time: 1, Diff: 1}}},
		{name: "positive overflow", rows: []hatSql.DifferentialRow{{Key: "x", Diff: 1}, {Key: "x", Diff: 1<<63 - 1}}},
		{name: "negative overflow", rows: []hatSql.DifferentialRow{{Key: "x", Diff: -1}, {Key: "x", Diff: -1 << 63}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := hatSql.ConsolidateDifferentialRows(test.rows); err == nil {
				t.Fatal("ConsolidateDifferentialRows() error = nil")
			}
		})
	}
}

func TestConsolidateDifferentialRowsHandlesEmptyInput(t *testing.T) {
	got, err := hatSql.ConsolidateDifferentialRows(nil)
	if err != nil || got != nil {
		t.Fatalf("empty consolidation = %#v, %v", got, err)
	}
}

func BenchmarkConsolidateDifferentialRows(b *testing.B) {
	rows := make([]hatSql.DifferentialRow, 1024)
	for index := range rows {
		key := "odd"
		if index%2 == 0 {
			key = "even"
		}
		rows[index] = hatSql.DifferentialRow{
			Key:  key,
			Time: uint64(index % 16),
			Diff: 1,
			Row:  hatSql.Row{"key": key},
		}
	}
	b.ReportAllocs()
	for range b.N {
		if _, err := hatSql.ConsolidateDifferentialRows(rows); err != nil {
			b.Fatal(err)
		}
	}
}
