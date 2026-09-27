package hatSql_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMZ034SQLIncrementalProjectionAppliesSignedRows(t *testing.T) {
	compiled, err := hatSql.CompileSQLQuery("FROM CACHE('items') SELECT id AS id, value + 1 AS next_value WHERE value >= 0")
	if err != nil {
		t.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalProjection()
	if err != nil {
		t.Fatal(err)
	}
	input := []hatSql.DifferentialRow{
		{Key: "item-1", Time: 7, Diff: -3, Row: hatSql.Row{"id": "a", "value": int64(3)}},
		{Key: "item-2", Time: 8, Diff: 2, Row: hatSql.Row{"id": "b", "value": int64(-1)}},
	}
	got, err := operator.Apply(input)
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.DifferentialRow{{
		Key:  "item-1",
		Time: 7,
		Diff: -3,
		Row:  hatSql.Row{"id": "a", "next_value": int64(4)},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Apply() = %#v, want %#v", got, want)
	}
	got[0].Row["id"] = "changed"
	if input[0].Row["id"] != "a" {
		t.Fatal("projection output aliases the input row")
	}
}

func TestMZ034SQLIncrementalProjectionRejectsUnsupportedShapes(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') SELECT *",
		"FROM CACHE('items') SELECT COUNT(*) AS total",
		"FROM CACHE('items') SELECT value AS value GROUP BY value",
		"FROM CACHE('items') SELECT value AS value ORDER BY value",
		"FROM CACHE('items') SELECT value AS value LIMIT 1",
	}
	for _, source := range queries {
		compiled, err := hatSql.CompileSQLQuery(source)
		if err != nil {
			t.Fatalf("CompileSQLQuery(%q): %v", source, err)
		}
		if _, err := compiled.CompileIncrementalProjection(); !errors.Is(err, hatSql.ErrSQLIncrementalProjectionUnsupported) {
			t.Errorf("CompileIncrementalProjection(%q) error = %v, want unsupported", source, err)
		}
	}
}

func TestMZ034SQLIncrementalProjectionBatchErrorsAreAtomic(t *testing.T) {
	compiled, err := hatSql.CompileSQLQuery("FROM CACHE('items') SELECT value + 1 AS next_value")
	if err != nil {
		t.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalProjection()
	if err != nil {
		t.Fatal(err)
	}
	valid := hatSql.DifferentialRow{Key: "valid", Time: 1, Diff: 1, Row: hatSql.Row{"value": int64(1)}}
	_, err = operator.Apply([]hatSql.DifferentialRow{valid, {Key: "missing", Time: 1, Diff: 1}})
	if !errors.Is(err, hatSql.ErrSQLIncrementalProjectionRowRequired) {
		t.Fatalf("Apply() error = %v, want row-required", err)
	}
}

func TestMZ034SQLIncrementalProjectionNilOperator(t *testing.T) {
	var operator *hatSql.SQLIncrementalProjection
	if _, err := operator.Apply(nil); !errors.Is(err, hatSql.ErrSQLIncrementalProjectionNil) {
		t.Fatalf("nil Apply() error = %v, want nil-operator", err)
	}
}

var benchmarkMZ034SQLIncrementalProjectionSink []hatSql.DifferentialRow

func BenchmarkMZ034IncrementalSQLProjection(b *testing.B) {
	compiled, err := hatSql.CompileSQLQuery("FROM CACHE('items') SELECT id AS id, value + 1 AS next_value WHERE value >= 0")
	if err != nil {
		b.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalProjection()
	if err != nil {
		b.Fatal(err)
	}
	update := []hatSql.DifferentialRow{{
		Key:  "item-1",
		Time: 7,
		Diff: 1,
		Row:  hatSql.Row{"id": "a", "value": int64(3)},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := operator.Apply(update)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkMZ034SQLIncrementalProjectionSink = result
	}
}
