package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type mz034SQLFilterBenchmarkResolver struct {
	rows []SQLRow
}

func (resolver mz034SQLFilterBenchmarkResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return resolver.rows, nil
}

func mz034SQLFilterBenchmarkRows(count int) []SQLRow {
	rows := make([]SQLRow, count)
	for index := range rows {
		rows[index] = SQLRow{
			"id":     int64(index),
			"active": index%2 == 0,
			"region": "region-" + string(rune('a'+index%8)),
			"value":  int64(index * 3),
		}
	}
	return rows
}

func BenchmarkMZ034RebuildSQLFilter(b *testing.B) {
	query, err := CompileSQLQuery("SELECT * FROM CACHE('events') WHERE active = TRUE")
	if err != nil {
		b.Fatal(err)
	}
	resolver := mz034SQLFilterBenchmarkResolver{rows: mz034SQLFilterBenchmarkRows(10000)}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := query.Execute(context.Background(), resolver, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func TestMZ034CompileIncrementalFilterAppliesSignedRows(t *testing.T) {
	compiled, err := CompileSQLQuery("SELECT * FROM CACHE('events') AS e WHERE e.active = TRUE")
	if err != nil {
		t.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalFilter()
	if err != nil {
		t.Fatal(err)
	}

	input := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"id": int64(1), "active": true}},
		{Key: "b", Time: 1, Diff: 1, Row: Row{"id": int64(2), "active": false}},
		{Key: "c", Time: 1, Diff: 1, Row: Row{"id": int64(3)}},
	}
	changes, err := operator.Apply(input)
	if err != nil {
		t.Fatal(err)
	}
	want := []DifferentialRow{{Key: "a", Time: 1, Diff: 1, Row: Row{"id": int64(1), "active": true}}}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("changes = %#v, want %#v", changes, want)
	}

	input[0].Row["id"] = int64(99)
	if changes[0].Row["id"] != int64(1) {
		t.Fatal("filter output leaked input row mutation")
	}

	changes, err = operator.Apply([]DifferentialRow{{Key: "a", Time: 2, Diff: -1, Row: Row{"id": int64(1), "active": true}}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(changes, []DifferentialRow{{Key: "a", Time: 2, Diff: -1, Row: Row{"id": int64(1), "active": true}}}) {
		t.Fatalf("retraction changes = %#v", changes)
	}
	if changes, err := operator.Apply([]DifferentialRow{{Key: "ignored", Diff: 0}}); err != nil || changes != nil {
		t.Fatalf("zero-diff changes = %#v, err=%v; want nil, nil", changes, err)
	}
}

func TestMZ034CompileIncrementalFilterRejectsUnsupportedShapes(t *testing.T) {
	tests := []string{
		"SELECT id FROM CACHE('events')",
		"SELECT * FROM CACHE('events') ORDER BY id",
		"SELECT * FROM CACHE('events') GROUP BY active",
	}
	for _, source := range tests {
		compiled, err := CompileSQLQuery(source)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := compiled.CompileIncrementalFilter(); !errors.Is(err, ErrSQLIncrementalFilterUnsupported) {
			t.Fatalf("CompileIncrementalFilter(%q) error = %v, want ErrSQLIncrementalFilterUnsupported", source, err)
		}
	}
	var nilQuery *CompiledSQLQuery
	if _, err := nilQuery.CompileIncrementalFilter(); !errors.Is(err, ErrSQLIncrementalFilterUnsupported) {
		t.Fatalf("nil query error = %v, want ErrSQLIncrementalFilterUnsupported", err)
	}
}

func TestMZ034CompileIncrementalFilterBatchErrorsAreAtomic(t *testing.T) {
	compiled, err := CompileSQLQuery("SELECT * FROM CACHE('events') WHERE active = TRUE")
	if err != nil {
		t.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalFilter()
	if err != nil {
		t.Fatal(err)
	}
	changes, err := operator.Apply([]DifferentialRow{
		{Key: "ok", Diff: 1, Row: Row{"active": true}},
		{Key: "bad", Diff: 1},
	})
	if !errors.Is(err, ErrSQLIncrementalFilterRowRequired) {
		t.Fatalf("batch error = %v, want ErrSQLIncrementalFilterRowRequired", err)
	}
	if changes != nil {
		t.Fatalf("changes from rejected batch = %#v, want nil", changes)
	}
	if _, err := operator.Apply([]DifferentialRow{{Key: "ok", Diff: 1, Row: Row{"active": true}}}); err != nil {
		t.Fatalf("valid batch after rejected batch: %v", err)
	}
}

var benchmarkMZ034IncrementalSQLFilterSink []DifferentialRow

func BenchmarkMZ034IncrementalSQLFilter(b *testing.B) {
	compiled, err := CompileSQLQuery("SELECT * FROM CACHE('events') WHERE active = TRUE")
	if err != nil {
		b.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalFilter()
	if err != nil {
		b.Fatal(err)
	}
	update := []DifferentialRow{{Key: "hot", Time: 1, Diff: 1, Row: Row{"id": int64(1), "active": true}}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		changes, err := operator.Apply(update)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkMZ034IncrementalSQLFilterSink = changes
	}
}
