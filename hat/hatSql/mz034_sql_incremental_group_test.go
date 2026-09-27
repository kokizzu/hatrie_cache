package hatSql

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

const mz034SQLGroupQuery = "FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total, SUM(src.score) AS total_score GROUP BY src.group"

func mz034SQLGroupUpdates() []DifferentialRow {
	return []DifferentialRow{
		{Key: "row-1", Time: 1, Diff: 1, Row: Row{"group": "red", "score": int64(3)}},
		{Key: "row-2", Time: 2, Diff: 1, Row: Row{"group": "red", "score": int64(5)}},
		{Key: "row-3", Time: 3, Diff: 1, Row: Row{"group": "blue", "score": int64(7)}},
	}
}

func mz034SQLGroupRowsByKey(rows []DifferentialRow) map[string]SQLRow {
	result := make(map[string]SQLRow)
	for _, row := range rows {
		if row.Diff != 1 {
			continue
		}
		result[row.Key] = row.Row
	}
	return result
}

func TestMZ034CompileIncrementalSQLGroupAggregate(t *testing.T) {
	compiled, err := CompileSQLQuery(mz034SQLGroupQuery)
	if err != nil {
		t.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalGroupAggregate()
	if err != nil {
		t.Fatal(err)
	}
	changes, err := operator.Apply(mz034SQLGroupUpdates())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]SQLRow{
		fmt.Sprintf("%#v", "red"):  {"bucket": "red", "total": int64(2), "total_score": float64(8)},
		fmt.Sprintf("%#v", "blue"): {"bucket": "blue", "total": int64(1), "total_score": float64(7)},
	}
	if got := mz034SQLGroupRowsByKey(changes); !reflect.DeepEqual(got, want) {
		t.Fatalf("initial changes = %#v, want %#v", got, want)
	}

	retraction, err := operator.Apply([]DifferentialRow{{
		Key: "row-2", Time: 4, Diff: -1, Row: Row{"group": "red", "score": int64(5)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(retraction) != 2 || retraction[0].Diff != -1 || retraction[1].Diff != 1 {
		t.Fatalf("retraction = %#v, want old and new red rows", retraction)
	}
	if !reflect.DeepEqual(retraction[0].Row, SQLRow{"bucket": "red", "total": int64(2), "total_score": float64(8)}) {
		t.Fatalf("old red row = %#v", retraction[0].Row)
	}
	if !reflect.DeepEqual(retraction[1].Row, SQLRow{"bucket": "red", "total": int64(1), "total_score": float64(3)}) {
		t.Fatalf("new red row = %#v", retraction[1].Row)
	}

	snapshot := operator.Snapshot()
	gotSnapshot := mz034SQLGroupRowsByKey(snapshot)
	wantSnapshot := map[string]SQLRow{
		fmt.Sprintf("%#v", "red"):  {"bucket": "red", "total": int64(1), "total_score": float64(3)},
		fmt.Sprintf("%#v", "blue"): {"bucket": "blue", "total": int64(1), "total_score": float64(7)},
	}
	if !reflect.DeepEqual(gotSnapshot, wantSnapshot) {
		t.Fatalf("snapshot = %#v, want %#v", gotSnapshot, wantSnapshot)
	}

	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{
			{"group": "red", "score": int64(3)},
			{"group": "blue", "score": int64(7)},
		}, nil
	})
	expected, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(expected.Rows) != len(snapshot) {
		t.Fatalf("rebuilt rows = %#v, incremental snapshot = %#v", expected.Rows, snapshot)
	}
	for _, row := range expected.Rows {
		key := fmt.Sprintf("%#v", row["bucket"])
		if !reflect.DeepEqual(gotSnapshot[key], row) {
			t.Fatalf("incremental row for %s = %#v, rebuilt = %#v", key, gotSnapshot[key], row)
		}
	}
}

func TestMZ034IncrementalSQLGroupAggregateModes(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  map[string]SQLRow
	}{
		{
			name:  "count only",
			query: "FROM CACHE('items') AS src SELECT src.group, COUNT(*) AS total GROUP BY src.group",
			want:  map[string]SQLRow{fmt.Sprintf("%#v", "red"): {"group": "red", "total": int64(2)}, fmt.Sprintf("%#v", "blue"): {"group": "blue", "total": int64(1)}},
		},
		{
			name:  "sum only",
			query: "FROM CACHE('items') AS src SELECT src.group, SUM(src.score) AS total_score GROUP BY src.group",
			want:  map[string]SQLRow{fmt.Sprintf("%#v", "red"): {"group": "red", "total_score": float64(8)}, fmt.Sprintf("%#v", "blue"): {"group": "blue", "total_score": float64(7)}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			compiled, err := CompileSQLQuery(test.query)
			if err != nil {
				t.Fatal(err)
			}
			operator, err := compiled.CompileIncrementalGroupAggregate()
			if err != nil {
				t.Fatal(err)
			}
			changes, err := operator.Apply(mz034SQLGroupUpdates())
			if err != nil {
				t.Fatal(err)
			}
			if got := mz034SQLGroupRowsByKey(changes); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("changes = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestMZ034IncrementalSQLGroupAggregateWhereAndAtomicErrors(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group, COUNT(*) AS total, SUM(src.score) AS total_score WHERE src.score >= 5 GROUP BY src.group")
	if err != nil {
		t.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalGroupAggregate()
	if err != nil {
		t.Fatal(err)
	}
	changes, err := operator.Apply(mz034SQLGroupUpdates())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]SQLRow{
		fmt.Sprintf("%#v", "red"):  {"group": "red", "total": int64(1), "total_score": float64(5)},
		fmt.Sprintf("%#v", "blue"): {"group": "blue", "total": int64(1), "total_score": float64(7)},
	}
	if got := mz034SQLGroupRowsByKey(changes); !reflect.DeepEqual(got, want) {
		t.Fatalf("filtered changes = %#v, want %#v", got, want)
	}
	ignored, err := operator.Apply([]DifferentialRow{{Key: "row-1", Diff: -1, Row: Row{"group": "red", "score": int64(3)}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ignored) != 0 {
		t.Fatalf("filtered retraction = %#v, want no change", ignored)
	}

	before := operator.Snapshot()
	_, err = operator.Apply([]DifferentialRow{{Key: "row-2", Diff: -2, Row: Row{"group": "red", "score": int64(5)}}})
	if !errors.Is(err, ErrDifferentialGroupByNegativeCount) {
		t.Fatalf("negative count error = %v", err)
	}
	if after := operator.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("snapshot changed after rejected batch: before=%#v after=%#v", before, after)
	}

	badValue, err := CompileSQLQuery(mz034SQLGroupQuery)
	if err != nil {
		t.Fatal(err)
	}
	badOperator, err := badValue.CompileIncrementalGroupAggregate()
	if err != nil {
		t.Fatal(err)
	}
	_, err = badOperator.Apply([]DifferentialRow{{Key: "bad", Diff: 1, Row: Row{"group": "red", "score": 1.5}}})
	if !errors.Is(err, ErrSQLIncrementalGroupAggregateValueType) {
		t.Fatalf("float SUM value error = %v", err)
	}
	_, err = badOperator.Apply([]DifferentialRow{{Key: "bad", Diff: 1, Row: Row{"group": "red", "score": nil}}})
	if !errors.Is(err, ErrSQLIncrementalGroupAggregateValueType) {
		t.Fatalf("NULL SUM value error = %v", err)
	}
}

func TestMZ034CompileIncrementalSQLGroupAggregateRejectsGlobalSemantics(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT src.group, COUNT(src.score) AS total GROUP BY src.group",
		"FROM CACHE('items') AS src SELECT src.group, AVG(src.score) AS average GROUP BY src.group",
		"FROM CACHE('items') AS src SELECT src.group, COUNT(*) AS total GROUP BY src.group ORDER BY total",
		"FROM CACHE('items') AS src SELECT src.group, COUNT(*) AS total GROUP BY src.group HAVING COUNT(*) > 1",
		"FROM CACHE('items') AS src SELECT src.group, COUNT(*) AS total GROUP BY src.group, src.score",
	}
	for _, source := range queries {
		compiled, err := CompileSQLQuery(source)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := compiled.CompileIncrementalGroupAggregate(); !errors.Is(err, ErrSQLIncrementalGroupAggregateUnsupported) {
			t.Fatalf("query %q error = %v", source, err)
		}
	}
	var nilQuery *CompiledSQLQuery
	if _, err := nilQuery.CompileIncrementalGroupAggregate(); !errors.Is(err, ErrSQLIncrementalGroupAggregateUnsupported) {
		t.Fatalf("nil query error = %v", err)
	}
}

func BenchmarkMZ034IncrementalSQLGroupAggregate(b *testing.B) {
	compiled, err := CompileSQLQuery(mz034SQLGroupQuery)
	if err != nil {
		b.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalGroupAggregate()
	if err != nil {
		b.Fatal(err)
	}
	if _, err := operator.Apply(mz034SQLGroupUpdates()); err != nil {
		b.Fatal(err)
	}
	update := []DifferentialRow{{Key: "hot-row", Diff: 1, Row: Row{"group": "red", "score": int64(11)}}}
	b.ReportAllocs()
	for range b.N {
		changes, err := operator.Apply(update)
		if err != nil {
			b.Fatal(err)
		}
		if len(changes) != 2 {
			b.Fatalf("changes = %#v, want old/new aggregate", changes)
		}
	}
}

func ExampleSQLIncrementalGroupAggregate() {
	compiled, _ := CompileSQLQuery(mz034SQLGroupQuery)
	operator, _ := compiled.CompileIncrementalGroupAggregate()
	changes, _ := operator.Apply([]DifferentialRow{{Key: "row-1", Diff: 1, Row: Row{"group": "red", "score": int64(3)}}})
	sort.Slice(changes, func(i, j int) bool { return changes[i].Key < changes[j].Key })
	fmt.Println(changes[0].Key, changes[0].Row["bucket"], changes[0].Row["total"], changes[0].Row["total_score"])
	// Output: "red" red 1 3
}
