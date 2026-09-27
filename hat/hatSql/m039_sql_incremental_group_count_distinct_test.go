package hatSql

import (
	"errors"
	"reflect"
	"sort"
	"testing"
)

const m039SQLGroupCountDistinctQuery = "FROM CACHE('items') AS src SELECT src.group, COUNT(DISTINCT src.value) AS unique_values GROUP BY src.group"

func m039SQLGroupCountDistinctRows() []SQLRow {
	return []SQLRow{
		{"group": "red", "value": int64(5), "active": true},
		{"group": "red", "value": int64(5), "active": true},
		{"group": "red", "value": int64(7), "active": true},
		{"group": "blue", "value": int64(5), "active": true},
		{"group": "blue", "value": int64(9), "active": true},
		{"group": "blue", "value": nil, "active": true},
	}
}

func TestM039SQLGroupCountDistinctMaterialized(t *testing.T) {
	rows := m039SQLGroupCountDistinctRows()
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	result, err := ExecuteSQLQuery(m039SQLGroupCountDistinctQuery, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	got := make(map[string]interface{}, len(result.Rows))
	for _, row := range result.Rows {
		got[row["group"].(string)] = row["unique_values"]
	}
	want := map[string]interface{}{"blue": int64(2), "red": int64(2)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("materialized result = %#v, want %#v", got, want)
	}
}

func TestM039SQLIncrementalGroupCountDistinct(t *testing.T) {
	compiled, err := CompileSQLQuery(m039SQLGroupCountDistinctQuery)
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	operator, err := compiled.CompileIncrementalGroupCountDistinct()
	if err != nil {
		t.Fatalf("CompileIncrementalGroupCountDistinct() error = %v", err)
	}
	seed := []DifferentialRow{
		{Key: "red-1", Time: 1, Diff: 1, Row: Row{"group": "red", "value": int64(5)}},
		{Key: "red-2", Time: 1, Diff: 1, Row: Row{"group": "red", "value": int64(5)}},
		{Key: "red-3", Time: 1, Diff: 1, Row: Row{"group": "red", "value": int64(7)}},
		{Key: "blue-1", Time: 1, Diff: 1, Row: Row{"group": "blue", "value": int64(5)}},
		{Key: "blue-2", Time: 1, Diff: 1, Row: Row{"group": "blue", "value": int64(9)}},
	}
	if _, err := operator.Apply(seed); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: `"blue"`, Diff: 1, Row: Row{"group": "blue", "unique_values": int64(2)}},
		{Key: `"red"`, Diff: 1, Row: Row{"group": "red", "unique_values": int64(2)}},
	}
	if got := operator.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", got, want)
	}

	if got, err := operator.Apply([]DifferentialRow{{Key: "red-1", Time: 2, Diff: -1, Row: Row{"group": "red", "value": int64(5)}}}); err != nil || got != nil {
		t.Fatalf("duplicate retraction = %#v, error %v, want nil output and nil error", got, err)
	}
	got, err := operator.Apply([]DifferentialRow{{Key: "red-2", Time: 3, Diff: -1, Row: Row{"group": "red", "value": int64(5)}}})
	if err != nil {
		t.Fatalf("distinct value retraction error = %v", err)
	}
	wantChanges := []DifferentialRow{
		{Key: `"red"`, Time: 3, Diff: -1, Row: Row{"group": "red", "unique_values": int64(2)}},
		{Key: `"red"`, Time: 3, Diff: 1, Row: Row{"group": "red", "unique_values": int64(1)}},
	}
	if !reflect.DeepEqual(got, wantChanges) {
		t.Fatalf("distinct value retraction = %#v, want %#v", got, wantChanges)
	}
}

func TestM039SQLIncrementalGroupCountDistinctWhereAndAtomicity(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group, COUNT(DISTINCT src.value) AS unique_values WHERE src.active GROUP BY src.group")
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	operator, err := compiled.CompileIncrementalGroupCountDistinct()
	if err != nil {
		t.Fatalf("CompileIncrementalGroupCountDistinct() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{
		{Key: "active", Diff: 1, Row: Row{"group": "red", "value": int64(1), "active": true}},
		{Key: "inactive", Diff: 1, Row: Row{"group": "red", "value": int64(2), "active": false}},
	}); err != nil {
		t.Fatalf("WHERE Apply() error = %v", err)
	}
	want := []DifferentialRow{{Key: `"red"`, Diff: 1, Row: Row{"group": "red", "unique_values": int64(1)}}}
	if got := operator.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("WHERE Snapshot() = %#v, want %#v", got, want)
	}

	_, err = operator.Apply([]DifferentialRow{
		{Key: "blue", Diff: 1, Row: Row{"group": "blue", "value": int64(8), "active": true}},
		{Key: "bad", Diff: 1, Row: Row{"group": "red", "value": "not-an-int", "active": true}},
	})
	if !errors.Is(err, ErrSQLIncrementalGroupCountDistinctValueType) {
		t.Fatalf("invalid value error = %v, want ErrSQLIncrementalGroupCountDistinctValueType", err)
	}
	want = []DifferentialRow{{Key: `"red"`, Diff: 1, Row: Row{"group": "red", "unique_values": int64(1)}}}
	if got := operator.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() after rejected batch = %#v, want %#v", got, want)
	}
}

func TestM039SQLIncrementalGroupCountDistinctValidation(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT src.group, COUNT(*) AS unique_values GROUP BY src.group",
		"FROM CACHE('items') AS src SELECT src.group, COUNT(DISTINCT src.value), COUNT(DISTINCT src.value) AS other GROUP BY src.group",
		"FROM CACHE('items') AS src SELECT src.group, COUNT(DISTINCT src.value) AS unique_values GROUP BY src.group ORDER BY src.group",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			compiled, err := CompileSQLQuery(query)
			if err != nil {
				if query == queries[0] {
					t.Fatalf("COUNT(*) query parse error = %v", err)
				}
				return
			}
			if _, err := compiled.CompileIncrementalGroupCountDistinct(); !errors.Is(err, ErrSQLIncrementalGroupCountDistinctUnsupported) {
				t.Fatalf("CompileIncrementalGroupCountDistinct() error = %v, want ErrSQLIncrementalGroupCountDistinctUnsupported", err)
			}
		})
	}
}

func TestM039SQLIncrementalGroupCountDistinctSnapshotOrder(t *testing.T) {
	compiled, err := CompileSQLQuery(m039SQLGroupCountDistinctQuery)
	if err != nil {
		t.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalGroupCountDistinct()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := operator.Apply([]DifferentialRow{
		{Key: "z", Diff: 1, Row: Row{"group": "z", "value": int64(1)}},
		{Key: "a", Diff: 1, Row: Row{"group": "a", "value": int64(1)}},
		{Key: "m", Diff: 1, Row: Row{"group": "m", "value": int64(1)}},
	}); err != nil {
		t.Fatal(err)
	}
	got := operator.Snapshot()
	keys := make([]string, len(got))
	for index, row := range got {
		keys[index] = row.Key
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatalf("Snapshot() keys = %#v, want sorted order", keys)
	}
}
