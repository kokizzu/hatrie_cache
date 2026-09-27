package hatSql

import (
	"errors"
	"testing"
)

func TestMZ037CompiledSQLIncrementalTopKMaintainsProjectedRows(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.score ORDER BY src.score DESC LIMIT 2")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	operator, err := compiled.CompileIncrementalTopK()
	if err != nil {
		t.Fatalf("compile incremental top-k: %v", err)
	}

	changes, err := operator.Apply([]DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"id": "a", "score": int64(10)}},
		{Key: "b", Time: 1, Diff: 1, Row: Row{"id": "b", "score": int64(20)}},
	})
	if err != nil {
		t.Fatalf("apply initial rows: %v", err)
	}
	assertMZ037SQLTopKKeys(t, operator.Snapshot(), "b", "a")
	if len(changes) != 2 || changes[0].Key != "b" || changes[1].Key != "a" {
		t.Fatalf("initial changes = %#v, want b then a", changes)
	}

	changes, err = operator.Apply([]DifferentialRow{
		{Key: "c", Time: 2, Diff: 1, Row: Row{"id": "c", "score": int64(30)}},
	})
	if err != nil {
		t.Fatalf("apply promoted row: %v", err)
	}
	assertMZ037SQLTopKKeys(t, operator.Snapshot(), "c", "b")
	if len(changes) != 2 || changes[0].Key != "a" || changes[0].Diff != -1 || changes[1].Key != "c" || changes[1].Diff != 1 {
		t.Fatalf("promotion changes = %#v, want -a then +c", changes)
	}

	if _, err := operator.Apply([]DifferentialRow{{Key: "c", Time: 3, Diff: -1}}); err != nil {
		t.Fatalf("delete promoted row: %v", err)
	}
	assertMZ037SQLTopKKeys(t, operator.Snapshot(), "b", "a")
}

func TestMZ037CompiledSQLIncrementalTopKRejectsUnsupportedShapes(t *testing.T) {
	tests := []string{
		"FROM CACHE('items') AS src SELECT src.id LIMIT 2",
		"FROM CACHE('items') AS src SELECT src.id ORDER BY src.score DESC, src.id LIMIT 2",
		"FROM CACHE('items') AS src SELECT DISTINCT src.id ORDER BY src.id LIMIT 2",
	}
	for _, source := range tests {
		compiled, err := CompileSQLQuery(source)
		if err != nil {
			t.Fatalf("compile %q: %v", source, err)
		}
		if _, err := compiled.CompileIncrementalTopK(); !errors.Is(err, ErrSQLIncrementalTopKUnsupported) {
			t.Fatalf("CompileIncrementalTopK(%q) error = %v, want %v", source, err, ErrSQLIncrementalTopKUnsupported)
		}
	}
}

func TestMZ037CompiledSQLIncrementalTopKFiltersAndOwnsOutput(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.score WHERE src.enabled = 1 ORDER BY src.score DESC LIMIT 2")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	operator, err := compiled.CompileIncrementalTopK()
	if err != nil {
		t.Fatalf("compile incremental top-k: %v", err)
	}
	if got, want := operator.Columns(), []string{"id", "score"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}

	if _, err := operator.Apply([]DifferentialRow{
		{Key: "filtered", Diff: 1, Row: Row{"id": "filtered", "score": int64(100), "enabled": int64(0)}},
		{Key: "kept", Diff: 1, Row: Row{"id": "kept", "score": int64(10), "enabled": int64(1)}},
	}); err != nil {
		t.Fatalf("apply filtered rows: %v", err)
	}
	snapshot := operator.Snapshot()
	assertMZ037SQLTopKKeys(t, snapshot, "kept")
	if _, hidden := snapshot[0].Row[sqlIncrementalTopKOrderColumn]; hidden {
		t.Fatalf("snapshot leaked internal order column: %#v", snapshot[0].Row)
	}

	if _, err := operator.Apply([]DifferentialRow{
		{Key: "kept", Diff: -1},
		{Key: "kept", Diff: 1, Row: Row{"id": "kept", "score": int64(20), "enabled": int64(1)}},
	}); err != nil {
		t.Fatalf("replace kept row: %v", err)
	}
	if snapshot = operator.Snapshot(); len(snapshot) != 1 || snapshot[0].Row["score"] != int64(20) {
		t.Fatalf("replacement snapshot = %#v, want score 20", snapshot)
	}
}

func TestMZ037CompiledSQLIncrementalTopKApplyIsAtomic(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.score ORDER BY src.score DESC LIMIT 2")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	operator, err := compiled.CompileIncrementalTopK()
	if err != nil {
		t.Fatalf("compile incremental top-k: %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{{Key: "a", Diff: 1, Row: Row{"id": "a", "score": int64(10)}}}); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{
		{Key: "b", Diff: 1, Row: Row{"id": "b", "score": int64(20)}},
		{Key: "a", Diff: 1, Row: Row{"id": "a", "score": int64(11)}},
	}); !errors.Is(err, ErrSQLIncrementalTopKRowConflict) {
		t.Fatalf("conflicting batch error = %v, want %v", err, ErrSQLIncrementalTopKRowConflict)
	}
	assertMZ037SQLTopKKeys(t, operator.Snapshot(), "a")
}

func TestMZ037CompiledSQLIncrementalTopKRejectsNullOrderWithoutMutation(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.score ORDER BY src.score DESC LIMIT 2")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	operator, err := compiled.CompileIncrementalTopK()
	if err != nil {
		t.Fatalf("compile incremental top-k: %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{{Key: "null", Diff: 1, Row: Row{"id": "null", "score": nil}}}); !errors.Is(err, ErrSQLIncrementalTopKOrderNull) {
		t.Fatalf("null order error = %v, want %v", err, ErrSQLIncrementalTopKOrderNull)
	}
	if snapshot := operator.Snapshot(); len(snapshot) != 0 {
		t.Fatalf("snapshot after null order = %#v, want empty", snapshot)
	}
}

func assertMZ037SQLTopKKeys(t *testing.T, rows []DifferentialRow, want ...string) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("snapshot length = %d, want %d: %#v", len(rows), len(want), rows)
	}
	for index, key := range want {
		if rows[index].Key != key || rows[index].Diff != 1 {
			t.Fatalf("snapshot[%d] = %#v, want key %q with diff 1", index, rows[index], key)
		}
	}
}
