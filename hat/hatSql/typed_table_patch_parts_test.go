package hatSql

import (
	"errors"
	"testing"
	"time"
)

func TestTypedTableLightweightDeletesPreserveSQLAndCompact(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		PatchParts: TypedTablePatchOptions{
			Enabled:        true,
			MergeThreshold: 100,
		},
		Columns: []TypedTableColumn{
			{Name: "name", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key   string
		name  string
		score int64
	}{
		{key: "a", name: "alpha", score: 1},
		{key: "b", name: "beta", score: 2},
		{key: "c", name: "gamma", score: 3},
	} {
		if _, err := table.Upsert(row.key, []TypedTableValue{TypedString(row.name), TypedInt64(row.score)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := table.ResolveSQLSource("CACHE", "events"); err != nil {
		t.Fatal(err)
	}
	change, err := table.Delete("b")
	if err != nil {
		t.Fatal(err)
	}
	if change.Operation != "DELETE" || len(change.Before) != 2 || change.Before[0].String != "beta" {
		t.Fatalf("delete change = %#v", change)
	}
	if got := len(table.keys); got != 3 {
		t.Fatalf("physical rows before compaction = %d, want 3", got)
	}
	rows, err := table.ResolveSQLSource("CACHE", "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0]["name"] != "alpha" || rows[1]["name"] != "gamma" {
		t.Fatalf("rows after logical delete = %#v", rows)
	}
	batch, available, err := table.ResolveSQLColumnarSource("CACHE", "events", []string{"name", "score"})
	if err != nil || !available || batch.Rows != 2 {
		t.Fatalf("columnar after logical delete = %#v, %t, %v", batch, available, err)
	}
	result, err := ExecuteQuery("FROM CACHE('events') SELECT name WHERE score >= 0", table)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["name"] != "alpha" || result.Rows[1]["name"] != "gamma" {
		t.Fatalf("SQL after logical delete = %#v", result.Rows)
	}
	if err := table.CompactPatchParts(); err != nil {
		t.Fatal(err)
	}
	if got := len(table.keys); got != 2 {
		t.Fatalf("physical rows after compaction = %d, want 2", got)
	}
	rows, err = table.Rows(), nil
	if len(rows) != 2 {
		t.Fatalf("rows after compaction = %#v", rows)
	}
	reinsert, err := table.Upsert("b", []TypedTableValue{TypedString("beta-new"), TypedInt64(4)})
	if err != nil {
		t.Fatal(err)
	}
	if reinsert.Operation != "INSERT" {
		t.Fatalf("reinsert operation = %q, want INSERT", reinsert.Operation)
	}
	rows = table.Rows()
	if len(rows) != 3 {
		t.Fatalf("rows after reinsert = %#v", rows)
	}
}

func TestTypedTablePatchPartsBackgroundCompaction(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:       "events",
		PatchParts: TypedTablePatchOptions{Enabled: true, MergeThreshold: 1},
		Columns:    []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("a", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("b", []TypedTableValue{TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Delete("a"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		table.mu.RLock()
		physicalRows := len(table.keys)
		table.mu.RUnlock()
		if physicalRows == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	t.Fatalf("background compaction retained %d physical rows", len(table.keys))
}

func TestTypedTablePatchPartsDisabledByDefault(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := table.CompactPatchParts(); !errors.Is(err, ErrTypedTablePatchPartsDisabled) {
		t.Fatalf("disabled compaction error = %v", err)
	}
	if _, err := table.Upsert("a", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if got := len(table.keys); got != 0 {
		t.Fatalf("default physical rows = %d, want 0", got)
	}
}
