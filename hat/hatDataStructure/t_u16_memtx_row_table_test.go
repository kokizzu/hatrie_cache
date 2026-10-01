package hatDataStructure_test

import (
	"reflect"
	"testing"

	hatDataStructure "hatrie_cache/hat/hatDataStructure"
)

func TestMemtxRowTableUpsertDeleteVisitAndCompact(t *testing.T) {
	table, err := hatDataStructure.NewMemtxRowTable(2)
	if err != nil {
		t.Fatalf("NewMemtxRowTable() error = %v", err)
	}
	if err := table.Upsert("a", []any{int64(1), "one"}); err != nil {
		t.Fatalf("Upsert(a) error = %v", err)
	}
	if err := table.Upsert("b", []any{int64(2), "two"}); err != nil {
		t.Fatalf("Upsert(b) error = %v", err)
	}
	if err := table.Upsert("a", []any{int64(3), "updated"}); err != nil {
		t.Fatalf("Upsert(a update) error = %v", err)
	}

	got, ok := table.Get("a")
	if !ok || !reflect.DeepEqual(got, []any{int64(3), "updated"}) {
		t.Fatalf("Get(a) = %#v, %v, want updated row", got, ok)
	}
	got[0] = int64(99)
	gotAgain, ok := table.Get("a")
	if !ok || !reflect.DeepEqual(gotAgain, []any{int64(3), "updated"}) {
		t.Fatalf("Get(a) after caller mutation = %#v, %v, want isolated copy", gotAgain, ok)
	}

	if !table.Delete("b") || table.Delete("missing") {
		t.Fatal("Delete() returned unexpected result")
	}
	if table.Len() != 1 || table.PhysicalRows() != 2 {
		t.Fatalf("row counts = live:%d physical:%d, want live:1 physical:2", table.Len(), table.PhysicalRows())
	}

	var visited []string
	table.Visit(func(key string, values []any) bool {
		visited = append(visited, key)
		if key != "a" || !reflect.DeepEqual(values, []any{int64(3), "updated"}) {
			t.Fatalf("Visit row = %q %#v", key, values)
		}
		return true
	})
	if !reflect.DeepEqual(visited, []string{"a"}) {
		t.Fatalf("visited keys = %#v, want [a]", visited)
	}

	table.Compact()
	if table.Len() != 1 || table.PhysicalRows() != 1 {
		t.Fatalf("post-compact counts = live:%d physical:%d, want 1/1", table.Len(), table.PhysicalRows())
	}
	if got, ok := table.Get("b"); ok || got != nil {
		t.Fatalf("deleted row after compact = %#v, %v", got, ok)
	}
}

func TestMemtxRowTableRejectsWrongWidthAndInvalidColumnCount(t *testing.T) {
	if _, err := hatDataStructure.NewMemtxRowTable(-1); err == nil {
		t.Fatal("NewMemtxRowTable(-1) error = nil")
	}
	table, err := hatDataStructure.NewMemtxRowTable(1)
	if err != nil {
		t.Fatalf("NewMemtxRowTable() error = %v", err)
	}
	if err := table.Upsert("key", []any{int64(1), int64(2)}); err == nil {
		t.Fatal("Upsert() wrong width error = nil")
	}
	if err := table.Upsert("", []any{int64(1)}); err == nil {
		t.Fatal("Upsert() empty key error = nil")
	}
}

func TestMemtxRowTableZeroValueAndDetachedVisit(t *testing.T) {
	var table hatDataStructure.MemtxRowTable
	if err := table.Upsert("key", nil); err != nil {
		t.Fatalf("zero-value Upsert() error = %v", err)
	}
	if got, ok := table.GetInto(make([]any, 0), "key"); !ok || len(got) != 0 {
		t.Fatalf("zero-value GetInto() = %#v, %v", got, ok)
	}
	called := false
	table.Visit(func(key string, values []any) bool {
		called = true
		if key != "key" || values == nil {
			t.Fatalf("Visit() row = %q %#v", key, values)
		}
		return true
	})
	if !called {
		t.Fatal("Visit() did not visit zero-width row")
	}
}
