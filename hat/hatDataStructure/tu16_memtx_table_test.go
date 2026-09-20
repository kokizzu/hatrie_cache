package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

func TestTU16MemtxTableLifecycleAndReuse(t *testing.T) {
	table, err := NewMemtxTable[uint64](MemtxTableOptions{Capacity: 2})
	if err != nil {
		t.Fatalf("NewMemtxTable() error = %v", err)
	}
	if err := table.Insert(10, 100); err != nil {
		t.Fatalf("Insert(10) error = %v", err)
	}
	if err := table.Insert(10, 101); !errors.Is(err, ErrMemtxTableDuplicateID) {
		t.Fatalf("duplicate Insert() error = %v, want %v", err, ErrMemtxTableDuplicateID)
	}
	if err := table.Insert(20, 200); err != nil {
		t.Fatalf("Insert(20) error = %v", err)
	}
	if err := table.Insert(30, 300); !errors.Is(err, ErrMemtxTableFull) {
		t.Fatalf("full Insert() error = %v, want %v", err, ErrMemtxTableFull)
	}
	if value, ok := table.Get(10); !ok || value != 100 {
		t.Fatalf("Get(10) = %d, %t, want 100, true", value, ok)
	}
	inserted, err := table.Upsert(10, 111)
	if err != nil || inserted {
		t.Fatalf("update Upsert() = %t, %v, want false, nil", inserted, err)
	}
	if !table.Delete(10) || table.Delete(10) {
		t.Fatal("Delete() did not report exactly one removal")
	}
	inserted, err = table.Upsert(30, 300)
	if err != nil || !inserted {
		t.Fatalf("insert Upsert() = %t, %v, want true, nil", inserted, err)
	}
	rows := table.ScanInto(nil)
	got := []MemtxEntry[uint64]{{ID: rows[0].ID, Value: rows[0].Value}, {ID: rows[1].ID, Value: rows[1].Value}}
	want := []MemtxEntry[uint64]{{ID: 30, Value: 300}, {ID: 20, Value: 200}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ScanInto() = %#v, want %#v", got, want)
	}
}

func TestTU16MemtxTableDefaultsAndCapacityValidation(t *testing.T) {
	table, err := NewMemtxTable[int](MemtxTableOptions{})
	if err != nil {
		t.Fatalf("default NewMemtxTable() error = %v", err)
	}
	if table.Capacity() != DefaultMemtxTableCapacity {
		t.Fatalf("default capacity = %d, want %d", table.Capacity(), DefaultMemtxTableCapacity)
	}
	for _, capacity := range []int{-1, MaxMemtxTableCapacity + 1} {
		if _, err := NewMemtxTable[int](MemtxTableOptions{Capacity: capacity}); !errors.Is(err, ErrMemtxTableCapacityInvalid) {
			t.Fatalf("capacity %d error = %v, want %v", capacity, err, ErrMemtxTableCapacityInvalid)
		}
	}
}

func TestTU16MemtxTableScanReusesDestination(t *testing.T) {
	table, err := NewMemtxTable[string](MemtxTableOptions{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	_ = table.Insert(1, "one")
	dst := make([]MemtxEntry[string], 0, 2)
	first := table.ScanInto(dst)
	second := table.ScanInto(first[:0])
	if len(second) != 1 || second[0].Value != "one" {
		t.Fatalf("reused ScanInto() = %#v", second)
	}
	if len(second) > 0 && &second[0] != &first[0] {
		t.Fatal("ScanInto() did not reuse destination backing array")
	}
}

func TestTU16MemtxTableNilAndReset(t *testing.T) {
	var nilTable *MemtxTable[int]
	if err := nilTable.Insert(1, 1); !errors.Is(err, ErrMemtxTableNil) {
		t.Fatalf("nil Insert error = %v, want %v", err, ErrMemtxTableNil)
	}
	if inserted, err := nilTable.Upsert(1, 1); inserted || !errors.Is(err, ErrMemtxTableNil) {
		t.Fatalf("nil Upsert = inserted %v, error %v", inserted, err)
	}
	if _, ok := nilTable.Get(1); ok || nilTable.Delete(1) || nilTable.Len() != 0 || nilTable.Capacity() != 0 {
		t.Fatalf("nil table operations returned a live row")
	}

	table, err := NewMemtxTable[int](MemtxTableOptions{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(0, 10); err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(1, 20); err != nil {
		t.Fatal(err)
	}
	table.Reset()
	if table.Len() != 0 {
		t.Fatalf("Len after Reset = %d, want 0", table.Len())
	}
	if _, ok := table.Get(0); ok {
		t.Fatal("Reset left a row addressable")
	}
	if err := table.Insert(2, 30); err != nil {
		t.Fatalf("Insert after Reset: %v", err)
	}
}
