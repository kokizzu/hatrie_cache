package hatDataStructure_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTU17LSMTableMaintainsRunsTombstonesCompactionAndSnapshot(t *testing.T) {
	table, err := hatDataStructure.NewLSMTable(hatDataStructure.LSMTableOptions{
		MemtableMaxRecords:      2,
		MaxRunsBeforeCompaction: 4,
		RunOptions:              hatDataStructure.SealedUpsertRunOptions{MaxRecords: 32},
	})
	if err != nil {
		t.Fatalf("NewLSMTable() error = %v", err)
	}
	if err := table.Put("a", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := table.Put("b", []byte("bee")); err != nil {
		t.Fatal(err)
	}
	if got := table.Stats().RunCount; got != 1 {
		t.Fatalf("automatic flush runs = %d, want 1", got)
	}
	if err := table.Put("a", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := table.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if got := table.Stats().RunCount; got != 2 {
		t.Fatalf("second automatic flush runs = %d, want 2", got)
	}
	if value, ok := table.Get("a"); !ok || !reflect.DeepEqual(value, []byte("new")) {
		t.Fatalf("Get(a) = %q, %t, want new", value, ok)
	}
	if value, ok := table.Get("b"); ok || value != nil {
		t.Fatalf("Get(b) = %q, %t, want tombstone miss", value, ok)
	}

	if err := table.Compact(); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if got := table.Stats().RunCount; got != 1 {
		t.Fatalf("compacted runs = %d, want 1", got)
	}
	if value, ok := table.Get("a"); !ok || !reflect.DeepEqual(value, []byte("new")) {
		t.Fatalf("Get(a) after compact = %q, %t, want new", value, ok)
	}

	wire, err := table.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	restored, err := hatDataStructure.UnmarshalLSMTable(wire, hatDataStructure.LSMTableOptions{
		MemtableMaxRecords: 2,
		RunOptions:         hatDataStructure.SealedUpsertRunOptions{MaxRecords: 32},
	})
	if err != nil {
		t.Fatalf("UnmarshalLSMTable() error = %v", err)
	}
	if value, ok := restored.Get("a"); !ok || !reflect.DeepEqual(value, []byte("new")) {
		t.Fatalf("restored Get(a) = %q, %t, want new", value, ok)
	}
	if value, ok := restored.Get("b"); ok || value != nil {
		t.Fatalf("restored Get(b) = %q, %t, want tombstone miss", value, ok)
	}

	wire[0] ^= 0xff
	if _, err := hatDataStructure.UnmarshalLSMTable(wire, hatDataStructure.LSMTableOptions{}); !errors.Is(err, hatDataStructure.ErrLSMTableCorrupt) {
		t.Fatalf("corrupt snapshot error = %v, want corrupt error", err)
	}
}

func TestTU17LSMTableRejectsInvalidWritesAndPreservesState(t *testing.T) {
	table, err := hatDataStructure.NewLSMTable(hatDataStructure.LSMTableOptions{
		RunOptions: hatDataStructure.SealedUpsertRunOptions{MaxValueBytes: 3},
	})
	if err != nil {
		t.Fatalf("NewLSMTable() error = %v", err)
	}
	if err := table.Put("kept", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := table.Put("too-large", []byte("long")); !errors.Is(err, hatDataStructure.ErrLSMTableValueTooLarge) {
		t.Fatalf("large value error = %v, want value limit", err)
	}
	if value, ok := table.Get("kept"); !ok || !reflect.DeepEqual(value, []byte("ok")) {
		t.Fatalf("kept value after rejected write = %q, %t", value, ok)
	}
	if err := table.Put("", []byte("bad")); !errors.Is(err, hatDataStructure.ErrLSMTableKeyRequired) {
		t.Fatalf("empty key error = %v, want key error", err)
	}
	if err := table.Delete(""); !errors.Is(err, hatDataStructure.ErrLSMTableKeyRequired) {
		t.Fatalf("empty delete key error = %v, want key error", err)
	}
	var nilTable *hatDataStructure.LSMTable
	if err := nilTable.Flush(); !errors.Is(err, hatDataStructure.ErrLSMTableNil) {
		t.Fatalf("nil Flush() error = %v, want nil error", err)
	}
}

func TestTU17LSMTableAutomaticCompactionIsBounded(t *testing.T) {
	table, err := hatDataStructure.NewLSMTable(hatDataStructure.LSMTableOptions{
		MemtableMaxRecords:      1,
		MaxRunsBeforeCompaction: 2,
		RunOptions:              hatDataStructure.SealedUpsertRunOptions{MaxRecords: 32},
	})
	if err != nil {
		t.Fatalf("NewLSMTable() error = %v", err)
	}
	for index := 0; index < 12; index++ {
		if err := table.Put("key", []byte{byte(index)}); err != nil {
			t.Fatalf("Put(%d) error = %v", index, err)
		}
		if got := table.Stats().RunCount; got > 2 {
			t.Fatalf("run count = %d after Put(%d), want <= 2", got, index)
		}
	}
	if value, ok := table.Get("key"); !ok || !reflect.DeepEqual(value, []byte{11}) {
		t.Fatalf("final Get(key) = %v, %t, want [11]", value, ok)
	}
}
