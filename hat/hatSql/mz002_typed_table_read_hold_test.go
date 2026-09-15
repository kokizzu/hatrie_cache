package hatSql

import (
	"errors"
	"testing"
)

func TestMZ002TypedTableChangeReadHoldPinsCompaction(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := int64(1); index <= 3; index++ {
		if _, err := table.Upsert(string(rune('a'+index-1)), []TypedTableValue{TypedInt64(index)}); err != nil {
			t.Fatal(err)
		}
	}

	hold, err := table.AcquireChangeReadHold(0)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Release()
	if hold.Since() != 0 || hold.Upper() != 3 {
		t.Fatalf("hold bounds = %d..%d, want 0..3", hold.Since(), hold.Upper())
	}
	changes, tail, err := hold.ChangesAfter(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || changes[0].Sequence != 1 || changes[1].Sequence != 2 || tail != 3 {
		t.Fatalf("first held read = %#v, tail %d", changes, tail)
	}
	if err := table.CompactChangesThrough(1); !errors.Is(err, ErrTypedTableChangeReadHoldActive) {
		t.Fatalf("compaction through held sequence error = %v, want %v", err, ErrTypedTableChangeReadHoldActive)
	}
	if err := hold.Advance(2); err != nil {
		t.Fatal(err)
	}
	if err := table.CompactChangesThrough(1); err != nil {
		t.Fatalf("compaction through consumed sequence: %v", err)
	}
	changes, tail, err = hold.ChangesAfter(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Sequence != 3 || tail != 3 {
		t.Fatalf("second held read = %#v, tail %d", changes, tail)
	}
	if err := hold.Release(); err != nil {
		t.Fatal(err)
	}
	if err := table.CompactChangesThrough(3); err != nil {
		t.Fatalf("compaction after release: %v", err)
	}
}

func TestMZ002TypedTableChangeReadHoldRejectsOutOfRangeReads(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("a", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	hold, err := table.AcquireChangeReadHold(0)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Release()
	if _, _, err := hold.ChangesAfter(2, 1); !errors.Is(err, ErrTypedTableChangeReadHoldRange) {
		t.Fatalf("read after hold upper error = %v, want %v", err, ErrTypedTableChangeReadHoldRange)
	}
	if err := hold.Advance(2); !errors.Is(err, ErrTypedTableChangeReadHoldRange) {
		t.Fatalf("advance beyond table tail error = %v, want %v", err, ErrTypedTableChangeReadHoldRange)
	}
}
