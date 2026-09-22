package hatDataStructure

import (
	"errors"
	"testing"
)

func TestT230OnReplaceReceivesOldAndCommittedNewImages(t *testing.T) {
	var events []MemtxReplaceEvent[int]
	table, err := NewMemtxTableWithHooks[int](MemtxTableOptions{Capacity: 1}, MemtxTableHooks[int]{
		OnReplace: func(event MemtxReplaceEvent[int]) {
			events = append(events, event)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(9, 10); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if _, err := table.Upsert(9, 20); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("OnReplace calls = %d; want 2", len(events))
	}
	if events[0].ID != 9 || events[0].Exists || events[0].Operation != MemtxReplaceInsert || events[0].Old != 0 || events[0].New != 10 {
		t.Fatalf("insert event = %#v", events[0])
	}
	if events[1].ID != 9 || !events[1].Exists || events[1].Operation != MemtxReplaceUpdate || events[1].Old != 10 || events[1].New != 20 {
		t.Fatalf("update event = %#v", events[1])
	}
	if value, ok := table.Get(9); !ok || value != events[1].New {
		t.Fatalf("stored value = %d, %v; event new = %d", value, ok, events[1].New)
	}
}

func TestT230OnReplaceDoesNotEmitRejectedWrites(t *testing.T) {
	reject := errors.New("reject")
	emitted := 0
	table, err := NewMemtxTableWithHooks[int](MemtxTableOptions{Capacity: 1}, MemtxTableHooks[int]{
		BeforeReplace: func(MemtxReplaceEvent[int]) (int, error) {
			return 0, reject
		},
		OnReplace: func(MemtxReplaceEvent[int]) {
			emitted++
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert(1, 1); !errors.Is(err, reject) {
		t.Fatalf("Upsert() error = %v; want %v", err, reject)
	}
	if emitted != 0 || table.Len() != 0 {
		t.Fatalf("rejected write emitted %d events and left %d rows; want 0, 0", emitted, table.Len())
	}
}

var t230OnReplaceSink int

func t230RecordOnReplace(event MemtxReplaceEvent[int]) {
	t230OnReplaceSink = event.New
}

func BenchmarkT230MemtxUpsertOnReplace(b *testing.B) {
	table, err := NewMemtxTableWithHooks[int](MemtxTableOptions{Capacity: 1}, MemtxTableHooks[int]{
		OnReplace: t230RecordOnReplace,
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < b.N; index++ {
		if _, err := table.Upsert(1, index); err != nil {
			b.Fatal(err)
		}
	}
	if t230OnReplaceSink == -1 {
		b.Fatal("unreachable")
	}
}
