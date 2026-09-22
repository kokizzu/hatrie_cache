package hatDataStructure

import (
	"errors"
	"testing"
)

func TestT229BeforeReplaceCanValidateAndTransformWrites(t *testing.T) {
	var seen []MemtxReplaceEvent[int]
	rejectOdd := errors.New("odd values are rejected")
	table, err := NewMemtxTableWithHooks[int](MemtxTableOptions{Capacity: 2}, MemtxTableHooks[int]{
		BeforeReplace: func(event MemtxReplaceEvent[int]) (int, error) {
			seen = append(seen, event)
			if event.New%2 != 0 {
				return 0, rejectOdd
			}
			return event.New * 10, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	inserted, err := table.Upsert(7, 2)
	if err != nil || !inserted {
		t.Fatalf("first Upsert() = inserted %v, err %v; want insert", inserted, err)
	}
	if value, ok := table.Get(7); !ok || value != 20 {
		t.Fatalf("Get(7) = %d, %v; want 20, true", value, ok)
	}

	if _, err := table.Upsert(7, 3); !errors.Is(err, rejectOdd) {
		t.Fatalf("rejected Upsert() error = %v; want %v", err, rejectOdd)
	}
	if value, ok := table.Get(7); !ok || value != 20 {
		t.Fatalf("rejected Upsert() changed row to %d, %v; want 20, true", value, ok)
	}

	if _, err := table.Upsert(7, 4); err != nil {
		t.Fatalf("accepted Upsert() error = %v", err)
	}
	if value, ok := table.Get(7); !ok || value != 40 {
		t.Fatalf("transformed Upsert() stored %d, %v; want 40, true", value, ok)
	}

	if len(seen) != 3 {
		t.Fatalf("hook calls = %d; want 3", len(seen))
	}
	if seen[0].ID != 7 || seen[0].Exists || seen[0].Operation != MemtxReplaceInsert || seen[0].Old != 0 || seen[0].New != 2 {
		t.Fatalf("insert event = %#v", seen[0])
	}
	if seen[1].ID != 7 || !seen[1].Exists || seen[1].Operation != MemtxReplaceUpdate || seen[1].Old != 20 || seen[1].New != 3 {
		t.Fatalf("rejected update event = %#v", seen[1])
	}
	if seen[2].Old != 20 || seen[2].New != 4 {
		t.Fatalf("accepted update event = %#v", seen[2])
	}
}

func TestT229BeforeReplaceRunsForDirectInsert(t *testing.T) {
	table, err := NewMemtxTableWithHooks[int](MemtxTableOptions{Capacity: 1}, MemtxTableHooks[int]{
		BeforeReplace: func(event MemtxReplaceEvent[int]) (int, error) {
			if event.Exists || event.Operation != MemtxReplaceInsert {
				t.Fatalf("insert event = %#v", event)
			}
			return event.New + 1, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(3, 10); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if value, ok := table.Get(3); !ok || value != 11 {
		t.Fatalf("Get(3) = %d, %v; want 11, true", value, ok)
	}
}

func TestT229BeforeReplaceErrorDoesNotConsumeCapacity(t *testing.T) {
	reject := errors.New("reject")
	table, err := NewMemtxTableWithHooks[int](MemtxTableOptions{Capacity: 1}, MemtxTableHooks[int]{
		BeforeReplace: func(MemtxReplaceEvent[int]) (int, error) {
			return 0, reject
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if inserted, err := table.Upsert(1, 1); !errors.Is(err, reject) || inserted {
		t.Fatalf("rejected insert = inserted %v, err %v; want false, reject", inserted, err)
	}
	if table.Len() != 0 {
		t.Fatalf("Len() = %d after rejected insert; want 0", table.Len())
	}
}

func BenchmarkT229MemtxUpsertBeforeReplace(b *testing.B) {
	table, err := NewMemtxTableWithHooks[int](MemtxTableOptions{Capacity: 1}, MemtxTableHooks[int]{
		BeforeReplace: func(event MemtxReplaceEvent[int]) (int, error) {
			return event.New, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < b.N; index++ {
		if _, err := table.Upsert(1, index); err != nil {
			b.Fatal(err)
		}
	}
}
