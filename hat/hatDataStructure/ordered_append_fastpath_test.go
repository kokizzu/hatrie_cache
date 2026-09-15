package hatDataStructure

import "testing"

func TestOrderedIndexMonotonicUpsertPreservesOrderAndPositionMap(t *testing.T) {
	index, err := NewOrderedIndex(func(value int) int { return value }, orderedIndexC220Compare, 64)
	if err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= 64; id++ {
		if err := index.Upsert(uint64(id), id); err != nil {
			t.Fatal(err)
		}
	}
	if len(index.entries) != 64 || index.positions == nil {
		t.Fatalf("monotonic build state = entries %d, positions nil=%v", len(index.entries), index.positions == nil)
	}
	for position, entry := range index.entries {
		wantID := uint64(position + 1)
		if entry.ID != wantID || entry.Key != position+1 || index.positions[entry.ID] != position {
			t.Fatalf("entry at %d = %#v, position map = %d; want ID %d and position %d", position, entry, index.positions[entry.ID], wantID, position)
		}
	}

	if err := index.Upsert(65, 65); err != nil {
		t.Fatal(err)
	}
	if got := index.entries[len(index.entries)-1]; got.ID != 65 || got.Key != 65 || index.positions[65] != 64 {
		t.Fatalf("tail entry = %#v, position = %d; want ID/key 65 at position 64", got, index.positions[65])
	}
}

func TestOrderedIndexMonotonicEquivalentKeysPreserveIDOrder(t *testing.T) {
	index, err := NewOrderedIndex(
		func(value int) int { return value },
		func(left, right int) int { return orderedIndexC220Compare(left/10, right/10) },
		3,
	)
	if err != nil {
		t.Fatal(err)
	}
	for id, value := range []int{11, 12, 19} {
		if err := index.Upsert(uint64(id+1), value); err != nil {
			t.Fatal(err)
		}
	}
	for position, wantID := range []uint64{1, 2, 3} {
		if got := index.entries[position].ID; got != wantID {
			t.Fatalf("equivalent-key entry %d ID = %d, want %d", position, got, wantID)
		}
	}
}

func TestOrderedIndexNonMonotonicUpsertStillInsertsInOrder(t *testing.T) {
	index, err := NewOrderedIndex(func(value int) int { return value }, orderedIndexC220Compare, 3)
	if err != nil {
		t.Fatal(err)
	}
	for id, value := range []int{100, 200, 150} {
		if err := index.Upsert(uint64(id+1), value); err != nil {
			t.Fatal(err)
		}
	}
	for position, want := range []struct {
		id  uint64
		key int
	}{
		{id: 1, key: 100},
		{id: 3, key: 150},
		{id: 2, key: 200},
	} {
		if got := index.entries[position]; got.ID != want.id || got.Key != want.key {
			t.Fatalf("entry %d = %#v, want ID %d key %d", position, got, want.id, want.key)
		}
	}
}
