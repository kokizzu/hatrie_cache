package hatDataStructure

import (
	"strconv"
	"testing"
)

type orderedIndexC221Record struct {
	Key   int
	Value int
}

func TestOrderedIndexSameKeyUpsertPreservesOrderAndSnapshotIsolation(t *testing.T) {
	index := newOrderedIndexC221Index(t, 64)
	cursor, ok := index.SnapshotCursor()
	if !ok {
		t.Fatal("SnapshotCursor() = false")
	}
	if err := index.Upsert(32, orderedIndexC221Record{Key: 32, Value: 3200}); err != nil {
		t.Fatal(err)
	}

	if err := cursor.Seek(32); err != nil {
		t.Fatal(err)
	}
	oldEntry, next, err := cursor.Next()
	if err != nil || !next || oldEntry.ID != 32 || oldEntry.Value.Value != 32 {
		t.Fatalf("snapshot entry = %#v, %v, %v; want old value 32", oldEntry, next, err)
	}
	cursor.Close()

	iterator, ok := index.Seek(32)
	if !ok {
		t.Fatal("Seek(32) = false")
	}
	entry, next, err := iterator.Next()
	iterator.Close()
	if err != nil || !next || entry.ID != 32 || entry.Value.Value != 3200 {
		t.Fatalf("current entry = %#v, %v, %v; want new value 3200", entry, next, err)
	}

	if got := testing.AllocsPerRun(100, func() {
		if err := index.Upsert(32, orderedIndexC221Record{Key: 32, Value: 3201}); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Fatalf("same-key upsert allocations = %f, want 0", got)
	}
	if !index.Delete(32) {
		t.Fatal("Delete(32) = false")
	}
	iterator, ok = index.Seek(32)
	if ok {
		entry, next, err = iterator.Next()
		iterator.Close()
		if err != nil {
			t.Fatal(err)
		}
		if next && entry.ID == 32 {
			t.Fatal("deleted ID 32 remained in the index")
		}
	}
}

func TestOrderedIndexEquivalentKeyUpsertPreservesTieOrder(t *testing.T) {
	index, err := NewOrderedIndex(
		func(record orderedIndexC221Record) int { return record.Key },
		func(left, right int) int { return orderedIndexC220Compare(left/10, right/10) },
		3,
	)
	if err != nil {
		t.Fatal(err)
	}
	for id, key := range []int{11, 12, 20} {
		if err := index.Upsert(uint64(id+1), orderedIndexC221Record{Key: key, Value: key}); err != nil {
			t.Fatal(err)
		}
	}
	if err := index.Upsert(1, orderedIndexC221Record{Key: 19, Value: 1900}); err != nil {
		t.Fatal(err)
	}

	iterator, ok := index.First()
	if !ok {
		t.Fatal("First() = false")
	}
	defer iterator.Close()
	for _, want := range []struct {
		id    uint64
		key   int
		value int
	}{
		{id: 1, key: 19, value: 1900},
		{id: 2, key: 12, value: 12},
		{id: 3, key: 20, value: 20},
	} {
		entry, next, err := iterator.Next()
		if err != nil || !next {
			t.Fatalf("Next() = %#v, %v, %v; want ID %d", entry, next, err, want.id)
		}
		if entry.ID != want.id || entry.Key != want.key || entry.Value.Value != want.value {
			t.Fatalf("entry = %#v; want ID %d key %d value %d", entry, want.id, want.key, want.value)
		}
	}
}

func BenchmarkC221OrderedIndexSameKeyUpsert(b *testing.B) {
	for _, size := range []int{64, 1024, 10000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			index := newOrderedIndexC221BenchmarkIndex(b, size)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				id := uint64(iteration%size + 1)
				if err := index.Upsert(id, orderedIndexC221Record{Key: int(id), Value: iteration}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func newOrderedIndexC221Index(t testing.TB, size int) *OrderedIndex[orderedIndexC221Record, int] {
	t.Helper()
	return newOrderedIndexC221BenchmarkIndex(t, size)
}

func newOrderedIndexC221BenchmarkIndex(t testing.TB, size int) *OrderedIndex[orderedIndexC221Record, int] {
	t.Helper()
	index, err := NewOrderedIndex(
		func(record orderedIndexC221Record) int { return record.Key },
		orderedIndexC220Compare,
		size,
	)
	if err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= size; id++ {
		if err := index.Upsert(uint64(id), orderedIndexC221Record{Key: id, Value: id}); err != nil {
			t.Fatal(err)
		}
	}
	return index
}
