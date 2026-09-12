package hatDataStructure_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestOrderedIndexSnapshotCursorKeepsStableViewAcrossMutations(t *testing.T) {
	index := newSnapshotCursorIndex(t, 1, 2, 3)
	cursor, ok := index.SnapshotCursor()
	if !ok {
		t.Fatal("SnapshotCursor() returned no cursor for a non-empty index")
	}

	if err := index.Upsert(0, 0); err != nil {
		t.Fatalf("Upsert(0) error = %v", err)
	}
	if !index.Delete(2) {
		t.Fatal("Delete(2) = false, want true")
	}
	if err := index.Upsert(4, 4); err != nil {
		t.Fatalf("Upsert(4) error = %v", err)
	}

	var got []uint64
	for {
		entry, ok, err := cursor.Next()
		if err != nil {
			t.Fatalf("snapshot cursor Next() error = %v", err)
		}
		if !ok {
			break
		}
		got = append(got, entry.Value)
	}
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("snapshot values = %v, want [1 2 3]", got)
	}
	if index.Len() != 4 {
		t.Fatalf("current index length = %d, want 4", index.Len())
	}
}

func TestOrderedIndexSnapshotCursorSupportsSeekAndClose(t *testing.T) {
	index := newSnapshotCursorIndex(t, 1, 2, 3)
	cursor, ok := index.SnapshotCursor()
	if !ok {
		t.Fatal("SnapshotCursor() returned no cursor")
	}
	if err := cursor.Seek(2); err != nil {
		t.Fatalf("Seek(2) error = %v", err)
	}
	entry, ok, err := cursor.Next()
	if err != nil || !ok || entry.Value != 2 {
		t.Fatalf("Seek(2) Next() = %#v, %v, %v; want value 2", entry, ok, err)
	}
	if err := cursor.SeekAfter(2); err != nil {
		t.Fatalf("SeekAfter(2) error = %v", err)
	}
	entry, ok, err = cursor.Next()
	if err != nil || !ok || entry.Value != 3 {
		t.Fatalf("SeekAfter(2) Next() = %#v, %v, %v; want value 3", entry, ok, err)
	}
	cursor.Close()
	if _, _, err := cursor.Next(); !errors.Is(err, hatDataStructure.ErrOrderedIndexSnapshotCursorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, hatDataStructure.ErrOrderedIndexSnapshotCursorClosed)
	}
	if err := cursor.Seek(1); !errors.Is(err, hatDataStructure.ErrOrderedIndexSnapshotCursorClosed) {
		t.Fatalf("Seek() after Close error = %v, want %v", err, hatDataStructure.ErrOrderedIndexSnapshotCursorClosed)
	}
}

func TestOrderedIndexSnapshotCursorSurvivesClear(t *testing.T) {
	index := newSnapshotCursorIndex(t, 1, 2)
	cursor, ok := index.SnapshotCursor()
	if !ok {
		t.Fatal("SnapshotCursor() returned no cursor")
	}
	index.Clear()
	for want := uint64(1); want <= 2; want++ {
		entry, ok, err := cursor.Next()
		if err != nil || !ok || entry.Value != want {
			t.Fatalf("snapshot after Clear Next() = %#v, %v, %v; want value %d", entry, ok, err, want)
		}
	}
	if index.Len() != 0 {
		t.Fatalf("cleared index length = %d, want 0", index.Len())
	}
}

func TestOrderedIndexSnapshotCursorValidatesNilAndEmptyIndexes(t *testing.T) {
	var nilIndex *hatDataStructure.OrderedIndex[uint64, uint64]
	if _, ok := nilIndex.SnapshotCursor(); ok {
		t.Fatal("nil SnapshotCursor() returned ok")
	}
	empty, err := hatDataStructure.NewOrderedIndex[uint64, uint64](
		func(value uint64) uint64 { return value },
		compareSnapshotUint64,
		0,
	)
	if err != nil {
		t.Fatalf("NewOrderedIndex() error = %v", err)
	}
	if _, ok := empty.SnapshotCursor(); ok {
		t.Fatal("empty SnapshotCursor() returned ok")
	}
	var nilCursor *hatDataStructure.OrderedIndexSnapshotCursor[uint64, uint64]
	if _, _, err := nilCursor.Next(); !errors.Is(err, hatDataStructure.ErrOrderedIndexSnapshotCursorNil) {
		t.Fatalf("nil cursor Next() error = %v, want %v", err, hatDataStructure.ErrOrderedIndexSnapshotCursorNil)
	}
}

func BenchmarkOrderedIndexSnapshotCursorNext(b *testing.B) {
	index := newSnapshotCursorBenchmarkIndex(b)
	cursor, ok := index.SnapshotCursor()
	if !ok {
		b.Fatal("SnapshotCursor() returned no cursor")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, ok, err := cursor.Next()
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			cursor, ok = index.SnapshotCursor()
			if !ok {
				b.Fatal("SnapshotCursor() returned no cursor")
			}
			entry, ok, err = cursor.Next()
			if err != nil || !ok {
				b.Fatalf("reset cursor Next() = %#v, %v, %v", entry, ok, err)
			}
		}
		orderedSnapshotCursorBenchmarkSink = entry
	}
}

func BenchmarkOrderedIndexIteratorNext(b *testing.B) {
	index := newSnapshotCursorBenchmarkIndex(b)
	iterator, ok := index.First()
	if !ok {
		b.Fatal("First() returned no iterator")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, ok, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			iterator, ok = index.First()
			if !ok {
				b.Fatal("First() returned no iterator")
			}
			entry, ok, err = iterator.Next()
			if err != nil || !ok {
				b.Fatalf("reset iterator Next() = %#v, %v, %v", entry, ok, err)
			}
		}
		orderedSnapshotCursorBenchmarkSink = entry
	}
}

func BenchmarkOrderedIndexUpsertWithoutSnapshot(b *testing.B) {
	index := newSnapshotCursorBenchmarkIndex(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := index.Upsert(512, 512); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrderedIndexUpsertWithSnapshot(b *testing.B) {
	index := newSnapshotCursorBenchmarkIndex(b)
	cursor, ok := index.SnapshotCursor()
	if !ok {
		b.Fatal("SnapshotCursor() returned no cursor")
	}
	defer cursor.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := index.Upsert(512, 512); err != nil {
			b.Fatal(err)
		}
	}
}

func newSnapshotCursorIndex(t testing.TB, values ...uint64) *hatDataStructure.OrderedIndex[uint64, uint64] {
	t.Helper()
	index, err := hatDataStructure.NewOrderedIndex[uint64, uint64](
		func(value uint64) uint64 { return value },
		compareSnapshotUint64,
		len(values),
	)
	if err != nil {
		t.Fatalf("NewOrderedIndex() error = %v", err)
	}
	for _, value := range values {
		if err := index.Upsert(value, value); err != nil {
			t.Fatalf("Upsert(%d) error = %v", value, err)
		}
	}
	return index
}

func newSnapshotCursorBenchmarkIndex(b testing.TB) *hatDataStructure.OrderedIndex[uint64, uint64] {
	b.Helper()
	values := make([]uint64, 1024)
	for i := range values {
		values[i] = uint64(i)
	}
	return newSnapshotCursorIndex(b, values...)
}

func compareSnapshotUint64(left, right uint64) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

var orderedSnapshotCursorBenchmarkSink hatDataStructure.OrderedIndexEntry[uint64, uint64]
