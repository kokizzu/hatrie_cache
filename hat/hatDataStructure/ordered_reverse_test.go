package hatDataStructure_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestOrderedIndexReverseTraversalAndSeek(t *testing.T) {
	index, err := hatDataStructure.NewOrderedIndex(
		func(value string) string { return value },
		func(left, right string) int {
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		},
		0,
	)
	if err != nil {
		t.Fatalf("NewOrderedIndex() error = %v", err)
	}
	for id, value := range []string{"a", "a", "b", "c"} {
		if err := index.Upsert(uint64(id+1), value); err != nil {
			t.Fatalf("Upsert(%d) error = %v", id+1, err)
		}
	}

	iterator, ok := index.Last()
	if !ok {
		t.Fatal("Last() returned no iterator")
	}
	defer iterator.Close()
	assertReverseEntries(t, &iterator, []hatDataStructure.OrderedIndexEntry[string, string]{
		{ID: 4, Key: "c", Value: "c"},
		{ID: 3, Key: "b", Value: "b"},
		{ID: 2, Key: "a", Value: "a"},
		{ID: 1, Key: "a", Value: "a"},
	})

	iterator, ok = index.SeekBefore("b")
	if !ok {
		t.Fatal("SeekBefore() returned no iterator")
	}
	defer iterator.Close()
	assertReverseEntries(t, &iterator, []hatDataStructure.OrderedIndexEntry[string, string]{
		{ID: 2, Key: "a", Value: "a"},
		{ID: 1, Key: "a", Value: "a"},
	})

	iterator, ok = index.SeekBeforeOrEqual("b")
	if !ok {
		t.Fatal("SeekBeforeOrEqual() returned no iterator")
	}
	defer iterator.Close()
	assertReverseEntries(t, &iterator, []hatDataStructure.OrderedIndexEntry[string, string]{
		{ID: 3, Key: "b", Value: "b"},
		{ID: 2, Key: "a", Value: "a"},
		{ID: 1, Key: "a", Value: "a"},
	})
}

func TestOrderedIndexReverseSnapshotCursorSurvivesMutation(t *testing.T) {
	index, err := hatDataStructure.NewOrderedIndex(
		func(value int) int { return value },
		func(left, right int) int {
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		},
		0,
	)
	if err != nil {
		t.Fatalf("NewOrderedIndex() error = %v", err)
	}
	for id, value := range []int{1, 2, 3} {
		if err := index.Upsert(uint64(id+1), value); err != nil {
			t.Fatalf("Upsert(%d) error = %v", id+1, err)
		}
	}
	cursor, ok := index.LastSnapshotCursor()
	if !ok {
		t.Fatal("LastSnapshotCursor() returned no cursor")
	}
	defer cursor.Close()
	if err := index.Upsert(4, 4); err != nil {
		t.Fatalf("Upsert() after cursor creation error = %v", err)
	}
	assertReverseSnapshotEntries(t, &cursor, []hatDataStructure.OrderedIndexEntry[int, int]{
		{ID: 3, Key: 3, Value: 3},
		{ID: 2, Key: 2, Value: 2},
		{ID: 1, Key: 1, Value: 1},
	})
}

func TestOrderedIndexReverseIteratorInvalidatesAfterMutation(t *testing.T) {
	index, err := hatDataStructure.NewOrderedIndex(
		func(value int) int { return value },
		func(left, right int) int { return left - right },
		0,
	)
	if err != nil {
		t.Fatalf("NewOrderedIndex() error = %v", err)
	}
	if err := index.Upsert(1, 1); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	iterator, ok := index.Last()
	if !ok {
		t.Fatal("Last() returned no iterator")
	}
	defer iterator.Close()
	if err := index.Upsert(2, 2); err != nil {
		t.Fatalf("Upsert() after iterator creation error = %v", err)
	}
	if _, ok, err := iterator.Next(); err == nil || ok {
		t.Fatalf("reverse Next() after mutation = ok %v, err %v; want invalidation", ok, err)
	}
}

func TestOrderedIndexReverseSnapshotSeekBoundaries(t *testing.T) {
	index, err := hatDataStructure.NewOrderedIndex(
		func(value int) int { return value },
		func(left, right int) int { return left - right },
		0,
	)
	if err != nil {
		t.Fatalf("NewOrderedIndex() error = %v", err)
	}
	for id, value := range []int{1, 2, 2, 3} {
		if err := index.Upsert(uint64(id+1), value); err != nil {
			t.Fatalf("Upsert(%d) error = %v", value, err)
		}
	}
	cursor, ok := index.LastSnapshotCursor()
	if !ok {
		t.Fatal("LastSnapshotCursor() returned no cursor")
	}
	defer cursor.Close()
	if err := cursor.SeekBefore(2); err != nil {
		t.Fatalf("SeekBefore() error = %v", err)
	}
	entry, ok, err := cursor.Next()
	if err != nil || !ok || entry.Key != 1 {
		t.Fatalf("reverse snapshot SeekBefore(2) = %#v, %v, %v; want key 1", entry, ok, err)
	}
	if err := cursor.SeekBeforeOrEqual(2); err != nil {
		t.Fatalf("SeekBeforeOrEqual() error = %v", err)
	}
	entry, ok, err = cursor.Next()
	if err != nil || !ok || entry.Key != 2 {
		t.Fatalf("reverse snapshot SeekBeforeOrEqual(2) = %#v, %v, %v; want key 2", entry, ok, err)
	}
}

func assertReverseEntries[T any, K comparable](t *testing.T, iterator *hatDataStructure.OrderedIndexIterator[T, K], want []hatDataStructure.OrderedIndexEntry[T, K]) {
	t.Helper()
	for index, expected := range want {
		entry, ok, err := iterator.Next()
		if err != nil || !ok {
			t.Fatalf("reverse Next() at %d = %#v, %v, %v; want %#v", index, entry, ok, err, expected)
		}
		if !reflect.DeepEqual(entry, expected) {
			t.Fatalf("reverse Next() at %d = %#v, want %#v", index, entry, expected)
		}
	}
	if _, ok, err := iterator.Next(); err != nil || ok {
		t.Fatalf("reverse Next() at EOF = ok %v, err %v; want false, nil", ok, err)
	}
}

func assertReverseSnapshotEntries[T any, K comparable](t *testing.T, cursor *hatDataStructure.OrderedIndexSnapshotCursor[T, K], want []hatDataStructure.OrderedIndexEntry[T, K]) {
	t.Helper()
	for index, expected := range want {
		entry, ok, err := cursor.Next()
		if err != nil || !ok {
			t.Fatalf("reverse snapshot Next() at %d = %#v, %v, %v; want %#v", index, entry, ok, err, expected)
		}
		if !reflect.DeepEqual(entry, expected) {
			t.Fatalf("reverse snapshot Next() at %d = %#v, want %#v", index, entry, expected)
		}
	}
	if _, ok, err := cursor.Next(); err != nil || ok {
		t.Fatalf("reverse snapshot Next() at EOF = ok %v, err %v; want false, nil", ok, err)
	}
}
