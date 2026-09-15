package hatDataStructure

import "testing"

func TestOrderedIndexSeekSmallBoundaries(t *testing.T) {
	tests := []struct {
		name           string
		keys           []int
		key            int
		forwardOK      bool
		forwardKey     int
		afterOK        bool
		afterKey       int
		beforeOK       bool
		beforeKey      int
		beforeEqualOK  bool
		beforeEqualKey int
	}{
		{name: "empty", key: 2},
		{name: "one-before", keys: []int{2}, key: 1, forwardOK: true, forwardKey: 2, afterOK: true, afterKey: 2},
		{name: "one-equal", keys: []int{2}, key: 2, forwardOK: true, forwardKey: 2, beforeEqualOK: true, beforeEqualKey: 2},
		{name: "one-after", keys: []int{2}, key: 3, beforeOK: true, beforeKey: 2, beforeEqualOK: true, beforeEqualKey: 2},
		{name: "two-first-equal", keys: []int{2, 4}, key: 2, forwardOK: true, forwardKey: 2, afterOK: true, afterKey: 4, beforeEqualOK: true, beforeEqualKey: 2},
		{name: "two-between", keys: []int{2, 4}, key: 3, forwardOK: true, forwardKey: 4, afterOK: true, afterKey: 4, beforeOK: true, beforeKey: 2, beforeEqualOK: true, beforeEqualKey: 2},
		{name: "two-second-equal", keys: []int{2, 4}, key: 4, forwardOK: true, forwardKey: 4, beforeOK: true, beforeKey: 2, beforeEqualOK: true, beforeEqualKey: 4},
		{name: "two-after", keys: []int{2, 4}, key: 5, beforeOK: true, beforeKey: 4, beforeEqualOK: true, beforeEqualKey: 4},
		{name: "duplicate-equal", keys: []int{2, 2}, key: 2, forwardOK: true, forwardKey: 2, beforeEqualOK: true, beforeEqualKey: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := newOrderedIndexC220BoundaryIndex(t, test.keys)
			assertOrderedIndexC220Forward(t, index, test.key, false, test.forwardOK, test.forwardKey)
			assertOrderedIndexC220Forward(t, index, test.key, true, test.afterOK, test.afterKey)
			assertOrderedIndexC220Reverse(t, index, test.key, false, test.beforeOK, test.beforeKey)
			assertOrderedIndexC220Reverse(t, index, test.key, true, test.beforeEqualOK, test.beforeEqualKey)
			assertOrderedIndexC220SnapshotForward(t, index, test.key, false, test.forwardOK, test.forwardKey)
			assertOrderedIndexC220SnapshotForward(t, index, test.key, true, test.afterOK, test.afterKey)
			assertOrderedIndexC220SnapshotReverse(t, index, test.key, false, test.beforeOK, test.beforeKey)
			assertOrderedIndexC220SnapshotReverse(t, index, test.key, true, test.beforeEqualOK, test.beforeEqualKey)
		})
	}
}

func newOrderedIndexC220BoundaryIndex(t testing.TB, keys []int) *OrderedIndex[int, int] {
	t.Helper()
	index, err := NewOrderedIndex(func(value int) int { return value }, orderedIndexC220Compare, len(keys))
	if err != nil {
		t.Fatal(err)
	}
	for id, key := range keys {
		if err := index.Upsert(uint64(id+1), key); err != nil {
			t.Fatal(err)
		}
	}
	return index
}

func assertOrderedIndexC220Forward(t *testing.T, index *OrderedIndex[int, int], key int, strict, wantOK bool, wantKey int) {
	t.Helper()
	var iterator OrderedIndexIterator[int, int]
	var ok bool
	if strict {
		iterator, ok = index.SeekAfter(key)
	} else {
		iterator, ok = index.Seek(key)
	}
	if ok != wantOK {
		t.Fatalf("forward seek key=%d strict=%v ok=%v, want %v", key, strict, ok, wantOK)
	}
	if !ok {
		return
	}
	defer iterator.Close()
	entry, next, err := iterator.Next()
	if err != nil || !next || entry.Key != wantKey {
		t.Fatalf("forward seek key=%d strict=%v = %#v, %v, %v; want key %d", key, strict, entry, next, err, wantKey)
	}
}

func assertOrderedIndexC220Reverse(t *testing.T, index *OrderedIndex[int, int], key int, inclusive, wantOK bool, wantKey int) {
	t.Helper()
	var iterator OrderedIndexIterator[int, int]
	var ok bool
	if inclusive {
		iterator, ok = index.SeekBeforeOrEqual(key)
	} else {
		iterator, ok = index.SeekBefore(key)
	}
	if ok != wantOK {
		t.Fatalf("reverse seek key=%d inclusive=%v ok=%v, want %v", key, inclusive, ok, wantOK)
	}
	if !ok {
		return
	}
	defer iterator.Close()
	entry, next, err := iterator.Next()
	if err != nil || !next || entry.Key != wantKey {
		t.Fatalf("reverse seek key=%d inclusive=%v = %#v, %v, %v; want key %d", key, inclusive, entry, next, err, wantKey)
	}
}

func assertOrderedIndexC220SnapshotForward(t *testing.T, index *OrderedIndex[int, int], key int, strict, wantOK bool, wantKey int) {
	t.Helper()
	cursor, ok := index.SnapshotCursor()
	if !ok {
		if wantOK {
			t.Fatalf("SnapshotCursor() = false for key=%d", key)
		}
		return
	}
	defer cursor.Close()
	var err error
	if strict {
		err = cursor.SeekAfter(key)
	} else {
		err = cursor.Seek(key)
	}
	if err != nil {
		t.Fatal(err)
	}
	entry, next, err := cursor.Next()
	if next != wantOK || err != nil {
		t.Fatalf("snapshot forward seek key=%d strict=%v = %#v, %v, %v; want next %v", key, strict, entry, next, err, wantOK)
	}
	if wantOK && entry.Key != wantKey {
		t.Fatalf("snapshot forward seek key=%d strict=%v key=%d, want %d", key, strict, entry.Key, wantKey)
	}
}

func assertOrderedIndexC220SnapshotReverse(t *testing.T, index *OrderedIndex[int, int], key int, inclusive, wantOK bool, wantKey int) {
	t.Helper()
	cursor, ok := index.LastSnapshotCursor()
	if !ok {
		if wantOK {
			t.Fatalf("LastSnapshotCursor() = false for key=%d", key)
		}
		return
	}
	defer cursor.Close()
	var err error
	if inclusive {
		err = cursor.SeekBeforeOrEqual(key)
	} else {
		err = cursor.SeekBefore(key)
	}
	if err != nil {
		t.Fatal(err)
	}
	entry, next, err := cursor.Next()
	if next != wantOK || err != nil {
		t.Fatalf("snapshot reverse seek key=%d inclusive=%v = %#v, %v, %v; want next %v", key, inclusive, entry, next, err, wantOK)
	}
	if wantOK && entry.Key != wantKey {
		t.Fatalf("snapshot reverse seek key=%d inclusive=%v key=%d, want %d", key, inclusive, entry.Key, wantKey)
	}
}
