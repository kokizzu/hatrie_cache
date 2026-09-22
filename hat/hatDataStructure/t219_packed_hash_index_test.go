//go:build t219

package hatDataStructure

import (
	"errors"
	"testing"
)

func t219HashString(value string) uint64 {
	var hash uint64 = 1469598103934665603
	for index := 0; index < len(value); index++ {
		hash ^= uint64(value[index])
		hash *= 1099511628211
	}
	return hash
}

func TestPackedHashIndexLookupAndCollisionResolution(t *testing.T) {
	index, err := NewPackedHashIndex[int, string](t219HashString, []HashIndexEntry[int, string]{
		{ID: 3, Key: "three", Value: 30},
		{ID: 1, Key: "one", Value: 10},
		{ID: 2, Key: "two", Value: 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]HashIndexEntry[int, string]{
		"one":   {ID: 1, Key: "one", Value: 10},
		"two":   {ID: 2, Key: "two", Value: 20},
		"three": {ID: 3, Key: "three", Value: 30},
	} {
		if got, ok := index.Lookup(key); !ok || got != want {
			t.Fatalf("Lookup(%q) = %#v/%t, want %#v/true", key, got, ok, want)
		}
	}
	if index.Contains("missing") {
		t.Fatal("Contains(missing) = true")
	}
	if _, ok := index.Lookup("missing"); ok {
		t.Fatal("Lookup(missing) = true")
	}
	if got := index.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}
	if got := index.Capacity(); got < 4 {
		t.Fatalf("Capacity() = %d, want room for the load factor", got)
	}

	colliding, err := NewPackedHashIndex[int, string](func(string) uint64 { return 1 }, []HashIndexEntry[int, string]{
		{ID: 1, Key: "a", Value: 1},
		{ID: 2, Key: "b", Value: 2},
		{ID: 3, Key: "c", Value: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]int{"a": 1, "b": 2, "c": 3} {
		if got, ok := colliding.Lookup(key); !ok || got.Value != want {
			t.Fatalf("collision Lookup(%q) = %#v/%t, want value %d", key, got, ok, want)
		}
	}
}

func TestPackedHashIndexRejectsDuplicateKeysAndMissingConfiguration(t *testing.T) {
	if _, err := NewPackedHashIndex[int, string](t219HashString, []HashIndexEntry[int, string]{
		{ID: 1, Key: "duplicate", Value: 1},
		{ID: 2, Key: "duplicate", Value: 2},
	}); !errors.Is(err, ErrPackedHashIndexDuplicateKey) {
		t.Fatalf("duplicate key error = %v, want %v", err, ErrPackedHashIndexDuplicateKey)
	}
	if _, err := NewPackedHashIndex[int, string](nil, nil); !errors.Is(err, ErrPackedHashIndexHashRequired) {
		t.Fatalf("nil hash error = %v, want %v", err, ErrPackedHashIndexHashRequired)
	}
	var index *PackedHashIndex[int, string]
	if _, ok := index.Lookup("missing"); ok {
		t.Fatal("nil Lookup() = true")
	}
	if index.Contains("missing") || index.Len() != 0 || index.Capacity() != 0 {
		t.Fatal("nil index reported state")
	}
}

func TestPackedHashIndexLookupDoesNotAllocate(t *testing.T) {
	index, err := NewPackedHashIndex[int, int](func(value int) uint64 { return uint64(value) }, []HashIndexEntry[int, int]{
		{ID: 1, Key: 1, Value: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(100, func() {
		if entry, ok := index.Lookup(1); !ok || entry.Value != 10 {
			t.Fatal("Lookup(1) failed")
		}
	}); got != 0 {
		t.Fatalf("Lookup() allocation count = %f, want 0", got)
	}
}
