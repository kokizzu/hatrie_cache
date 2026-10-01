package hatDataStructure

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestTU23TupleMultikeyIndexExpandsDeduplicatesAndLooksUp(t *testing.T) {
	index := NewTupleMultikeyIndex(TupleMultikeyIndexOptions{
		MaxCombinationsPerItem: 8,
		MaxItems:               8,
	})
	if err := index.Set(2, [][]string{{"red", "blue", "red"}, {"small", "large"}}); err != nil {
		t.Fatalf("set item 2: %v", err)
	}
	if err := index.Set(1, [][]string{{"red"}, {"small"}}); err != nil {
		t.Fatalf("set item 1: %v", err)
	}
	if err := index.Set(3, [][]string{{"red"}, {"large"}}); err != nil {
		t.Fatalf("set item 3: %v", err)
	}

	if got := index.Lookup([]string{"red", "small"}, nil); !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("red/small lookup = %#v, want [1 2]", got)
	}
	if got := index.Lookup([]string{"red", "large"}, nil); !reflect.DeepEqual(got, []uint64{2, 3}) {
		t.Fatalf("red/large lookup = %#v, want [2 3]", got)
	}
	if !index.Contains([]string{"blue", "large"}, 2) || index.Contains([]string{"blue", "large"}, 1) {
		t.Fatal("Contains returned an incorrect result")
	}
	if got := index.KeyCount(); got != 4 {
		t.Fatalf("key count = %d, want 4", got)
	}
	if got := index.Len(); got != 3 {
		t.Fatalf("item count = %d, want 3", got)
	}

	destination := make([]uint64, 0, 2)
	got := index.Lookup([]string{"red", "small"}, destination)
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("reused lookup = %#v", got)
	}
}

func TestTU23TupleMultikeyIndexSeparatesAmbiguousValuesAndMaintainsUpdates(t *testing.T) {
	index := NewTupleMultikeyIndex(TupleMultikeyIndexOptions{})
	if err := index.Set(7, [][]string{{"a\x00b", "same"}, {"c"}}); err != nil {
		t.Fatalf("set item 7: %v", err)
	}
	if err := index.Set(8, [][]string{{"a"}, {"b\x00c"}}); err != nil {
		t.Fatalf("set item 8: %v", err)
	}
	if got := index.Lookup([]string{"a\x00b", "c"}, nil); !reflect.DeepEqual(got, []uint64{7}) {
		t.Fatalf("first ambiguous lookup = %#v, want [7]", got)
	}
	if got := index.Lookup([]string{"a", "b\x00c"}, nil); !reflect.DeepEqual(got, []uint64{8}) {
		t.Fatalf("second ambiguous lookup = %#v, want [8]", got)
	}

	if err := index.Set(7, [][]string{{"updated"}, {"value"}}); err != nil {
		t.Fatalf("update item 7: %v", err)
	}
	if got := index.Lookup([]string{"a\x00b", "c"}, nil); len(got) != 0 {
		t.Fatalf("old key after update = %#v, want empty", got)
	}
	if got := index.Lookup([]string{"updated", "value"}, nil); !reflect.DeepEqual(got, []uint64{7}) {
		t.Fatalf("new key after update = %#v, want [7]", got)
	}
	if !index.Delete(8) || index.Delete(8) {
		t.Fatal("delete lifecycle returned an incorrect result")
	}
	if got := index.Len(); got != 1 {
		t.Fatalf("item count after delete = %d, want 1", got)
	}
	if err := index.Set(7, [][]string{{}}); err != nil {
		t.Fatalf("clear item: %v", err)
	}
	if got := index.Len(); got != 0 {
		t.Fatalf("item count after clear = %d, want 0", got)
	}
}

func TestTU23TupleMultikeyIndexRejectsExplosionsAtomically(t *testing.T) {
	index := NewTupleMultikeyIndex(TupleMultikeyIndexOptions{
		MaxCombinationsPerItem: 4,
		MaxItems:               1,
	})
	if err := index.Set(1, [][]string{{"a"}, {"b"}}); err != nil {
		t.Fatalf("set initial item: %v", err)
	}
	if err := index.Set(1, [][]string{{"a", "b"}, {"c", "d", "e"}}); !errors.Is(err, ErrTupleMultikeyCombinationLimit) {
		t.Fatalf("combination limit error = %v, want ErrTupleMultikeyCombinationLimit", err)
	}
	if got := index.Lookup([]string{"a", "b"}, nil); !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("old key after rejected update = %#v, want [1]", got)
	}
	if got := index.Lookup([]string{"a", "c"}, nil); len(got) != 0 {
		t.Fatalf("new key after rejected update = %#v, want empty", got)
	}
	if err := index.Set(2, [][]string{{"other"}, {"value"}}); err == nil {
		t.Fatal("item limit violation unexpectedly succeeded")
	}
	if err := index.Set(1, nil); err != nil {
		t.Fatalf("clear existing item: %v", err)
	}
	if err := index.Set(2, [][]string{{"other"}, {"value"}}); err != nil {
		t.Fatalf("set item after clear: %v", err)
	}
}

func TestTU23TupleMultikeyIndexConcurrentReadsAndWrites(t *testing.T) {
	index := NewTupleMultikeyIndex(TupleMultikeyIndexOptions{MaxCombinationsPerItem: 4, MaxItems: 64})
	if err := index.Set(0, [][]string{{"initial"}, {"value"}}); err != nil {
		t.Fatalf("set initial item: %v", err)
	}
	var waitGroup sync.WaitGroup
	waitGroup.Add(3)
	go func() {
		defer waitGroup.Done()
		for iteration := 0; iteration < 1000; iteration++ {
			id := uint64(iteration % 64)
			if err := index.Set(id, [][]string{{"region-" + string(rune('a'+iteration%8))}, {"kind"}}); err != nil {
				t.Errorf("concurrent set: %v", err)
				return
			}
		}
	}()
	go func() {
		defer waitGroup.Done()
		destination := make([]uint64, 0, 64)
		for iteration := 0; iteration < 1000; iteration++ {
			destination = index.Lookup([]string{"region-" + string(rune('a'+iteration%8)), "kind"}, destination)
		}
	}()
	go func() {
		defer waitGroup.Done()
		for iteration := 0; iteration < 1000; iteration++ {
			index.Contains([]string{"region-" + string(rune('a'+iteration%8)), "kind"}, uint64(iteration%64))
			index.Len()
			index.KeyCount()
		}
	}()
	waitGroup.Wait()
}
