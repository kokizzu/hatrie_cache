package hatDataStructure

import (
	"reflect"
	"testing"
)

func TestU64PostingListPreservesInlineAndOverflowOrder(t *testing.T) {
	list := newU64PostingList(9)
	if list.rest != nil {
		t.Fatalf("singleton posting allocated overflow storage: %#v", list)
	}
	list = list.appendInOrder(3)
	list = list.appendInOrder(9)
	if got, want := list.values(nil), []uint64{9, 3, 9}; !reflect.DeepEqual(got, want) {
		t.Fatalf("insertion-order values = %v, want %v", got, want)
	}

	list = newU64PostingList(9)
	for _, id := range []uint64{4, 12, 0, 7} {
		list = list.insertSorted(id)
	}
	if got, want := list.values(nil), []uint64{0, 4, 7, 9, 12}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sorted values = %v, want %v", got, want)
	}
	if list = list.insertSorted(7); !reflect.DeepEqual(list.values(nil), []uint64{0, 4, 7, 9, 12}) {
		t.Fatalf("duplicate sorted insert changed values: %v", list.values(nil))
	}
}

func TestU64PostingListRemovesZeroAndCollapsesToSingleton(t *testing.T) {
	list := newU64PostingList(0).insertSorted(4).insertSorted(8).insertSorted(12)
	var removed, empty bool
	list, removed, empty = list.removeSorted(0)
	if !removed || empty {
		t.Fatal("removeSorted(0) reported false")
	}
	if got, want := list.values(nil), []uint64{4, 8, 12}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after removing zero = %v, want %v", got, want)
	}
	list, removed, empty = list.removeSorted(4)
	if !removed || empty || list.rest == nil {
		t.Fatalf("removeSorted(4) did not retain multi-list state: list=%#v removed=%v", list, removed)
	}
	list, removed, empty = list.removeSorted(8)
	if !removed || empty || list.rest != nil || list.first != 12 {
		t.Fatalf("singleton collapse did not happen correctly: list=%#v removed=%v", list, removed)
	}
	list, removed, empty = list.removeSorted(12)
	if !removed || !empty || list.rest != nil || list.first != 0 {
		t.Fatalf("final removal did not empty correctly: list=%#v removed=%v", list, removed)
	}
}

func TestCompactPostingIndexesHandleSingletonZeroID(t *testing.T) {
	functional, err := NewFunctionalIndex(func(value int) int { return value }, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := functional.Upsert(0, 7); err != nil {
		t.Fatal(err)
	}
	if got := functional.LookupIDs(7); !reflect.DeepEqual(got, []uint64{0}) {
		t.Fatalf("functional singleton IDs = %v, want [0]", got)
	}

	hash, err := NewHashIndex(func(value int) int { return value }, HashIndexOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := hash.Upsert(0, 11); err != nil {
		t.Fatal(err)
	}
	if got := hash.LookupIDs(11); !reflect.DeepEqual(got, []uint64{0}) {
		t.Fatalf("hash singleton IDs = %v, want [0]", got)
	}

	multikey := NewStringMultikeyIndex(StringMultikeyIndexOptions{})
	if err := multikey.Set(0, []string{"tag"}); err != nil {
		t.Fatal(err)
	}
	if got := multikey.Lookup("tag", nil); !reflect.DeepEqual(got, []uint64{0}) || !multikey.Contains("tag", 0) {
		t.Fatalf("multikey singleton lookup = %v, contains = false", got)
	}

	conditional, err := NewConditionalFunctionalIndex(
		func(value int) int { return value },
		func(value int) bool { return value > 0 },
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := conditional.Upsert(0, 1); err != nil {
		t.Fatal(err)
	}
	if !conditional.Contains(1, 0) {
		t.Fatal("conditional index did not find singleton zero ID")
	}
}
