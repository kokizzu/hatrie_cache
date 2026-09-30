package hatDataStructure

import (
	"reflect"
	"testing"
)

func TestSparseBitsetValuesIntoMatchesValuesAndReusesDestination(t *testing.T) {
	var bitset SparseBitset
	for _, value := range []uint64{9, 1, 1 << 16, 7, 1 << 32} {
		bitset.Add(value)
	}
	want := bitset.Values()
	destination := make([]uint64, 0, len(want))
	got := bitset.ValuesInto(destination)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ValuesInto() = %#v, Values() = %#v", got, want)
	}
	if len(got) == 0 || &got[0] != &destination[:1][0] {
		t.Fatal("ValuesInto() did not reuse destination backing storage")
	}

	bitset.Add(3)
	bitset.Remove(7)
	got = bitset.ValuesInto(got[:0])
	want = bitset.Values()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reused ValuesInto() = %#v, Values() = %#v", got, want)
	}
}

func TestSparseBitsetValuesIntoEmptyKeepsDestinationReusable(t *testing.T) {
	destination := make([]uint64, 0, 4)
	var bitset SparseBitset
	got := bitset.ValuesInto(destination)
	if len(got) != 0 || cap(got) != cap(destination) {
		t.Fatalf("empty ValuesInto() len/cap = %d/%d, want 0/%d", len(got), cap(got), cap(destination))
	}
}
