package hatDataStructure_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestSparseBitsetIsUsableByImporters(t *testing.T) {
	bitset := hatDataStructure.NewSparseBitset()
	values := []uint64{0, 1, 65535, 65536, 1 << 40, ^uint64(0)}
	if got := bitset.Add(values[0], values[1:]...); got != len(values) {
		t.Fatalf("Add() = %d, want %d", got, len(values))
	}
	if bitset.Add(1) != 0 || !bitset.Contains(1<<40) || bitset.Contains(2) {
		t.Fatal("duplicate or membership behavior changed")
	}
	if got := bitset.Remove(1, 2); got != 1 {
		t.Fatalf("Remove() = %d, want 1", got)
	}
	want := []uint64{0, 65535, 65536, 1 << 40, ^uint64(0)}
	if got := bitset.Values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Values() = %v, want %v", got, want)
	}
	if info := bitset.Info(); info.Cardinality != uint64(len(want)) || info.Containers != 4 {
		t.Fatalf("Info() = %#v, want five values across four containers", info)
	}
	restored, err := hatDataStructure.NewSparseBitsetFromSnapshot(bitset.Snapshot())
	if err != nil {
		t.Fatalf("NewSparseBitsetFromSnapshot() error = %v", err)
	}
	if got := restored.Values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored Values() = %v, want %v", got, want)
	}
}
