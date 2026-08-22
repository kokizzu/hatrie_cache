package hatDataStructure_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestRoaringBitmapIsUsableByImporters(t *testing.T) {
	bitmap := hatDataStructure.NewRoaringBitmap()
	if got := bitmap.Add(0, 1, 65535, 65536, 1<<31); got != 5 {
		t.Fatalf("Add() = %d, want 5", got)
	}
	if bitmap.Add(1) != 0 {
		t.Fatal("Add(duplicate) = non-zero, want 0")
	}
	if !bitmap.Contains(65536) || bitmap.Contains(2) {
		t.Fatal("Contains() does not report inserted and absent values")
	}
	if got := bitmap.Remove(1, 2); got != 1 {
		t.Fatalf("Remove() = %d, want 1", got)
	}
	if got, want := bitmap.Values(), []uint32{0, 65535, 65536, 1 << 31}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Values() = %v, want %v", got, want)
	}
	if info := bitmap.Info(); info.Cardinality != 4 || info.Containers != 3 {
		t.Fatalf("Info() = %#v, want four values across three containers", info)
	}

	snapshot := bitmap.Snapshot()
	restored, err := hatDataStructure.NewRoaringBitmapFromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("NewRoaringBitmapFromSnapshot() error = %v", err)
	}
	if got := restored.Values(); !reflect.DeepEqual(got, bitmap.Values()) {
		t.Fatalf("restored Values() = %v, want %v", got, bitmap.Values())
	}

	var containers int
	restored.VisitContainers(func(key uint16, cardinality uint32, values []uint16, bitset []uint64) bool {
		containers++
		if cardinality == 0 || (len(values) == 0 && len(bitset) == 0) {
			t.Fatalf("container %d is missing its read-only payload", key)
		}
		return true
	})
	if containers != 3 {
		t.Fatalf("VisitContainers() visited %d containers, want 3", containers)
	}
}
