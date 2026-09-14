package hatDataStructure

import (
	"reflect"
	"testing"
)

func TestTR026BitmapIndexTypedLookupAndUpdates(t *testing.T) {
	index := NewBitmapIndex[string]()
	if !index.Add("active", 1) || !index.Add("active", 3) || !index.Add("inactive", 2) {
		t.Fatal("first inserts were not reported as new")
	}
	if index.Add("active", 1) {
		t.Fatal("duplicate insert was reported as new")
	}
	if !index.Contains("active", 1) || index.Contains("active", 2) {
		t.Fatal("Contains returned the wrong membership")
	}
	if got, want := index.Rows("active"), []uint32{1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Rows(active) = %v, want %v", got, want)
	}
	if !index.Remove("active", 1) || index.Remove("active", 1) {
		t.Fatal("remove membership result is wrong")
	}
	if got, want := index.Rows("active"), []uint32{3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Rows(active) after remove = %v, want %v", got, want)
	}
	if got := index.ValueCount(); got != 2 {
		t.Fatalf("ValueCount = %d, want 2", got)
	}
	if got := index.IndexedRows(); got != 2 {
		t.Fatalf("IndexedRows = %d, want 2", got)
	}
}

func TestTR026BitmapIndexUnionIntersectionAndVisit(t *testing.T) {
	index := NewBitmapIndex[string]()
	for _, item := range []struct {
		key string
		row uint32
	}{
		{key: "red", row: 1},
		{key: "red", row: 2},
		{key: "red", row: 4},
		{key: "large", row: 2},
		{key: "large", row: 4},
		{key: "large", row: 5},
	} {
		index.Add(item.key, item.row)
	}
	if got, want := index.Intersect("red", "large").Values(), []uint32{2, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Intersect = %v, want %v", got, want)
	}
	if got, want := index.Union("red", "large").Values(), []uint32{1, 2, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Union = %v, want %v", got, want)
	}
	var visited []uint32
	completed := index.Visit("red", func(row uint32) bool {
		visited = append(visited, row)
		return true
	})
	if !completed || !reflect.DeepEqual(visited, []uint32{1, 2, 4}) {
		t.Fatalf("Visit = %v/%v, want [1 2 4]/true", visited, completed)
	}
	completed = index.Visit("red", func(uint32) bool { return false })
	if completed {
		t.Fatal("Visit stop was reported as complete")
	}
}

func TestTR026BitmapIndexVisitsDenseContainers(t *testing.T) {
	index := NewBitmapIndex[string]()
	for row := uint32(0); row < 5000; row++ {
		if !index.Add("dense", row) {
			t.Fatalf("row %d was unexpectedly already indexed", row)
		}
	}
	var first, last uint32
	var visited uint32
	if !index.Visit("dense", func(row uint32) bool {
		if visited == 0 {
			first = row
		}
		last = row
		visited++
		return true
	}) {
		t.Fatal("dense Visit stopped unexpectedly")
	}
	if visited != 5000 || first != 0 || last != 4999 {
		t.Fatalf("dense Visit = count %d range %d..%d, want 5000 range 0..4999", visited, first, last)
	}
}

func TestTR026BitmapIndexSupportsNumericKeysAndZeroValue(t *testing.T) {
	var index BitmapIndex[uint64]
	if !index.Add(20260914, 42) || !index.Contains(20260914, 42) {
		t.Fatal("zero-value numeric index did not initialize")
	}
	if index.Contains(20260915, 42) || index.Rows(20260915) != nil {
		t.Fatal("missing numeric key returned a match")
	}
	info := index.Info()
	if info.DistinctValues != 1 || info.IndexedRows != 1 || info.EncodedBytes == 0 {
		t.Fatalf("Info = %#v, want one indexed value and row", info)
	}
}

func TestTR026BitmapIndexNilAndEmptyInputs(t *testing.T) {
	var index *BitmapIndex[string]
	if index.Add("key", 1) || index.Remove("key", 1) || index.Contains("key", 1) {
		t.Fatal("nil index operation reported a match")
	}
	if index.Rows("key") != nil || index.ValueCount() != 0 || index.IndexedRows() != 0 {
		t.Fatal("nil index reported non-empty state")
	}
	if index.Intersect("key").Count() != 0 || index.Union("key").Count() != 0 {
		t.Fatal("missing key set operation returned rows")
	}
}
