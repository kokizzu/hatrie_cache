package hatDataStructure_test

import (
	"errors"
	"reflect"
	"sort"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTT021PackedRTreeQueriesIntersectingEntries(t *testing.T) {
	entries := []hatDataStructure.SpatialEntry[string]{
		{Bounds: hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 1, MaxY: 1}, Value: "west"},
		{Bounds: hatDataStructure.SpatialBox{MinX: 2, MinY: 2, MaxX: 3, MaxY: 3}, Value: "center"},
		{Bounds: hatDataStructure.SpatialBox{MinX: 5, MinY: 5, MaxX: 6, MaxY: 6}, Value: "east"},
	}
	tree, err := hatDataStructure.NewPackedRTree(entries, hatDataStructure.PackedRTreeOptions{LeafSize: 2})
	if err != nil {
		t.Fatalf("NewPackedRTree() error = %v", err)
	}
	got, err := tree.Query(hatDataStructure.SpatialBox{MinX: 1, MinY: 1, MaxX: 5, MaxY: 5})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	sort.Strings(got)
	if want := []string{"center", "east", "west"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Query() = %#v, want %#v", got, want)
	}
}

func TestTT021PackedRTreeQueryIntoReusesDestinationAndVisitCanStop(t *testing.T) {
	entries := []hatDataStructure.SpatialEntry[int]{
		{Bounds: hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 1, MaxY: 1}, Value: 1},
		{Bounds: hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 2, MaxY: 2}, Value: 2},
		{Bounds: hatDataStructure.SpatialBox{MinX: 10, MinY: 10, MaxX: 11, MaxY: 11}, Value: 3},
	}
	tree, err := hatDataStructure.NewPackedRTree(entries, hatDataStructure.PackedRTreeOptions{})
	if err != nil {
		t.Fatalf("NewPackedRTree() error = %v", err)
	}
	destination := make([]int, 0, 2)
	got, err := tree.QueryInto(hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 2, MaxY: 2}, destination)
	if err != nil {
		t.Fatalf("QueryInto() error = %v", err)
	}
	if len(got) != 2 || cap(got) != cap(destination) {
		t.Fatalf("QueryInto() = %#v with cap %d, want two results and reused capacity", got, cap(got))
	}
	visited := 0
	count, err := tree.Visit(hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 2, MaxY: 2}, func(int) bool {
		visited++
		return visited < 1
	})
	if err != nil {
		t.Fatalf("Visit() error = %v", err)
	}
	if count != 1 || visited != 1 {
		t.Fatalf("Visit() count/visits = %d/%d, want 1/1", count, visited)
	}
}

func TestTT021PackedRTreeValidatesBoxesAndDoesNotBorrowInput(t *testing.T) {
	entries := []hatDataStructure.SpatialEntry[string]{
		{Bounds: hatDataStructure.SpatialBox{MinX: 1, MinY: 1, MaxX: 2, MaxY: 2}, Value: "value"},
	}
	tree, err := hatDataStructure.NewPackedRTree(entries, hatDataStructure.PackedRTreeOptions{LeafSize: 1})
	if err != nil {
		t.Fatalf("NewPackedRTree() error = %v", err)
	}
	entries[0].Value = "caller-mutated"
	got, err := tree.Query(hatDataStructure.SpatialBox{MinX: 1, MinY: 1, MaxX: 2, MaxY: 2})
	if err != nil || !reflect.DeepEqual(got, []string{"value"}) {
		t.Fatalf("input mutation changed tree = %#v/%v", got, err)
	}
	for name, box := range map[string]hatDataStructure.SpatialBox{
		"reversed x": {MinX: 2, MinY: 1, MaxX: 1, MaxY: 2},
		"reversed y": {MinX: 1, MinY: 2, MaxX: 2, MaxY: 1},
	} {
		if _, err := tree.Query(box); !errors.Is(err, hatDataStructure.ErrSpatialBoxInvalid) {
			t.Fatalf("Query(%s) error = %v, want ErrSpatialBoxInvalid", name, err)
		}
	}
	if _, err := hatDataStructure.NewPackedRTree(entries, hatDataStructure.PackedRTreeOptions{LeafSize: 0}); err != nil {
		t.Fatalf("zero LeafSize should use default, error = %v", err)
	}
}

func TestTT021PackedRTreeEmptyAndNilInputs(t *testing.T) {
	tree, err := hatDataStructure.NewPackedRTree[int](nil, hatDataStructure.PackedRTreeOptions{})
	if err != nil {
		t.Fatalf("empty NewPackedRTree() error = %v", err)
	}
	if tree.Len() != 0 {
		t.Fatalf("empty Len() = %d, want 0", tree.Len())
	}
	if got, err := tree.Query(hatDataStructure.SpatialBox{MaxX: 1, MaxY: 1}); err != nil || len(got) != 0 {
		t.Fatalf("empty Query() = %#v/%v, want empty result", got, err)
	}
	var nilTree *hatDataStructure.PackedRTree[int]
	if _, err := nilTree.Query(hatDataStructure.SpatialBox{MaxX: 1, MaxY: 1}); !errors.Is(err, hatDataStructure.ErrPackedRTreeNil) {
		t.Fatalf("nil Query() error = %v, want ErrPackedRTreeNil", err)
	}
}
