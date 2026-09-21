package hatDataStructure_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTT021MutablePackedRTreeUpdatesAndCompacts(t *testing.T) {
	tree, err := hatDataStructure.NewMutablePackedRTree([]hatDataStructure.MutableSpatialEntry[string]{
		{ID: 1, Bounds: hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 1, MaxY: 1}, Value: "west"},
		{ID: 2, Bounds: hatDataStructure.SpatialBox{MinX: 10, MinY: 10, MaxX: 11, MaxY: 11}, Value: "east"},
		{ID: 3, Bounds: hatDataStructure.SpatialBox{MinX: 20, MinY: 20, MaxX: 21, MaxY: 21}, Value: "far"},
	}, hatDataStructure.PackedRTreeOptions{LeafSize: 2})
	if err != nil {
		t.Fatalf("NewMutablePackedRTree() error = %v", err)
	}
	got, err := tree.Query(hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 12, MaxY: 12})
	if err != nil {
		t.Fatalf("initial Query() error = %v", err)
	}
	if want := []string{"west", "east"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("initial Query() = %#v, want %#v", got, want)
	}

	if err := tree.Upsert(1, hatDataStructure.SpatialBox{MinX: 30, MinY: 30, MaxX: 31, MaxY: 31}, "moved"); err != nil {
		t.Fatalf("Upsert() replacement error = %v", err)
	}
	if err := tree.Upsert(4, hatDataStructure.SpatialBox{MinX: 5, MinY: 5, MaxX: 6, MaxY: 6}, "middle"); err != nil {
		t.Fatalf("Upsert() insert error = %v", err)
	}
	if !tree.Delete(2) || tree.Delete(99) {
		t.Fatal("Delete() presence result is incorrect")
	}
	if tree.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", tree.Len())
	}
	if tree.PendingUpdates() != 3 {
		t.Fatalf("PendingUpdates() = %d, want 3", tree.PendingUpdates())
	}

	got, err = tree.Query(hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 12, MaxY: 12})
	if err != nil {
		t.Fatalf("overlay Query() error = %v", err)
	}
	if want := []string{"middle"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("overlay Query() = %#v, want %#v", got, want)
	}
	got, err = tree.Query(hatDataStructure.SpatialBox{MinX: 29, MinY: 29, MaxX: 32, MaxY: 32})
	if err != nil {
		t.Fatalf("moved Query() error = %v", err)
	}
	if want := []string{"moved"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("moved Query() = %#v, want %#v", got, want)
	}

	destination := make([]string, 1, 4)
	destination[0] = "stale"
	got, err = tree.QueryInto(hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 12, MaxY: 12}, destination)
	if err != nil {
		t.Fatalf("QueryInto() error = %v", err)
	}
	if !reflect.DeepEqual(got, []string{"middle"}) || cap(got) != cap(destination) {
		t.Fatalf("QueryInto() = %#v cap=%d, want [middle] cap=%d", got, cap(got), cap(destination))
	}

	if err := tree.Compact(); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if tree.PendingUpdates() != 0 {
		t.Fatalf("PendingUpdates() after Compact() = %d, want 0", tree.PendingUpdates())
	}
	got, err = tree.Query(hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 12, MaxY: 12})
	if err != nil {
		t.Fatalf("compacted Query() error = %v", err)
	}
	if want := []string{"middle"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("compacted Query() = %#v, want %#v", got, want)
	}
}

func TestTT021MutablePackedRTreeRejectsInvalidAndDuplicateEntries(t *testing.T) {
	if _, err := hatDataStructure.NewMutablePackedRTree([]hatDataStructure.MutableSpatialEntry[int]{
		{ID: 1, Bounds: hatDataStructure.SpatialBox{MaxX: 1, MaxY: 1}, Value: 1},
		{ID: 1, Bounds: hatDataStructure.SpatialBox{MaxX: 2, MaxY: 2}, Value: 2},
	}, hatDataStructure.PackedRTreeOptions{}); err == nil {
		t.Fatal("duplicate IDs unexpectedly succeeded")
	}
	tree, err := hatDataStructure.NewMutablePackedRTree([]hatDataStructure.MutableSpatialEntry[int]{
		{ID: 1, Bounds: hatDataStructure.SpatialBox{MaxX: 1, MaxY: 1}, Value: 1},
	}, hatDataStructure.PackedRTreeOptions{})
	if err != nil {
		t.Fatalf("NewMutablePackedRTree() error = %v", err)
	}
	if err := tree.Upsert(2, hatDataStructure.SpatialBox{MinX: 2, MaxX: 1, MaxY: 1}, 2); err == nil {
		t.Fatal("invalid Upsert() unexpectedly succeeded")
	}
	if tree.Len() != 1 {
		t.Fatalf("Len() after invalid Upsert() = %d, want 1", tree.Len())
	}
}
