package hatDataStructure_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestRTreePublicAPI(t *testing.T) {
	tree, err := hatDataStructure.NewRTree(0)
	if err != nil {
		t.Fatalf("NewRTree() error = %v", err)
	}
	if err := tree.Upsert(42, hatDataStructure.RTreeBounds{MinX: -1, MinY: -1, MaxX: 1, MaxY: 1}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	ids, err := tree.SearchPoint(0, 0)
	if err != nil {
		t.Fatalf("SearchPoint() error = %v", err)
	}
	if !reflect.DeepEqual(ids, []uint64{42}) {
		t.Fatalf("SearchPoint() = %v, want [42]", ids)
	}
}
