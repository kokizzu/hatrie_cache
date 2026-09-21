package hatDataStructure_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTT021PackedRTreeRejectsNilVisitor(t *testing.T) {
	tree, err := hatDataStructure.NewPackedRTree([]hatDataStructure.SpatialEntry[int]{
		{
			Bounds: hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 1, MaxY: 1},
			Value:  1,
		},
	}, hatDataStructure.PackedRTreeOptions{})
	if err != nil {
		t.Fatalf("NewPackedRTree: %v", err)
	}
	if _, err := tree.Visit(hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 1, MaxY: 1}, nil); !errors.Is(err, hatDataStructure.ErrPackedRTreeVisitorRequired) {
		t.Fatalf("Visit(nil) error = %v, want ErrPackedRTreeVisitorRequired", err)
	}
}

func TestTT021SpatialBoxPublicIntersectionRejectsInvalidInput(t *testing.T) {
	valid := hatDataStructure.SpatialBox{MinX: 0, MinY: 0, MaxX: 1, MaxY: 1}
	invalid := hatDataStructure.SpatialBox{MinX: 2, MinY: 0, MaxX: 1, MaxY: 1}
	if valid.Intersects(invalid) {
		t.Fatal("Intersects returned true for an invalid box")
	}
}
