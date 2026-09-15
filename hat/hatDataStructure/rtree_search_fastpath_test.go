package hatDataStructure

import "testing"

func TestRTreeSearchSmallResultsRemainSorted(t *testing.T) {
	tree := NewDefaultRTree()
	for _, id := range []uint64{20, 10} {
		if err := tree.Upsert(id, RTreeBounds{MinX: float64(id), MinY: 0, MaxX: float64(id) + 0.5, MaxY: 0.5}); err != nil {
			t.Fatalf("Upsert(%d) error = %v", id, err)
		}
	}

	got, err := tree.Search(RTreeBounds{MinX: 9, MinY: -1, MaxX: 21, MaxY: 1})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(got) != 2 || got[0] != 10 || got[1] != 20 {
		t.Fatalf("Search() = %v, want [10 20]", got)
	}

	got, err = tree.Search(RTreeBounds{MinX: 10, MinY: 0, MaxX: 10.5, MaxY: 0.5})
	if err != nil {
		t.Fatalf("single-result Search() error = %v", err)
	}
	if len(got) != 1 || got[0] != 10 {
		t.Fatalf("single-result Search() = %v, want [10]", got)
	}

	got, err = tree.Search(RTreeBounds{MinX: -2, MinY: -2, MaxX: -1, MaxY: -1})
	if err != nil {
		t.Fatalf("empty Search() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty Search() = %v, want empty", got)
	}

	got, err = tree.SearchInto([]uint64{99}, RTreeBounds{MinX: 10, MinY: 0, MaxX: 10.5, MaxY: 0.5})
	if err != nil {
		t.Fatalf("SearchInto() error = %v", err)
	}
	if len(got) != 2 || got[0] != 99 || got[1] != 10 {
		t.Fatalf("SearchInto() = %v, want [99 10]", got)
	}
}
