package hatDataStructure

import (
	"math"
	"reflect"
	"sync"
	"testing"
)

func TestRTreeSearchUpsertAndDelete(t *testing.T) {
	tree, err := NewRTree(4)
	if err != nil {
		t.Fatalf("NewRTree() error = %v", err)
	}
	items := []struct {
		id     uint64
		bounds RTreeBounds
	}{
		{1, RTreeBounds{MinX: 0, MinY: 0, MaxX: 2, MaxY: 2}},
		{2, RTreeBounds{MinX: 10, MinY: 10, MaxX: 12, MaxY: 12}},
		{3, RTreeBounds{MinX: 1, MinY: 1, MaxX: 4, MaxY: 4}},
	}
	for _, item := range items {
		if err := tree.Upsert(item.id, item.bounds); err != nil {
			t.Fatalf("Upsert(%d) error = %v", item.id, err)
		}
	}
	got, err := tree.Search(RTreeBounds{MinX: 1, MinY: 1, MaxX: 3, MaxY: 3})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if want := []uint64{1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Search() = %v, want %v", got, want)
	}
	reused, err := tree.SearchInto([]uint64{99}, RTreeBounds{MinX: 1, MinY: 1, MaxX: 3, MaxY: 3})
	if err != nil {
		t.Fatalf("SearchInto() error = %v", err)
	}
	if want := []uint64{99, 1, 3}; !reflect.DeepEqual(reused, want) {
		t.Fatalf("SearchInto() = %v, want %v", reused, want)
	}
	got, err = tree.SearchPoint(11, 11)
	if err != nil {
		t.Fatalf("SearchPoint() error = %v", err)
	}
	if want := []uint64{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("SearchPoint() = %v, want %v", got, want)
	}
	pointBuffer, err := tree.SearchPointInto(make([]uint64, 0, 1), 11, 11)
	if err != nil {
		t.Fatalf("SearchPointInto() error = %v", err)
	}
	if want := []uint64{2}; !reflect.DeepEqual(pointBuffer, want) {
		t.Fatalf("SearchPointInto() = %v, want %v", pointBuffer, want)
	}
	if err := tree.Upsert(2, RTreeBounds{MinX: 2, MinY: 2, MaxX: 3, MaxY: 3}); err != nil {
		t.Fatalf("Upsert() replacement error = %v", err)
	}
	got, err = tree.SearchPoint(2, 2)
	if err != nil {
		t.Fatalf("SearchPoint() after replacement error = %v", err)
	}
	if want := []uint64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("SearchPoint() after replacement = %v, want %v", got, want)
	}
	if !tree.Delete(1) || tree.Delete(99) || tree.Len() != 2 {
		t.Fatalf("Delete/Len() state is incorrect: len=%d", tree.Len())
	}
}

func TestRTreeSplitsAndCondenses(t *testing.T) {
	tree, err := NewRTree(4)
	if err != nil {
		t.Fatalf("NewRTree() error = %v", err)
	}
	for id := uint64(1); id <= 96; id++ {
		x := float64((id - 1) % 12)
		y := float64((id - 1) / 12)
		if err := tree.Upsert(id, RTreeBounds{MinX: x, MinY: y, MaxX: x + 0.25, MaxY: y + 0.25}); err != nil {
			t.Fatalf("Upsert(%d) error = %v", id, err)
		}
	}
	if tree.Len() != 96 {
		t.Fatalf("Len() = %d, want 96", tree.Len())
	}
	all, err := tree.Search(RTreeBounds{MinX: -1, MinY: -1, MaxX: 20, MaxY: 20})
	if err != nil {
		t.Fatalf("full Search() error = %v", err)
	}
	if len(all) != 96 {
		t.Fatalf("full Search() returned %d IDs, want 96", len(all))
	}
	for id := uint64(2); id <= 96; id += 2 {
		if !tree.Delete(id) {
			t.Fatalf("Delete(%d) = false, want true", id)
		}
	}
	remaining, err := tree.Search(RTreeBounds{MinX: -1, MinY: -1, MaxX: 20, MaxY: 20})
	if err != nil {
		t.Fatalf("Search() after deletes error = %v", err)
	}
	if len(remaining) != 48 || remaining[0] != 1 || remaining[len(remaining)-1] != 95 {
		t.Fatalf("Search() after deletes = %v, want 48 odd IDs", remaining)
	}
}

func TestRTreeRejectsInvalidBoundsAndCoordinates(t *testing.T) {
	tree, err := NewRTree(4)
	if err != nil {
		t.Fatalf("NewRTree() error = %v", err)
	}
	invalid := []RTreeBounds{
		{MinX: 2, MinY: 0, MaxX: 1, MaxY: 1},
		{MinX: 0, MinY: 2, MaxX: 1, MaxY: 1},
		{MinX: math.NaN(), MinY: 0, MaxX: 1, MaxY: 1},
		{MinX: 0, MinY: 0, MaxX: math.Inf(1), MaxY: 1},
	}
	for _, bounds := range invalid {
		if err := tree.Upsert(1, bounds); err == nil {
			t.Fatalf("Upsert(%+v) unexpectedly succeeded", bounds)
		}
	}
	if _, err := NewRTree(3); err == nil {
		t.Fatal("NewRTree(3) unexpectedly succeeded")
	}
	if _, err := NewRTree(maxRTreeMaxEntries + 1); err == nil {
		t.Fatal("NewRTree(max+1) unexpectedly succeeded")
	}
	if _, err := tree.SearchPoint(0, 0); err != nil {
		t.Fatalf("SearchPoint(0, 0) error = %v", err)
	}
	if _, err := tree.SearchPoint(math.Inf(1), 0); err == nil {
		t.Fatal("SearchPoint(infinity, 0) unexpectedly succeeded")
	}
	if _, err := tree.Search(RTreeBounds{MinX: 1, MinY: 1, MaxX: 0, MaxY: 0}); err == nil {
		t.Fatal("Search() with inverted bounds unexpectedly succeeded")
	}
}

func TestRTreeZeroValueCanBeUsed(t *testing.T) {
	var tree RTree
	if err := tree.Upsert(7, RTreeBounds{MinX: 0, MinY: 0, MaxX: 1, MaxY: 1}); err != nil {
		t.Fatalf("zero-value Upsert() error = %v", err)
	}
	if tree.Len() != 1 {
		t.Fatalf("zero-value Len() = %d, want 1", tree.Len())
	}
}

func TestRTreeSearchUsesInclusiveEdges(t *testing.T) {
	tree := NewDefaultRTree()
	if err := tree.Upsert(1, RTreeBounds{MinX: 0, MinY: 0, MaxX: 1, MaxY: 1}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	got, err := tree.SearchPoint(1, 1)
	if err != nil {
		t.Fatalf("SearchPoint() error = %v", err)
	}
	if want := []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("SearchPoint() = %v, want %v", got, want)
	}
}

func TestRTreeConcurrentAccess(t *testing.T) {
	tree := NewDefaultRTree()
	for id := uint64(0); id < 64; id++ {
		if err := tree.Upsert(id, RTreeBounds{MinX: float64(id), MinY: 0, MaxX: float64(id) + 1, MaxY: 1}); err != nil {
			t.Fatalf("Upsert(%d) error = %v", id, err)
		}
	}
	var wait sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for iteration := 0; iteration < 200; iteration++ {
				if _, err := tree.Search(RTreeBounds{MinX: float64(worker), MinY: -1, MaxX: 64, MaxY: 2}); err != nil {
					t.Errorf("Search() error = %v", err)
					return
				}
			}
		}(worker)
	}
	for worker := 0; worker < 2; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for iteration := 0; iteration < 100; iteration++ {
				id := uint64((iteration + worker) % 64)
				if err := tree.Upsert(id, RTreeBounds{MinX: float64(id), MinY: float64(worker), MaxX: float64(id) + 1, MaxY: float64(worker) + 1}); err != nil {
					t.Errorf("Upsert(%d) error = %v", id, err)
					return
				}
			}
		}(worker)
	}
	wait.Wait()
	if tree.Len() != 64 {
		t.Fatalf("Len() after concurrent access = %d, want 64", tree.Len())
	}
}

func TestRTreeMatchesReferenceAfterMixedMutations(t *testing.T) {
	tree, err := NewRTree(4)
	if err != nil {
		t.Fatalf("NewRTree() error = %v", err)
	}
	reference := make(map[uint64]RTreeBounds)
	for step := 0; step < 300; step++ {
		id := uint64((step*17 + 3) % 47)
		if step%5 == 0 {
			deleted := tree.Delete(id)
			_, existed := reference[id]
			if deleted != existed {
				t.Fatalf("step %d Delete(%d) = %v, reference present = %v", step, id, deleted, existed)
			}
			delete(reference, id)
		} else {
			bounds := RTreeBounds{
				MinX: float64((step*7)%23) - 4,
				MinY: float64((step*11)%19) - 3,
				MaxX: float64((step*7)%23) - 3.25,
				MaxY: float64((step*11)%19) - 2.25,
			}
			if err := tree.Upsert(id, bounds); err != nil {
				t.Fatalf("step %d Upsert(%d) error = %v", step, id, err)
			}
			reference[id] = bounds
		}
		query := RTreeBounds{MinX: -2, MinY: -1, MaxX: 9, MaxY: 8}
		got, err := tree.Search(query)
		if err != nil {
			t.Fatalf("step %d Search() error = %v", step, err)
		}
		var want []uint64
		for candidate, bounds := range reference {
			if bounds.Intersects(query) {
				want = append(want, candidate)
			}
		}
		sortUint64s(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("step %d Search() = %v, want %v", step, got, want)
		}
	}
}

func sortUint64s(values []uint64) {
	for index := 1; index < len(values); index++ {
		value := values[index]
		position := index
		for position > 0 && values[position-1] > value {
			values[position] = values[position-1]
			position--
		}
		values[position] = value
	}
}
