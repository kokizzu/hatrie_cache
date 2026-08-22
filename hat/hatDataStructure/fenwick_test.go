package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestFenwickTreeIsUsableByImporters(t *testing.T) {
	tree, err := hatDataStructure.NewFenwickTree(8)
	if err != nil {
		t.Fatalf("NewFenwickTree() error = %v", err)
	}
	if _, ok := tree.Add(2, 5); !ok {
		t.Fatal("Add(2, 5) = false, want true")
	}
	if _, ok := tree.Add(5, -2); !ok {
		t.Fatal("Add(5, -2) = false, want true")
	}
	if got, ok := tree.PrefixSum(5); !ok || got != 3 {
		t.Fatalf("PrefixSum(5) = %d/%v, want 3/true", got, ok)
	}
	if got, ok := tree.RangeSum(2, 5); !ok || got != 3 {
		t.Fatalf("RangeSum(2, 5) = %d/%v, want 3/true", got, ok)
	}
	if info := tree.Info(); info.Size != 8 || info.Updates != 2 || info.Total != 3 {
		t.Fatalf("Info() = %#v, want populated tree", info)
	}
}
