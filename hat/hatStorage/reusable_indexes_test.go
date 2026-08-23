package hatStorage_test

import (
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestReusableIndexesAreUsableByImporters(t *testing.T) {
	var indexes hatStorage.ReusableIndexes
	if !indexes.Mark(3) || indexes.Mark(3) {
		t.Fatal("Mark() did not deduplicate index")
	}
	if !indexes.Has(3) || indexes.Len() != 1 {
		t.Fatal("marked index is not tracked")
	}
	if got, ok := indexes.Take(); !ok || got != 3 {
		t.Fatalf("Take() = %d/%v, want 3/true", got, ok)
	}
	if indexes.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", indexes.Len())
	}
}
