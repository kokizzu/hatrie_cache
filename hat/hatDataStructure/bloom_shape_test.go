package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestBloomFilterShapeIsUsableByImporters(t *testing.T) {
	bits, hashes, err := hatDataStructure.BloomFilterShape(10_000, 0.01)
	if err != nil {
		t.Fatalf("BloomFilterShape() error = %v", err)
	}
	if bits < 64 || hashes == 0 {
		t.Fatalf("BloomFilterShape() = %d/%d, want usable shape", bits, hashes)
	}
	if _, _, err := hatDataStructure.BloomFilterShape(0, 0.01); err == nil {
		t.Fatal("BloomFilterShape(0, 0.01) error = nil")
	}
}
