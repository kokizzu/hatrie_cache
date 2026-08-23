package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestCuckooFilterShapeIsUsableByImporters(t *testing.T) {
	buckets, fingerprintBits, err := hatDataStructure.CuckooFilterShape(10_000, 0.01)
	if err != nil {
		t.Fatalf("CuckooFilterShape() error = %v", err)
	}
	if buckets != 4096 || fingerprintBits != 10 {
		t.Fatalf("CuckooFilterShape() = %d/%d, want 4096/10", buckets, fingerprintBits)
	}
	if _, _, err := hatDataStructure.CuckooFilterShape(0, 0.01); err == nil {
		t.Fatal("CuckooFilterShape() accepted zero capacity")
	}
}
