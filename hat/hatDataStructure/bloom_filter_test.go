package hatDataStructure_test

import (
	"testing"
	"unsafe"

	"hatrie_cache/hat/hatDataStructure"
)

func TestBloomFilterHeaderLayoutIsCompact(t *testing.T) {
	if got := unsafe.Sizeof(hatDataStructure.BloomFilter{}); got != 40 {
		t.Fatalf("BloomFilter size = %d, want 40 bytes", got)
	}
}

func TestBloomFilterPublicCoreRoundTrip(t *testing.T) {
	filter, err := hatDataStructure.NewBloomFilterWithShape(65, 3)
	if err != nil {
		t.Fatalf("NewBloomFilterWithShape() error = %v", err)
	}
	if filter.EncodedSize() != 0 || len(filter.RawWords()) != 0 {
		t.Fatalf("new filter allocated backing: %d bytes, %d words", filter.EncodedSize(), len(filter.RawWords()))
	}
	if !filter.AddBytes([]byte(`{"id":1}`)) || !filter.AddJSONString("fast") {
		t.Fatal("AddBytes/AddJSONString() did not report changed bits")
	}
	if !filter.ContainsBytes([]byte(`{"id":1}`)) || !filter.ContainsJSONString("fast") {
		t.Fatal("filter does not contain inserted value")
	}

	snapshot := filter.Snapshot()
	restored, err := hatDataStructure.NewBloomFilterFromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("NewBloomFilterFromSnapshot() error = %v", err)
	}
	if restored.Info() != filter.Info() || !restored.ContainsBytes([]byte(`{"id":1}`)) || !restored.ContainsJSONString("fast") {
		t.Fatal("snapshot round trip changed bloom filter state")
	}

	bad := snapshot
	bad.Bits = "AQAAAAAAAAAAAAAAAA=="
	if err := hatDataStructure.ValidateBloomFilterSnapshot(bad); err == nil {
		t.Fatal("ValidateBloomFilterSnapshot() accepted invalid bitset length")
	}
}
