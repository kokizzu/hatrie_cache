package hatSql

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestSQLSpillBloomFiltersRejectOnlyDisjointPartitions(t *testing.T) {
	left, err := hatDataStructure.NewBloomFilter(64, 0.001)
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatDataStructure.NewBloomFilter(64, 0.001)
	if err != nil {
		t.Fatal(err)
	}
	left.AddJSONString("number:1")
	right.AddJSONString("number:2")
	if sqlBloomFiltersMayIntersect(left, right) {
		t.Fatal("disjoint Bloom filters may intersect")
	}
	right.AddJSONString("number:1")
	if !sqlBloomFiltersMayIntersect(left, right) {
		t.Fatal("matching Bloom filters do not intersect")
	}
}
