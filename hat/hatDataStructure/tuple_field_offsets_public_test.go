package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTupleFieldOffsetCachePublicAPI(t *testing.T) {
	cache, err := hatDataStructure.NewPackedTuple([][]byte{[]byte("region"), []byte("42")})
	if err != nil {
		t.Fatalf("NewPackedTuple() error = %v", err)
	}
	field, err := cache.Field(1)
	if err != nil || string(field) != "42" {
		t.Fatalf("Field(1) = (%q, %v), want (42, nil)", field, err)
	}
	start, end, err := cache.Offset(0)
	if err != nil || end-start != 6 {
		t.Fatalf("Offset(0) = (%d, %d, %v), want width 6 and nil", start, end, err)
	}
}
