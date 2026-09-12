package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTupleFieldUpdatesPublicAPI(t *testing.T) {
	cache, err := hatDataStructure.NewPackedTuple([][]byte{[]byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := cache.ApplyUpdates([]hatDataStructure.TupleFieldUpdate{{
		Index: 0,
		Kind:  hatDataStructure.TupleFieldSet,
		Value: []byte("changed"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	field, err := updated.Field(0)
	if err != nil || string(field) != "changed" {
		t.Fatalf("updated Field(0) = (%q, %v)", field, err)
	}
}
