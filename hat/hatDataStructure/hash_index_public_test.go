package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestHashIndexPublicAPI(t *testing.T) {
	index, err := hatDataStructure.NewHashIndex(
		func(value string) string { return value },
		hatDataStructure.HashIndexOptions{Unique: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, "value"); err != nil {
		t.Fatal(err)
	}
	entry, ok := index.LookupOne("value")
	if !ok || entry.ID != 1 || entry.Value != "value" {
		t.Fatalf("LookupOne() = %#v, %v", entry, ok)
	}
}
