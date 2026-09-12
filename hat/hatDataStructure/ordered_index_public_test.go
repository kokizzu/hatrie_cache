package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestOrderedIndexPublicAPI(t *testing.T) {
	index, err := hatDataStructure.NewOrderedIndex(
		func(value struct{ Key string }) string { return value.Key },
		func(left, right string) int {
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(7, struct{ Key string }{Key: "region"}); err != nil {
		t.Fatal(err)
	}
	iterator, ok := index.First()
	if !ok {
		t.Fatal("First() = false")
	}
	entry, next, err := iterator.Next()
	if err != nil || !next || entry.ID != 7 || entry.Key != "region" {
		t.Fatalf("public iterator = %#v, %v, %v", entry, next, err)
	}
}
