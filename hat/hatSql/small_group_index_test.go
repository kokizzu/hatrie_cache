package hatSql

import (
	"fmt"
	"testing"
)

func TestSQLHashGroupAggregateIndexesPromotesSmallEntries(t *testing.T) {
	indexes := newSQLHashGroupAggregateIndexes()
	keys := []string{"text:zero", "int64:1", "text:two", "int64:3"}
	for index, key := range keys {
		foundIndex, exists := indexes.find(key)
		if exists {
			t.Fatalf("key %q found before insertion at index %d", key, index)
		}
		if foundIndex != 0 {
			t.Fatalf("key %q returned unexpected index %d before insertion", key, foundIndex)
		}
		if indexes.large != nil {
			t.Fatalf("key %q allocated the map before capacity was reached", key)
		}
		indexes.add(key, index)
	}
	for index, key := range keys {
		foundIndex, exists := indexes.find(key)
		if !exists || foundIndex != index {
			t.Fatalf("key %q lookup = %d, %v; want inserted index %d", key, foundIndex, exists, index)
		}
	}
}

func TestSQLHashGroupAggregateIndexesPromotesAfterCapacity(t *testing.T) {
	indexes := newSQLHashGroupAggregateIndexes()
	for index := 0; index < sqlHashGroupAggregateSmallIndexCapacity+1; index++ {
		key := fmt.Sprintf("int64:%d", index)
		found, exists := indexes.find(key)
		if exists {
			t.Fatalf("key %q found before insertion", key)
		}
		if found != 0 {
			t.Fatalf("key %q returned unexpected index %d before insertion", key, found)
		}
		indexes.add(key, index)
	}
	if indexes.large == nil {
		t.Fatal("map was not created after small-index promotion")
	}
	if indexes.smallLen != 0 {
		t.Fatalf("small index retained %d entries after promotion", indexes.smallLen)
	}
	for index := 0; index < sqlHashGroupAggregateSmallIndexCapacity+1; index++ {
		key := fmt.Sprintf("int64:%d", index)
		found, exists := indexes.find(key)
		if !exists || found != index {
			t.Fatalf("promoted key %q lookup = %d, %v", key, found, exists)
		}
	}
}
