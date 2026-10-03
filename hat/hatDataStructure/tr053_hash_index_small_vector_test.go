package hatDataStructure

import (
	"strconv"
	"testing"
)

type tr053HashTestValue struct {
	Key   string
	Value int
}

func TestTR053HashIndexUniqueSmallVectorPromotionAndDemotion(t *testing.T) {
	index, err := NewHashIndex(func(value tr053HashTestValue) string { return value.Key }, HashIndexOptions{Unique: true, Capacity: 8})
	if err != nil {
		t.Fatalf("NewHashIndex() error = %v", err)
	}
	if index.entries != nil || index.uniqueByKey != nil {
		t.Fatal("small unique index allocated hash maps before its first entry")
	}
	for id := 0; id < 32; id++ {
		if err := index.Upsert(uint64(id), tr053HashTestValue{Key: "key-" + strconv.Itoa(id), Value: id}); err != nil {
			t.Fatalf("Upsert(%d) error = %v", id, err)
		}
	}
	if index.entries != nil || index.uniqueByKey != nil {
		t.Fatal("small unique index allocated hash maps")
	}
	if err := index.Upsert(7, tr053HashTestValue{Key: "replacement", Value: 700}); err != nil {
		t.Fatalf("same-ID key replacement error = %v", err)
	}
	if got, ok := index.LookupOne("replacement"); !ok || got.ID != 7 || got.Value.Value != 700 {
		t.Fatalf("replacement lookup = %#v/%v, want ID 7 and value 700", got, ok)
	}
	if _, ok := index.LookupOne("key"); ok {
		t.Fatal("old key remained after same-ID replacement")
	}
	for id := 32; id < 65; id++ {
		if err := index.Upsert(uint64(id), tr053HashTestValue{Key: "key-" + strconv.Itoa(id), Value: id}); err != nil {
			t.Fatalf("promotion Upsert(%d) error = %v", id, err)
		}
	}
	if index.entries == nil || index.uniqueByKey == nil {
		t.Fatal("large unique index did not promote to hash maps")
	}
	if err := index.Upsert(65, tr053HashTestValue{Key: "key-0", Value: 65}); err == nil {
		t.Fatal("duplicate unique key was accepted after promotion")
	}
	for id := 64; id >= 32; id-- {
		if !index.Delete(uint64(id)) {
			t.Fatalf("Delete(%d) reported missing promoted entry", id)
		}
	}
	if index.entries != nil || index.uniqueByKey != nil {
		t.Fatal("small unique index retained hash maps after demotion")
	}
	if index.Len() != 32 {
		t.Fatalf("Len() = %d, want 32 after demotion", index.Len())
	}
	if got, ok := index.LookupOne("replacement"); !ok || got.ID != 7 || got.Value.Value != 700 {
		t.Fatalf("demoted replacement lookup = %#v/%v", got, ok)
	}
}
