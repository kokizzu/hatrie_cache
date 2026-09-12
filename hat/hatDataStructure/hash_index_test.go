package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

type hashIndexTestRecord struct {
	Email string
	Group string
}

func TestHashIndexUniqueLookupAndAtomicConflict(t *testing.T) {
	index, err := NewHashIndex(
		func(record hashIndexTestRecord) string { return record.Email },
		HashIndexOptions{Unique: true, Capacity: 4},
	)
	if err != nil {
		t.Fatalf("NewHashIndex() error = %v", err)
	}
	if err := index.Upsert(1, hashIndexTestRecord{Email: "one@example.com", Group: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(2, hashIndexTestRecord{Email: "two@example.com", Group: "b"}); err != nil {
		t.Fatal(err)
	}
	if entry, ok := index.LookupOne("one@example.com"); !ok || entry.ID != 1 || entry.Value.Group != "a" {
		t.Fatalf("LookupOne(one@example.com) = %#v, %v", entry, ok)
	}
	if err := index.Upsert(2, hashIndexTestRecord{Email: "one@example.com", Group: "changed"}); !errors.Is(err, ErrHashIndexDuplicateKey) {
		t.Fatalf("conflicting Upsert() error = %v, want duplicate key", err)
	}
	if entry, ok := index.LookupOne("two@example.com"); !ok || entry.ID != 2 || entry.Value.Group != "b" {
		t.Fatalf("conflicting Upsert changed original = %#v, %v", entry, ok)
	}
	if err := index.Upsert(1, hashIndexTestRecord{Email: "one-renamed@example.com", Group: "updated"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := index.LookupOne("one@example.com"); ok {
		t.Fatal("old unique key still found after update")
	}
	if entry, ok := index.LookupOne("one-renamed@example.com"); !ok || entry.ID != 1 || entry.Value.Group != "updated" {
		t.Fatalf("updated LookupOne() = %#v, %v", entry, ok)
	}
	if !index.Contains("one-renamed@example.com") || index.Contains("missing@example.com") {
		t.Fatal("Contains() result is incorrect")
	}
	if got := index.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestHashIndexNonUniquePostingOrderAndUpdates(t *testing.T) {
	index, err := NewHashIndex(
		func(record hashIndexTestRecord) string { return record.Group },
		HashIndexOptions{Capacity: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	for id, record := range map[uint64]hashIndexTestRecord{
		30: {Email: "thirty", Group: "a"},
		10: {Email: "ten", Group: "a"},
		20: {Email: "twenty", Group: "a"},
	} {
		if err := index.Upsert(id, record); err != nil {
			t.Fatal(err)
		}
	}
	if got := index.LookupIDs("a"); !reflect.DeepEqual(got, []uint64{10, 20, 30}) {
		t.Fatalf("LookupIDs(a) = %#v", got)
	}
	if err := index.Upsert(20, hashIndexTestRecord{Email: "twenty", Group: "b"}); err != nil {
		t.Fatal(err)
	}
	if got := index.LookupIDs("a"); !reflect.DeepEqual(got, []uint64{10, 30}) {
		t.Fatalf("LookupIDs(a) after update = %#v", got)
	}
	if got := index.LookupIDs("b"); !reflect.DeepEqual(got, []uint64{20}) {
		t.Fatalf("LookupIDs(b) after update = %#v", got)
	}
	if !index.Delete(10) || index.Delete(10) {
		t.Fatal("Delete() presence result is incorrect")
	}
	if got := index.LookupIDs("a"); !reflect.DeepEqual(got, []uint64{30}) {
		t.Fatalf("LookupIDs(a) after delete = %#v", got)
	}
	if got := index.DistinctKeys(); got != 2 {
		t.Fatalf("DistinctKeys() = %d, want 2", got)
	}
}

func TestHashIndexLookupOneDoesNotAllocate(t *testing.T) {
	index, err := NewHashIndex(func(value int) int { return value }, HashIndexOptions{Unique: true, Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, 1); err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(100, func() {
		if _, ok := index.LookupOne(1); !ok {
			t.Fatal("LookupOne() = false")
		}
	}); got != 0 {
		t.Fatalf("LookupOne() allocation count = %f, want 0", got)
	}
}

func TestHashIndexRejectsMissingConfiguration(t *testing.T) {
	if _, err := NewHashIndex[int, int](nil, HashIndexOptions{}); !errors.Is(err, ErrHashIndexExtractorRequired) {
		t.Fatalf("nil extractor error = %v", err)
	}
	var index *HashIndex[int, int]
	if err := index.Upsert(1, 1); !errors.Is(err, ErrHashIndexNil) {
		t.Fatalf("nil Upsert error = %v", err)
	}
	if _, ok := index.LookupOne(1); ok {
		t.Fatal("nil LookupOne() = true")
	}
}
