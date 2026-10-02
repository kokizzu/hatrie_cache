package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

type tu23Record struct {
	ID   uint64
	Tags []string
	Name string
}

func tu23Tags(record tu23Record) []string {
	return record.Tags
}

func TestTU23MultiKeyIndexDeduplicatesAndSortsPostings(t *testing.T) {
	index, err := NewMultiKeyIndex(tu23Tags, MultiKeyIndexOptions{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	first := tu23Record{ID: 1, Tags: []string{"go", "go", "db"}, Name: "first"}
	second := tu23Record{ID: 2, Tags: []string{"sql", "db"}, Name: "second"}
	if err := index.Upsert(first.ID, first); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(second.ID, second); err != nil {
		t.Fatal(err)
	}
	if got := index.LookupIDs("go"); !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("LookupIDs(go) = %#v, want [1]", got)
	}
	if got := index.LookupIDs("db"); !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("LookupIDs(db) = %#v, want [1 2]", got)
	}
	if got := index.Lookup("sql"); len(got) != 1 || got[0].Name != second.Name {
		t.Fatalf("Lookup(sql) = %#v, want second record", got)
	}
	if got := index.DistinctKeys(); got != 3 {
		t.Fatalf("DistinctKeys() = %d, want 3", got)
	}
	if got := index.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestTU23MultiKeyIndexUpdateAndDelete(t *testing.T) {
	index, err := NewMultiKeyIndex(tu23Tags, MultiKeyIndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, tu23Record{ID: 1, Tags: []string{"old", "shared"}}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(2, tu23Record{ID: 2, Tags: []string{"shared"}}); err != nil {
		t.Fatal(err)
	}
	updated := tu23Record{ID: 1, Tags: []string{"new", "shared"}, Name: "updated"}
	if err := index.Upsert(updated.ID, updated); err != nil {
		t.Fatal(err)
	}
	if got := index.LookupIDs("old"); len(got) != 0 {
		t.Fatalf("old posting = %#v, want empty", got)
	}
	if got := index.LookupIDs("shared"); !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("shared posting = %#v, want [1 2]", got)
	}
	if !index.Delete(1) || index.Delete(1) {
		t.Fatal("Delete() presence result is incorrect")
	}
	if got := index.LookupIDs("shared"); !reflect.DeepEqual(got, []uint64{2}) {
		t.Fatalf("shared posting after delete = %#v, want [2]", got)
	}
	if got := index.Len(); got != 1 {
		t.Fatalf("Len() after delete = %d, want 1", got)
	}
}

func TestTU23MultiKeyIndexRejectsOversizedUpdatesAtomically(t *testing.T) {
	index, err := NewMultiKeyIndex(tu23Tags, MultiKeyIndexOptions{MaxKeysPerEntry: 2})
	if err != nil {
		t.Fatal(err)
	}
	initial := tu23Record{ID: 1, Tags: []string{"a", "b"}}
	if err := index.Upsert(initial.ID, initial); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, tu23Record{ID: 1, Tags: []string{"a", "b", "c"}}); !errors.Is(err, ErrMultiKeyIndexLimit) {
		t.Fatalf("oversized update error = %v, want ErrMultiKeyIndexLimit", err)
	}
	if got := index.LookupIDs("a"); !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("a posting after rejected update = %#v, want [1]", got)
	}
	if got := index.LookupIDs("c"); len(got) != 0 {
		t.Fatalf("c posting after rejected update = %#v, want empty", got)
	}
	if got := index.Len(); got != 1 {
		t.Fatalf("Len() after rejected update = %d, want 1", got)
	}
}

func TestTU23MultiKeyIndexAllowsEmptyKeysAndRejectsInvalidConfiguration(t *testing.T) {
	index, err := NewMultiKeyIndex(tu23Tags, MultiKeyIndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, tu23Record{ID: 1, Tags: nil}); err != nil {
		t.Fatal(err)
	}
	if got := index.Len(); got != 1 {
		t.Fatalf("Len() for unkeyed record = %d, want 1", got)
	}
	if got := index.LookupIDs("missing"); len(got) != 0 {
		t.Fatalf("missing posting = %#v, want empty", got)
	}
	if _, err := NewMultiKeyIndex[tu23Record, string](nil, MultiKeyIndexOptions{}); !errors.Is(err, ErrMultiKeyIndexExtractorRequired) {
		t.Fatalf("nil extractor error = %v", err)
	}
	if _, err := NewMultiKeyIndex(tu23Tags, MultiKeyIndexOptions{MaxKeysPerEntry: -1}); !errors.Is(err, ErrMultiKeyIndexLimit) {
		t.Fatalf("invalid max keys error = %v", err)
	}
	var nilIndex *MultiKeyIndex[tu23Record, string]
	if err := nilIndex.Upsert(1, tu23Record{}); !errors.Is(err, ErrMultiKeyIndexNil) {
		t.Fatalf("nil Upsert error = %v", err)
	}
}
