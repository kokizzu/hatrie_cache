package hatDataStructure

import (
	"reflect"
	"testing"
)

func TestVectorIndexSearchIntoMatchesSearchAndReusesDestination(t *testing.T) {
	index, err := NewVectorIndex(2)
	if err != nil {
		t.Fatal(err)
	}
	for id, values := range map[string][]float32{
		"near":  {1, 0},
		"other": {0, 1},
		"tie-b": {1, 0},
		"tie-a": {1, 0},
	} {
		if err := index.Upsert(id, values); err != nil {
			t.Fatal(err)
		}
	}

	destination := make([]VectorMatch, 0, 3)
	got, err := index.SearchInto(destination, []float32{1, 0}, 3, func(id string) bool {
		return id != "other"
	})
	if err != nil {
		t.Fatal(err)
	}
	want, err := index.Search([]float32{1, 0}, 3, func(id string) bool {
		return id != "other"
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SearchInto() = %#v, Search() = %#v", got, want)
	}
	if len(got) > 0 && &got[0] != &destination[:1][0] {
		t.Fatal("SearchInto() did not reuse destination backing storage")
	}

	tied, err := index.SearchInto(got[:0], []float32{1, 0}, 1, nil)
	if err != nil || len(tied) != 1 || tied[0].ID != "near" {
		t.Fatalf("tie-broken SearchInto() = %#v, %v, want near", tied, err)
	}
	if _, err := index.SearchInto(got[:0], []float32{1, 0}, -1, nil); err == nil {
		t.Fatal("negative limit was accepted")
	}

	full, err := index.Search([]float32{1, 0}, 4, nil)
	if err != nil || len(full) != 4 || full[0].ID != "near" {
		t.Fatalf("Search() after SearchInto() = %#v, %v", full, err)
	}
}

func TestVectorIndexSearchIntoZeroLimitSkipsInvalidQuery(t *testing.T) {
	index, err := NewVectorIndex(2)
	if err != nil {
		t.Fatal(err)
	}
	destination := make([]VectorMatch, 0, 1)
	got, err := index.SearchInto(destination, []float32{0}, 0, nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("zero-limit SearchInto() = %#v, %v", got, err)
	}
}
