package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestVectorIndexSearchFiltersAndRanksCosineSimilarity(t *testing.T) {
	index, err := hatDataStructure.NewVectorIndex(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert("near", []float32{1, 0}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert("other", []float32{0, 1}); err != nil {
		t.Fatal(err)
	}
	matches, err := index.Search([]float32{1, 0}, 2, func(id string) bool { return id != "near" })
	if err != nil || len(matches) != 1 || matches[0].ID != "other" || matches[0].Score != 0 {
		t.Fatalf("Search() = %#v, %v", matches, err)
	}
}
