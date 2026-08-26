package hatCache

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestSearchSQLVectorHybridFiltersBeforeRanking(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.UpsertStringChecked("documents", `[{"id":1,"kind":"keep"},{"id":2,"kind":"skip"}]`); err != nil {
		t.Fatal(err)
	}
	index, err := hatDataStructure.NewVectorIndex(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert("1", []float32{0, 1}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert("2", []float32{1, 0}); err != nil {
		t.Fatal(err)
	}
	matches, err := SearchSQLVectorHybrid(context.Background(), "FROM CACHE('documents') AS document WHERE document.kind = 'keep' SELECT document.id, document.kind", trie, nil, SQLQueryOptions{}, index, []float32{1, 0}, 2, "id")
	if err != nil || len(matches) != 1 || matches[0].ID != "1" || matches[0].Row["kind"] != "keep" {
		t.Fatalf("SearchSQLVectorHybrid() = %#v, %v", matches, err)
	}
}
