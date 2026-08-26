package hatCache

import (
	"context"
	"testing"
)

func TestSQLResultCacheInvalidatesOnTrieWrite(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.UpsertStringChecked("people", `[{"id":1}]`); err != nil {
		t.Fatal(err)
	}
	cache := NewSQLResultCache(2)
	query := "FROM CACHE('people') AS person SELECT person.id"
	first, err := cache.Execute(context.Background(), trie, query, nil)
	if err != nil || first.Rows[0]["id"] != float64(1) {
		t.Fatalf("first Execute() = %#v, %v", first, err)
	}
	if err := trie.UpsertStringChecked("people", `[{"id":2}]`); err != nil {
		t.Fatal(err)
	}
	second, err := cache.Execute(context.Background(), trie, query, nil)
	if err != nil || second.Rows[0]["id"] != float64(2) {
		t.Fatalf("second Execute() = %#v, %v", second, err)
	}
}
