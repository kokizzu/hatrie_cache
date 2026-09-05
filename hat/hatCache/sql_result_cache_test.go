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

func TestSQLResultCacheSeparatesParameterizedBindings(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.UpsertStringChecked("people", `[{"id":"ada","name":"Ada"},{"id":"lin","name":"Lin"}]`); err != nil {
		t.Fatal(err)
	}
	cache := NewSQLResultCache(2)
	query := "FROM CACHE('people') AS person WHERE person.id = $1 SELECT person.name"

	first, err := cache.Execute(context.Background(), trie, query, []interface{}{"ada"})
	if err != nil || len(first.Rows) != 1 || first.Rows[0]["name"] != "Ada" {
		t.Fatalf("first parameterized Execute() = %#v, %v", first, err)
	}
	first.Rows[0]["name"] = "changed"

	sameBinding, err := cache.Execute(context.Background(), trie, query, []interface{}{"ada"})
	if err != nil || len(sameBinding.Rows) != 1 || sameBinding.Rows[0]["name"] != "Ada" {
		t.Fatalf("same parameterized binding = %#v, %v", sameBinding, err)
	}

	differentBinding, err := cache.Execute(context.Background(), trie, query, []interface{}{"lin"})
	if err != nil || len(differentBinding.Rows) != 1 || differentBinding.Rows[0]["name"] != "Lin" {
		t.Fatalf("different parameterized binding = %#v, %v", differentBinding, err)
	}
}
