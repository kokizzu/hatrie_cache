package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLTextIndexPrefixContains(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("articles", `[
  {"id":1,"body":"Fast cache indexes for Go"},
  {"id":2,"body":"A slow database migration"},
  {"id":3,"body":"Go query planning and cache design"}
]`)
	if err := trie.CreateSQLJSONTextIndex("articles", "body"); err != nil {
		t.Fatalf("CreateSQLJSONTextIndex() error = %v", err)
	}
	result, err := ExecuteSQLQuery(`
FROM CACHE('articles') AS article
WHERE CONTAINS_PREFIX(article.body, 'cac')
SELECT article.id
ORDER BY article.id`, trie)
	if err != nil {
		t.Fatalf("prefix query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(1)}, {"id": float64(3)}}) {
		t.Fatalf("prefix query rows = %#v", result.Rows)
	}
}

func TestSQLTextPrefixContainsWithoutIndex(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("articles", `[
  {"id":1,"body":"Fast cache indexes for Go"},
  {"id":2,"body":"A slow database migration"},
  {"id":3,"body":"Go query planning and cache design"}
]`)
	result, err := ExecuteSQLQuery(`
FROM CACHE('articles') AS article
WHERE CONTAINS_PREFIX(article.body, 'dat')
SELECT article.id`, trie)
	if err != nil {
		t.Fatalf("prefix scan query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(2)}}) {
		t.Fatalf("prefix scan query rows = %#v", result.Rows)
	}
}

func TestSQLTextPrefixResolverRefreshesAndNormalizes(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("articles", `[
  {"id":1,"body":"Fast cache indexes for Go"},
  {"id":2,"body":"A slow database migration"},
  {"id":3,"body":"Go query planning and cache design"}
]`)
	if err := trie.CreateSQLJSONTextIndex("articles", "body"); err != nil {
		t.Fatalf("CreateSQLJSONTextIndex() error = %v", err)
	}
	rows, available, err := trie.ResolveSQLTextPrefixSource("CACHE", "articles", "body", " CAC! ")
	if err != nil || !available || !reflect.DeepEqual(rows, []SQLRow{
		{"id": float64(1), "body": "Fast cache indexes for Go"},
		{"id": float64(3), "body": "Go query planning and cache design"},
	}) {
		t.Fatalf("initial prefix rows/availability/error = %#v/%t/%v", rows, available, err)
	}
	rows, available, err = trie.ResolveSQLTextPrefixSource("CACHE", "articles", "body", "cache database")
	if err != nil || !available || len(rows) != 0 {
		t.Fatalf("multi-token prefix rows/availability/error = %#v/%t/%v", rows, available, err)
	}

	trie.UpsertString("articles", `[{"id":4,"body":"Caching is reliable"}]`)
	rows, available, err = trie.ResolveSQLTextPrefixSource("CACHE", "articles", "body", "cach")
	if err != nil || !available || !reflect.DeepEqual(rows, []SQLRow{{"id": float64(4), "body": "Caching is reliable"}}) {
		t.Fatalf("replacement prefix rows/availability/error = %#v/%t/%v", rows, available, err)
	}
}
