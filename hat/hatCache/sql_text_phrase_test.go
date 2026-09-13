package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLTextPhraseIndexMatchesAndRefreshes(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("articles", `[
  {"id":1,"body":"Fast cache indexes for Go"},
  {"id":2,"body":"Fast database cache indexes"},
  {"id":3,"body":"Go query planning and cache design"}
]`)
	if err := trie.CreateSQLJSONTextIndex("articles", "body"); err != nil {
		t.Fatalf("CreateSQLJSONTextIndex() error = %v", err)
	}

	result, err := ExecuteSQLQuery(`
FROM CACHE('articles') AS article
WHERE CONTAINS_PHRASE(article.body, 'fast cache')
SELECT article.id
ORDER BY article.id`, trie)
	if err != nil {
		t.Fatalf("phrase query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(1)}}) {
		t.Fatalf("phrase query rows = %#v", result.Rows)
	}

	result, err = ExecuteSQLQuery(`
FROM CACHE('articles') AS article
WHERE CONTAINS_PROXIMITY(article.body, 'fast indexes', 2)
SELECT article.id
ORDER BY article.id`, trie)
	if err != nil {
		t.Fatalf("proximity query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(1)}, {"id": float64(2)}}) {
		t.Fatalf("proximity query rows = %#v", result.Rows)
	}

	trie.UpsertString("articles", `[{"id":4,"body":"Fast cache replacement"}]`)
	result, err = ExecuteSQLQuery(`
FROM CACHE('articles') AS article
WHERE CONTAINS_PHRASE(article.body, 'fast cache')
SELECT article.id`, trie)
	if err != nil {
		t.Fatalf("refreshed phrase query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(4)}}) {
		t.Fatalf("refreshed phrase query rows = %#v", result.Rows)
	}
}

func TestSQLTextPhraseContainsWithoutIndex(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("articles", `[
  {"id":1,"body":"Fast cache indexes for Go"},
  {"id":2,"body":"Fast database cache indexes"},
  {"id":3,"body":"Go query planning and cache design"}
]`)
	result, err := ExecuteSQLQuery(`
FROM CACHE('articles') AS article
WHERE CONTAINS_PHRASE(article.body, 'fast cache')
SELECT article.id`, trie)
	if err != nil {
		t.Fatalf("phrase scan query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(1)}}) {
		t.Fatalf("phrase scan query rows = %#v", result.Rows)
	}
}

func TestSQLTextPhraseResolverPreservesSourceOrder(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("articles", `[
  {"id":1,"body":"alpha beta gamma"},
  {"id":2,"body":"alpha red beta gamma"},
  {"id":3,"body":"gamma beta alpha"}
]`)
	if err := trie.CreateSQLJSONTextIndex("articles", "body"); err != nil {
		t.Fatalf("CreateSQLJSONTextIndex() error = %v", err)
	}
	rows, available, err := trie.ResolveSQLTextProximitySource("CACHE", "articles", "body", "alpha gamma", 2)
	if err != nil || !available {
		t.Fatalf("ResolveSQLTextProximitySource() availability/error = %t/%v", available, err)
	}
	if !reflect.DeepEqual(rows, []SQLRow{
		{"id": float64(1), "body": "alpha beta gamma"},
		{"id": float64(2), "body": "alpha red beta gamma"},
	}) {
		t.Fatalf("proximity resolver rows = %#v", rows)
	}
}
