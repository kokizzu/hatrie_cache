package hatCache

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLTextPhraseIndexCrossFieldUnion(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("articles", `[
  {"id":1,"title":"quick brown release","body":"unrelated"},
  {"id":2,"title":"unrelated","body":"lazy red fox"},
  {"id":3,"title":"quick release","body":"unrelated"},
  {"id":4,"title":"quick brown release","body":"lazy fox"}
]`)
	if err := trie.CreateSQLJSONTextIndex("articles", "title"); err != nil {
		t.Fatal(err)
	}
	if err := trie.CreateSQLJSONTextIndex("articles", "body"); err != nil {
		t.Fatal(err)
	}
	rows, available, err := trie.ResolveSQLTextProximityMultiFieldUnionSource("CACHE", "articles", []hatSql.SQLTextProximityFieldQuery{
		{Field: "title", Query: hatSql.SQLTextProximityQuery{Query: "quick brown"}},
		{Field: "body", Query: hatSql.SQLTextProximityQuery{Query: "lazy fox", MaxGap: 1, Proximity: true}},
	})
	if err != nil || !available {
		t.Fatalf("multi-field resolver availability/error = %t/%v", available, err)
	}
	if !reflect.DeepEqual(rows, []SQLRow{
		{"id": float64(1), "title": "quick brown release", "body": "unrelated"},
		{"id": float64(2), "title": "unrelated", "body": "lazy red fox"},
		{"id": float64(4), "title": "quick brown release", "body": "lazy fox"},
	}) {
		t.Fatalf("multi-field rows = %#v", rows)
	}
	result, err := ExecuteSQLQuery(`
FROM CACHE('articles') AS article
WHERE CONTAINS_PHRASE(article.title, 'quick brown') OR CONTAINS_PROXIMITY(article.body, 'lazy fox', 1)
SELECT article.id
ORDER BY article.id`, trie)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(1)}, {"id": float64(2)}, {"id": float64(4)}}) {
		t.Fatalf("cross-field query rows = %#v", result.Rows)
	}
}

func TestSQLTextPhraseIndexCrossFieldUnionRequiresEverySidecar(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("articles", `[{"id":1,"title":"quick brown","body":"lazy fox"}]`)
	if err := trie.CreateSQLJSONTextIndex("articles", "title"); err != nil {
		t.Fatal(err)
	}
	rows, available, err := trie.ResolveSQLTextProximityMultiFieldUnionSource("CACHE", "articles", []hatSql.SQLTextProximityFieldQuery{
		{Field: "title", Query: hatSql.SQLTextProximityQuery{Query: "quick brown"}},
		{Field: "body", Query: hatSql.SQLTextProximityQuery{Query: "lazy fox"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if available || rows != nil {
		t.Fatalf("partial multi-field index = %#v/%t, want unavailable", rows, available)
	}
}
