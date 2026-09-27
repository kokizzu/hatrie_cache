package hatCache

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLTextPhraseIndexUnionAcrossFields(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("articles", `[
  {"id":1,"kind":"a","title":"quick brown fox","body":"unrelated"},
  {"id":2,"kind":"b","title":"unrelated","body":"lazy red fox"},
  {"id":3,"kind":"a","title":"quick brown fox","body":"lazy fox"},
  {"id":4,"kind":"other","title":"unrelated","body":"unrelated"}
]`)
	for _, field := range []string{"title", "body"} {
		if err := trie.CreateSQLJSONTextIndex("articles", field); err != nil {
			t.Fatalf("CreateSQLJSONTextIndex(%q) error = %v", field, err)
		}
	}

	queries := []hatSql.SQLTextProximityFieldQuery{
		{Field: "title", Query: hatSql.SQLTextProximityQuery{Query: "quick brown"}},
		{Field: "body", Query: hatSql.SQLTextProximityQuery{Query: "lazy fox", MaxGap: 1, Proximity: true}},
	}
	rows, available, err := trie.ResolveSQLTextProximityMultiFieldUnionSource("CACHE", "articles", queries)
	if err != nil || !available {
		t.Fatalf("ResolveSQLTextProximityMultiFieldUnionSource() availability/error = %t/%v", available, err)
	}
	if want := []SQLRow{
		{"id": float64(1), "kind": "a", "title": "quick brown fox", "body": "unrelated"},
		{"id": float64(2), "kind": "b", "title": "unrelated", "body": "lazy red fox"},
		{"id": float64(3), "kind": "a", "title": "quick brown fox", "body": "lazy fox"},
	}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("cross-field union rows = %#v, want %#v", rows, want)
	}

	result, err := ExecuteSQLQuery(`
FROM CACHE('articles') AS article
WHERE CONTAINS_PHRASE(article.title, 'quick brown')
   OR CONTAINS_PROXIMITY(article.body, 'lazy fox', 1)
SELECT article.id
ORDER BY article.id`, trie)
	if err != nil {
		t.Fatalf("cross-field union query error = %v", err)
	}
	if want := []SQLRow{{"id": float64(1)}, {"id": float64(2)}, {"id": float64(3)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("cross-field union query rows = %#v, want %#v", result.Rows, want)
	}

	result, err = ExecuteSQLQuery(`
FROM CACHE('articles') AS article
WHERE (CONTAINS_PHRASE(article.title, 'quick brown') AND article.kind = 'a')
   OR (CONTAINS_PROXIMITY(article.body, 'lazy fox', 1) AND article.kind = 'b')
SELECT article.id
ORDER BY article.id`, trie)
	if err != nil {
		t.Fatalf("mixed cross-field union query error = %v", err)
	}
	if want := []SQLRow{{"id": float64(1)}, {"id": float64(2)}, {"id": float64(3)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("mixed cross-field union query rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLTextPhraseIndexUnionAcrossFieldsRejectsInvalidOrUnavailable(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("articles", `[{"id":1,"title":"quick brown"}]`)
	if err := trie.CreateSQLJSONTextIndex("articles", "title"); err != nil {
		t.Fatalf("CreateSQLJSONTextIndex() error = %v", err)
	}
	_, available, err := trie.ResolveSQLTextProximityMultiFieldUnionSource("CACHE", "articles", []hatSql.SQLTextProximityFieldQuery{
		{Field: "title", Query: hatSql.SQLTextProximityQuery{Query: "quick brown", MaxGap: -1}},
	})
	if err == nil || available {
		t.Fatalf("invalid gap availability/error = %t/%v, want false/error", available, err)
	}
	_, available, err = trie.ResolveSQLTextProximityMultiFieldUnionSource("CACHE", "articles", []hatSql.SQLTextProximityFieldQuery{
		{Field: "body", Query: hatSql.SQLTextProximityQuery{Query: "quick brown"}},
	})
	if err != nil || available {
		t.Fatalf("missing index availability/error = %t/%v, want false/nil", available, err)
	}
}
