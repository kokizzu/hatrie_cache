package hatCache

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLJSONPartialIndexRestrictsAndRefreshes(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued","active":true},{"id":2,"state":"queued","active":false},{"id":3,"state":"running","active":true}]`)
	if err := trie.CreateSQLJSONPartialIndex("jobs", "state", "active", true); err != nil {
		t.Fatal(err)
	}
	rows, available, err := trie.ResolveSQLCompositeIndexedSource("CACHE", "jobs", []string{"state", "active"}, []interface{}{"queued", true})
	if err != nil || !available || len(rows) != 1 || rows[0]["id"] != float64(1) {
		t.Fatalf("partial rows/available/error = %#v/%t/%v", rows, available, err)
	}
	result, err := hatSql.ExecuteSQLQuery("FROM CACHE('jobs') WHERE state = 'queued' AND active = true SELECT id", trie)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != float64(1) {
		t.Fatalf("partial SQL result/error = %#v/%v", result, err)
	}
	if rows, available, err := trie.ResolveSQLCompositeIndexedSource("CACHE", "jobs", []string{"state", "active"}, []interface{}{"queued", false}); err != nil || available || rows != nil {
		t.Fatalf("unmatched condition rows/available/error = %#v/%t/%v", rows, available, err)
	}
	trie.UpsertString("jobs", `[{"id":4,"state":"queued","active":true}]`)
	rows, available, err = trie.ResolveSQLCompositeIndexedSource("CACHE", "jobs", []string{"state", "active"}, []interface{}{"queued", true})
	if err != nil || !available || len(rows) != 1 || rows[0]["id"] != float64(4) {
		t.Fatalf("refreshed partial rows/available/error = %#v/%t/%v", rows, available, err)
	}
}
