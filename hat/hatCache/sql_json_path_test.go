package hatCache

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLJSONPathIndexAcceleratesNestedEquality(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"profile":{"city":"Singapore"}},{"id":2,"profile":{"city":"Jakarta"}}]`)
	if err := trie.CreateSQLJSONPathIndex("people", "$.profile.city"); err != nil {
		t.Fatalf("CreateSQLJSONPathIndex() error = %v", err)
	}
	query := "FROM CACHE('people') AS p WHERE JSON_VALUE(p.profile, '$.city') = 'Singapore' SELECT p.id"
	result, err := hatSql.ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != float64(1) {
		t.Fatalf("result.Rows = %#v, want id 1", result.Rows)
	}
	explained, err := hatSql.ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(EXPLAIN ANALYZE) error = %v", err)
	}
	for _, step := range explained.Plan {
		if step.Node == "INDEX SCAN" {
			return
		}
	}
	t.Fatalf("plan = %#v, want INDEX SCAN", explained.Plan)
}
