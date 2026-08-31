package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarDictionaryLiteralINUsesNumericConjunction(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":8,"score":8,"state":"queued"},
  {"id":7,"score":7,"state":"running"},
  {"id":6,"score":6,"state":"done"},
  {"id":5,"score":5,"state":"queued"},
  {"id":4,"score":4,"state":"failed"},
  {"id":3,"score":3,"state":"running"},
  {"id":2,"score":2,"state":"done"},
  {"id":1,"score":1,"state":"failed"}
]`)
	query := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') AND job.score >= 5 SELECT job.id, job.state"
	columnar, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := ExecuteSQLQuery(query, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(columnar, materialized) {
		t.Fatalf("columnar result = %#v, materialized result = %#v", columnar, materialized)
	}
	reversedQuery := "FROM CACHE('jobs') AS job WHERE job.score >= 5 AND job.state IN ('queued', 'running', 'missing') SELECT job.id, job.state"
	reversedColumnar, err := ExecuteSQLQuery(reversedQuery, trie)
	if err != nil {
		t.Fatal(err)
	}
	reversedMaterialized, err := ExecuteSQLQuery(reversedQuery, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reversedColumnar, reversedMaterialized) {
		t.Fatalf("reversed columnar result = %#v, materialized result = %#v", reversedColumnar, reversedMaterialized)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range explained.Plan {
		if step.Node == "COLUMNAR DICTIONARY IN NUMERIC FILTER" {
			return
		}
	}
	t.Fatalf("plan = %#v, want COLUMNAR DICTIONARY IN NUMERIC FILTER", explained.Plan)
}
