package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarDictionaryDistinctUsesDictionaryLiteralIN(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"score":8,"state":"queued"},
  {"score":7,"state":"running"},
  {"score":6,"state":"done"},
  {"score":5,"state":"queued"},
  {"score":4,"state":"failed"},
  {"score":3,"state":"running"},
  {"score":2,"state":"done"},
  {"score":1,"state":"failed"}
]`)
	query := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') SELECT DISTINCT job.state ORDER BY job.state"
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
	mixedQuery := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') AND job.score >= 5 SELECT DISTINCT job.state ORDER BY job.state"
	mixedColumnar, err := ExecuteSQLQuery(mixedQuery, trie)
	if err != nil {
		t.Fatal(err)
	}
	mixedMaterialized, err := ExecuteSQLQuery(mixedQuery, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mixedColumnar, mixedMaterialized) {
		t.Fatalf("mixed columnar result = %#v, materialized result = %#v", mixedColumnar, mixedMaterialized)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range explained.Plan {
		if step.Node == "COLUMNAR DICTIONARY DISTINCT FILTER" {
			return
		}
	}
	t.Fatalf("plan = %#v, want COLUMNAR DICTIONARY DISTINCT FILTER", explained.Plan)
}
