package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarDictionaryGroupAggregateUsesDictionaryLiteralIN(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"owner":"alpha","score":8,"state":"queued"},
  {"owner":"bravo","score":7,"state":"running"},
  {"owner":"charlie","score":6,"state":"done"},
  {"owner":"delta","score":5,"state":"queued"},
  {"owner":"alpha","score":4,"state":"failed"},
  {"owner":"bravo","score":3,"state":"running"},
  {"owner":"charlie","score":2,"state":"done"},
  {"owner":"delta","score":1,"state":"failed"}
]`)
	query := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') SELECT job.owner, COUNT(*) AS total, SUM(job.score) AS score_sum GROUP BY job.owner ORDER BY score_sum DESC LIMIT 3"
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
	mixedQuery := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') AND job.score >= 4 SELECT job.owner, COUNT(*) AS total, SUM(job.score) AS score_sum GROUP BY job.owner ORDER BY score_sum DESC LIMIT 3"
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
		if step.Node == "COLUMNAR DICTIONARY GROUP ORDER" {
			return
		}
	}
	t.Fatalf("plan = %#v, want COLUMNAR DICTIONARY GROUP ORDER", explained.Plan)
}
