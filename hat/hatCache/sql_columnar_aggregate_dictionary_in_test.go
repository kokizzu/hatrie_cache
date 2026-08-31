package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarNumericAggregateUsesDictionaryLiteralIN(t *testing.T) {
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
	query := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') SELECT COUNT(*) AS total, SUM(job.score) AS score_sum"
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
	mixedQuery := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') AND job.score >= 4 SELECT COUNT(*) AS total, SUM(job.score) AS score_sum"
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
	if !sqlColumnarAggregateDictionaryINPlanHasNode(explained.Plan, "COLUMNAR DICTIONARY NUMERIC AGGREGATE") {
		t.Fatalf("plan = %#v, want COLUMNAR DICTIONARY NUMERIC AGGREGATE", explained.Plan)
	}
}

func sqlColumnarAggregateDictionaryINPlanHasNode(plan []SQLExplainStep, node string) bool {
	for _, step := range plan {
		if step.Node == node {
			return true
		}
	}
	return false
}
