package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarTopNUsesDictionaryLiteralIN(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":1,"score":8,"state":"queued"},
  {"id":2,"score":7,"state":"running"},
  {"id":3,"score":6,"state":"done"},
  {"id":4,"score":5,"state":"queued"},
  {"id":5,"score":4,"state":"failed"},
  {"id":6,"score":3,"state":"running"},
  {"id":7,"score":2,"state":"done"},
  {"id":8,"score":1,"state":"failed"}
]`)
	fields := []string{"score", "state", "id"}
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
			t.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}
	query := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') SELECT job.id, job.score, job.state ORDER BY job.score DESC LIMIT 3"
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
	mixedQuery := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') AND job.score >= 4 SELECT job.id, job.score, job.state ORDER BY job.score DESC LIMIT 3"
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
	if !sqlColumnarTopNDictionaryINPlanHasNode(explained.Plan, "COLUMNAR TOP-N") {
		t.Fatalf("plan = %#v, want COLUMNAR TOP-N", explained.Plan)
	}
}

func sqlColumnarTopNDictionaryINPlanHasNode(plan []SQLExplainStep, node string) bool {
	for _, step := range plan {
		if step.Node == node {
			return true
		}
	}
	return false
}
