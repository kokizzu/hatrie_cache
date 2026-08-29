package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarScanUsesMixedVectorConjunction(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":1,"state":"queued"},
  {"id":2,"state":"running"},
  {"id":3,"state":"queued-later"}
]`)
	query := "FROM CACHE('jobs') AS job WHERE job.state LIKE 'queued%' AND job.id >= 3 SELECT job.id"
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
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatal(err)
	}
	if !sqlColumnarMixedPlanHasNode(explained.Plan, "COLUMNAR VECTOR FILTER") {
		t.Fatalf("plan = %#v, want COLUMNAR VECTOR FILTER", explained.Plan)
	}
}

func sqlColumnarMixedPlanHasNode(plan []SQLExplainStep, node string) bool {
	for _, step := range plan {
		if step.Node == node {
			return true
		}
	}
	return false
}
