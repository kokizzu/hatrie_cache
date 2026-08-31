package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarScanUsesDictionaryLiteralIN(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":1,"state":"queued"},
  {"id":2,"state":"running"},
  {"id":3,"state":"done"},
  {"id":4,"state":"queued"},
  {"id":5,"state":"failed"},
  {"id":6,"state":"running"},
  {"id":7,"state":"done"},
  {"id":8,"state":"failed"}
]`)
	query := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') SELECT job.id, job.state"
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
	if !sqlColumnarDictionaryINPlanHasNode(explained.Plan, "COLUMNAR DICTIONARY IN FILTER") {
		t.Fatalf("plan = %#v, want COLUMNAR DICTIONARY IN FILTER", explained.Plan)
	}
}

func sqlColumnarDictionaryINPlanHasNode(plan []SQLExplainStep, node string) bool {
	for _, step := range plan {
		if step.Node == node {
			return true
		}
	}
	return false
}
