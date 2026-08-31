package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarDictionaryGroupAggregateWithoutOrder(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"state":"running","value":8},{"state":"queued","value":7},{"state":"done","value":6},{"state":"running","value":5},{"state":"failed","value":4},{"state":"queued","value":3},{"state":"done","value":2},{"state":"failed","value":1}]`)
	query := "FROM CACHE('jobs') AS job SELECT job.state, COUNT(*) AS total, SUM(job.value) AS value_sum GROUP BY job.state"
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
	for _, step := range explained.Plan {
		if step.Node == "COLUMNAR DICTIONARY GROUP AGGREGATE" {
			return
		}
	}
	t.Fatalf("plan = %#v, want COLUMNAR DICTIONARY GROUP AGGREGATE", explained.Plan)
}
