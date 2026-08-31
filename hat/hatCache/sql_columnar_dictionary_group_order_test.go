package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarDictionaryGroupAggregateOrdersByProjectedAggregate(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"state":"queued","value":2},
  {"state":"queued","value":3},
  {"state":"queued","value":5},
  {"state":"running","value":13},
  {"state":"running","value":17},
  {"state":"done","value":19},
  {"state":"done","value":23},
  {"state":"done","value":29},
  {"state":"done","value":31}
]`)
	query := "FROM CACHE('jobs') AS job SELECT job.state, COUNT(*) AS count, SUM(job.value) AS total, AVG(job.value) AS average GROUP BY job.state ORDER BY total DESC LIMIT 2"
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
		if step.Node == "COLUMNAR DICTIONARY GROUP ORDER" {
			return
		}
	}
	t.Fatalf("plan = %#v, want COLUMNAR DICTIONARY GROUP ORDER", explained.Plan)
}
