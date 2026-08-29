package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarScanUsesNumericVectorConjunction(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("metrics", `[
  {"id":1,"value":10},
  {"id":2,"value":20},
  {"id":3,"value":30},
  {"id":4,"value":40}
]`)
	query := "FROM CACHE('metrics') AS metric WHERE metric.value >= 20 AND metric.id < 4 SELECT metric.id"
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
		if step.Node == "COLUMNAR NUMERIC FILTER" {
			return
		}
	}
	t.Fatalf("plan = %#v, want COLUMNAR NUMERIC FILTER", explained.Plan)
}
