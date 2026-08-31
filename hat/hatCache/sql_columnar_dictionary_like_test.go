package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarDictionaryLikeUsesCodeFilter(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":8,"state":"queued"},
  {"id":7,"state":"queue-retry"},
  {"id":6,"state":"running"},
  {"id":5,"state":"done"},
  {"id":4,"state":"queued"},
  {"id":3,"state":"queue-retry"},
  {"id":2,"state":"running"},
  {"id":1,"state":"done"}
]`)
	query := "FROM CACHE('jobs') AS job WHERE job.state LIKE 'queue%' SELECT job.id, job.state"
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
		if step.Node == "COLUMNAR DICTIONARY LIKE FILTER" {
			return
		}
	}
	t.Fatalf("plan = %#v, want COLUMNAR DICTIONARY LIKE FILTER", explained.Plan)
}
