package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarDictionaryLiteralORUsesCodeFilter(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":8,"state":"queued"},
  {"id":7,"state":"running"},
  {"id":6,"state":"done"},
  {"id":5,"state":"queued"},
  {"id":4,"state":"failed"},
  {"id":3,"state":"running"},
  {"id":2,"state":"done"},
  {"id":1,"state":"failed"}
]`)
	query := "FROM CACHE('jobs') AS job WHERE 'queued' = job.state OR job.state = 'running' OR job.state = 'missing' SELECT job.id, job.state"
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
		if step.Node == "COLUMNAR DICTIONARY OR FILTER" {
			return
		}
	}
	t.Fatalf("plan = %#v, want COLUMNAR DICTIONARY OR FILTER", explained.Plan)
}
