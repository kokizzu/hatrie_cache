package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarGenericFilterUsesExecutionArena(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":1,"state":"queued"},
  {"id":2,"state":"running"},
  {"id":3,"state":"queued-later"}
]`)
	query := "FROM CACHE('jobs') AS job WHERE job.state REGEXP '^queued' SELECT job.id"
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
}
