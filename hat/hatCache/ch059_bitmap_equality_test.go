package hatCache

import (
	"reflect"
	"testing"
)

func TestCH059BitmapEqualityMatchesBatchedBitmapCandidates(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":1,"state":"ready"},
  {"id":2,"state":"queued"},
  {"id":3,"state":"ready"},
  {"id":4,"state":"done"}
]`)
	if err := trie.CreateSQLJSONBitmapIndex("jobs", "state"); err != nil {
		t.Fatalf("CreateSQLJSONBitmapIndex() error = %v", err)
	}
	equality, equalityAvailable, err := trie.ResolveSQLIndexedSource("CACHE", "jobs", "state", "ready")
	if err != nil || !equalityAvailable {
		t.Fatalf("ResolveSQLIndexedSource() = %#v, %v, %v", equality, equalityAvailable, err)
	}
	batched, batchAvailable, err := trie.ResolveSQLIndexedValues("CACHE", "jobs", "state", []interface{}{"ready"})
	if err != nil || !batchAvailable {
		t.Fatalf("ResolveSQLIndexedValues() = %#v, %v, %v", batched, batchAvailable, err)
	}
	if !reflect.DeepEqual(equality, batched) {
		t.Fatalf("equality rows = %#v, batch rows = %#v", equality, batched)
	}
}
