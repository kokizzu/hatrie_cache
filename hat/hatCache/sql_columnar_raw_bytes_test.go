package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLColumnarRawBytesBatchUsesLockedRawStorage(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	data := []byte(`[
  {"id":1,"state":"queued","payload":"unselected"},
  {"id":2,"state":"running","payload":"unselected"}
]`)
	trie.UpsertBytes("jobs", data)

	want, err := sqlJSONColumnarBatch("jobs", data, []string{"state", "id"})
	if err != nil {
		t.Fatal(err)
	}
	got, handled, err := trie.sqlColumnarRawBytesBatch("jobs", []string{"state", "id"})
	if err != nil || !handled {
		t.Fatalf("sqlColumnarRawBytesBatch() = %#v, %v, %v", got, handled, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("raw columnar batch = %#v, want %#v", got, want)
	}
	resolved, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", []string{"state", "id"})
	if err != nil || !available {
		t.Fatalf("ResolveSQLColumnarSource() = %#v, %v, %v", resolved, available, err)
	}
	if !reflect.DeepEqual(resolved, want) {
		t.Fatalf("resolved columnar batch = %#v, want %#v", resolved, want)
	}
}
