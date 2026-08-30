package hatCache

import (
	"testing"
	"unsafe"
)

func TestSQLJSONByteSourceUsesOwnedBackingAndGeneration(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertBytes("people", []byte(`[{"id":1,"name":"Ada"}]`))
	if err := trie.CreateSQLJSONFieldIndex("people", "id"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	source, err := trie.sqlJSONSource("people")
	if err != nil {
		t.Fatalf("sqlJSONSource() error = %v", err)
	}
	trie.mu.RLock()
	hval, _, err := trie.readValueRLockedChecked("people", true)
	if err != nil || !hval.IsBytesAtRaws() || hval.OnDisk() {
		trie.mu.RUnlock()
		t.Fatalf("byte source value = %#v, %v", hval, err)
	}
	backing := trie.raws.array[hval.Index]
	trie.mu.RUnlock()
	if len(backing) == 0 || unsafe.StringData(source.raw) != unsafe.SliceData(backing) {
		t.Fatal("sqlJSONSource() copied an in-memory byte source")
	}
	if _, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", "id", float64(1)); err != nil || !available {
		t.Fatalf("initial ResolveSQLIndexedSource() = available %v, error %v", available, err)
	}
	trie.UpsertBytes("people", []byte(`[{"id":2,"name":"Grace"}]`))
	rows, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", "id", float64(2))
	if err != nil || !available || len(rows) != 1 || rows[0]["name"] != "Grace" {
		t.Fatalf("updated ResolveSQLIndexedSource() = %#v, %v, %v", rows, available, err)
	}
}
