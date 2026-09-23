package hatCache

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenPebbleStoreReadOnlyLoadsAndRejectsWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.pebble")
	writable, err := OpenPebbleStore(path)
	if err != nil {
		t.Fatal(err)
	}
	trie := CreateHatTrie()
	trie.UpsertString("c240", "readonly")
	if err := writable.Save(trie); err != nil {
		t.Fatal(err)
	}
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}

	readOnly, err := OpenPebbleStoreReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	loaded := CreateHatTrie()
	if _, err := readOnly.Load(loaded); err != nil {
		t.Fatal(err)
	}
	if got := loaded.GetString("c240"); got != "readonly" {
		t.Fatalf("read-only Load() value = %q, want readonly", got)
	}
	if err := readOnly.Save(trie); err == nil {
		t.Fatal("read-only Save() unexpectedly succeeded")
	}
}

func TestOpenPebbleStoreReadOnlyDoesNotCreateMissingPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.pebble")
	if _, err := OpenPebbleStoreReadOnly(path); err == nil {
		t.Fatal("OpenPebbleStoreReadOnly() unexpectedly opened a missing path")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("read-only open created missing path, Stat() error = %v", err)
	}
}
