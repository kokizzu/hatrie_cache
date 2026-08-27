package hatCache

import (
	"bytes"
	"testing"

	"hatrie_cache/hat/hatCodec"
)

func TestLevelDBStoreEncryptionHidesRecordsAndRestores(t *testing.T) {
	cipher, err := hatCodec.NewStreamCipher("test-key", bytes.Repeat([]byte{5}, 32))
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenLevelDBStoreWithFormatAndCipher(t.TempDir()+"/store", DefaultStorageFormat, cipher)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	trie := newTestTrie(t)
	trie.UpsertString("secret", "secret-value")
	if err := store.Save(trie); err != nil {
		t.Fatal(err)
	}
	raw, err := store.db.Get(levelDBKey("secret"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("secret-value")) {
		t.Fatal("plaintext was present in encrypted LevelDB record")
	}
	trie.UpsertString("secret", "updated-secret-value")
	if err := store.SaveKeys(trie, []string{"secret"}); err != nil {
		t.Fatal(err)
	}
	raw, err = store.db.Get(levelDBKey("secret"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("updated-secret-value")) {
		t.Fatal("plaintext was present after encrypted incremental LevelDB save")
	}
	restored := newTestTrie(t)
	if _, err := store.Load(restored); err != nil {
		t.Fatal(err)
	}
	value := restored.GetString("secret")
	if value != "updated-secret-value" {
		t.Fatalf("restored value = %q", value)
	}
}
