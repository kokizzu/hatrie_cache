package hatCache

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestVolatileHatTrieKeepsLargeBytesInMemory(t *testing.T) {
	trie := CreateVolatileHatTrie()
	defer trie.Destroy()

	if !trie.IsVolatile() {
		t.Fatal("CreateVolatileHatTrie() did not create a volatile trie")
	}
	payload := bytes.Repeat([]byte("v"), DiskBytesThreshold+1)
	if err := trie.UpsertBytesChecked("large", payload); err != nil {
		t.Fatal(err)
	}
	value := trie.Get("large")
	if value.OnDisk() {
		t.Fatal("volatile trie marked a large value as disk-backed")
	}
	if got := trie.GetBytes("large"); !bytes.Equal(got, payload) {
		t.Fatalf("GetBytes() length = %d, want %d", len(got), len(payload))
	}
	got := trie.GetBytes("large")
	got[0] = 'x'
	if got = trie.GetBytes("large"); got[0] != 'v' {
		t.Fatal("GetBytes() returned mutable backing storage")
	}
	if len(trie.disks.paths) != 0 {
		t.Fatalf("volatile disk paths = %d, want 0", len(trie.disks.paths))
	}
	if trie.disks.dir != "" || trie.disks.rootDir != "" {
		t.Fatalf("volatile disk directories = %q/%q, want empty", trie.disks.dir, trie.disks.rootDir)
	}
}

func TestDefaultHatTrieStillSpillsLargeBytesToDisk(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	payload := bytes.Repeat([]byte("d"), DiskBytesThreshold+1)
	if err := trie.UpsertBytesChecked("large", payload); err != nil {
		t.Fatal(err)
	}
	value := trie.Get("large")
	if !value.OnDisk() {
		t.Fatal("default trie stopped marking large values as disk-backed")
	}
	if len(trie.disks.paths) != 1 || trie.disks.dir == "" {
		t.Fatalf("default disk storage = paths %d, dir %q", len(trie.disks.paths), trie.disks.dir)
	}
}

func TestVolatileHatTrieRejectsPersistence(t *testing.T) {
	trie := CreateVolatileHatTrie()
	defer trie.Destroy()

	path := t.TempDir() + "/volatile.hc"
	if err := trie.SaveSnapshot(path); !errors.Is(err, ErrVolatilePersistence) {
		t.Fatalf("SaveSnapshot() error = %v, want %v", err, ErrVolatilePersistence)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("volatile snapshot path Stat() error = %v, want not exist", err)
	}
	bundlePath := t.TempDir() + "/bundle"
	if _, err := CreateBackupBundle(bundlePath, trie, nil, BackupBundleOptions{}); !errors.Is(err, ErrVolatilePersistence) {
		t.Fatalf("CreateBackupBundle() error = %v, want %v", err, ErrVolatilePersistence)
	}
}
