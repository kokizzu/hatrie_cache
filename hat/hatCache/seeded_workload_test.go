package hatCache

import (
	"math/rand"
	"path/filepath"
	"testing"
)

func TestSeededReadWriteRecoveryWorkload(t *testing.T) {
	random := rand.New(rand.NewSource(20260828))
	trie := newTestTrie(t)
	for step := 0; step < 200; step++ {
		key := "key:" + string(rune('a'+random.Intn(8)))
		if random.Intn(3) == 0 {
			trie.Delete(key)
		} else {
			trie.UpsertString(key, "value")
		}
		_ = trie.GetString(key)
		if step%25 == 0 {
			path := filepath.Join(t.TempDir(), "snapshot.json.gz")
			if err := trie.SaveSnapshot(path); err != nil {
				t.Fatalf("step %d save: %v", step, err)
			}
			recovered := newTestTrie(t)
			if err := recovered.LoadSnapshot(path); err != nil {
				t.Fatalf("step %d recover: %v", step, err)
			}
			if recovered.Size() != trie.Size() {
				t.Fatalf("step %d size = %d, want %d", step, recovered.Size(), trie.Size())
			}
		}
	}
}
