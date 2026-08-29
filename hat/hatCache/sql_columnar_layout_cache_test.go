package hatCache

import "testing"

func TestSQLColumnarLayoutCachePromotesAndInvalidates(t *testing.T) {
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)

	trie.UpsertString("jobs", `[{"id":1,"state":"queued"},{"id":2,"state":"running"}]`)
	fields := []string{"id", "state"}

	for scan := 0; scan < 2; scan++ {
		batch, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields)
		if err != nil || !available || batch.Rows != 2 {
			t.Fatalf("warm-up ResolveSQLColumnarSource() = %#v, %v, %v", batch, available, err)
		}
	}
	if stats := trie.sqlColumnarLayoutCacheStats(); stats.Entries != 1 || stats.Hits != 0 {
		t.Fatalf("layout cache after promotion = %#v, want one entry and no hits", stats)
	}

	batch, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields)
	if err != nil || !available || batch.Columns["state"][0] != "queued" {
		t.Fatalf("cached ResolveSQLColumnarSource() = %#v, %v, %v", batch, available, err)
	}
	if stats := trie.sqlColumnarLayoutCacheStats(); stats.Hits != 1 {
		t.Fatalf("layout cache after hit = %#v, want one hit", stats)
	}

	batch.Columns["state"][0] = "corrupted by caller"
	batch, available, err = trie.ResolveSQLColumnarSource("CACHE", "jobs", fields)
	if err != nil || !available || batch.Columns["state"][0] != "queued" {
		t.Fatalf("cached batch was not isolated from the caller = %#v, %v, %v", batch, available, err)
	}

	trie.UpsertString("jobs", `[{"id":3,"state":"done"}]`)
	if stats := trie.sqlColumnarLayoutCacheStats(); stats.Entries != 0 {
		t.Fatalf("layout cache after overwrite = %#v, want no stale entries", stats)
	}
	batch, available, err = trie.ResolveSQLColumnarSource("CACHE", "jobs", fields)
	if err != nil || !available || batch.Rows != 1 || batch.Columns["id"][0] != float64(3) || batch.Columns["state"][0] != "done" {
		t.Fatalf("post-overwrite ResolveSQLColumnarSource() = %#v, %v, %v", batch, available, err)
	}
}

func (ht *HatTrie) sqlColumnarLayoutCacheStats() sqlColumnarLayoutCacheStats {
	return ht.sqlColumnarLayouts.stats()
}
