package hatCache

import "testing"

func TestHatTrieBorrowSQLColumnarSourceSharesOnlyImmutableLayout(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued"},{"id":2,"state":"running"}]`)
	fields := []string{"id", "state"}
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
			t.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}

	borrowed, available, err := trie.BorrowSQLColumnarSource("CACHE", "jobs", fields)
	if err != nil || !available || borrowed.Rows != 2 {
		t.Fatalf("BorrowSQLColumnarSource() = %#v, %t, %v", borrowed, available, err)
	}
	again, available, err := trie.BorrowSQLColumnarSource("CACHE", "jobs", fields)
	if err != nil || !available || &borrowed.Columns["id"][0] != &again.Columns["id"][0] {
		t.Fatalf("borrowed layouts must share immutable cached slices: available = %t, error = %v", available, err)
	}
	resolved, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields)
	if err != nil || !available || &borrowed.Columns["id"][0] == &resolved.Columns["id"][0] {
		t.Fatalf("ResolveSQLColumnarSource() must retain caller isolation: available = %t, error = %v", available, err)
	}
}
