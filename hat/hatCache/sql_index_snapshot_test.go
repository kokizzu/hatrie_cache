package hatCache

import "testing"

func TestSQLJSONIndexesShareSourceSnapshot(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"active":true,"name":"Ada Lovelace"},{"id":2,"active":false,"name":"Grace Hopper"}]`)
	if err := trie.CreateSQLJSONBitmapIndex("people", "active"); err != nil {
		t.Fatalf("CreateSQLJSONBitmapIndex() error = %v", err)
	}
	if err := trie.CreateSQLJSONTextIndex("people", "name"); err != nil {
		t.Fatalf("CreateSQLJSONTextIndex() error = %v", err)
	}
	if _, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", "active", true); err != nil || !available {
		t.Fatalf("ResolveSQLIndexedSource() = available %v, error %v", available, err)
	}
	if _, available, err := trie.ResolveSQLTextSource("CACHE", "people", "name", "ada"); err != nil || !available {
		t.Fatalf("ResolveSQLTextSource() = available %v, error %v", available, err)
	}

	trie.sqlIndexMu.RLock()
	first := trie.sqlJSONIndexSnapshots["people"]
	bitmap := trie.sqlJSONBitmapIndexes["people"]["active"]
	text := trie.sqlJSONTextIndexes["people"]["name"]
	trie.sqlIndexMu.RUnlock()
	if first == nil || len(first.rows) != 2 || len(bitmap.rows) != 2 || len(text.rows) != 2 {
		t.Fatalf("unexpected shared index rows: snapshot=%#v bitmap=%d text=%d", first, len(bitmap.rows), len(text.rows))
	}
	if &first.rows[0] != &bitmap.rows[0] || &first.rows[0] != &text.rows[0] {
		t.Fatalf("indexes did not retain the shared source rows")
	}

	trie.UpsertString("people", `[{"id":3,"active":true,"name":"Margaret Hamilton"}]`)
	if _, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", "active", true); err != nil || !available {
		t.Fatalf("updated ResolveSQLIndexedSource() = available %v, error %v", available, err)
	}
	if _, available, err := trie.ResolveSQLTextSource("CACHE", "people", "name", "margaret"); err != nil || !available {
		t.Fatalf("updated ResolveSQLTextSource() = available %v, error %v", available, err)
	}
	trie.sqlIndexMu.RLock()
	second := trie.sqlJSONIndexSnapshots["people"]
	bitmap = trie.sqlJSONBitmapIndexes["people"]["active"]
	text = trie.sqlJSONTextIndexes["people"]["name"]
	trie.sqlIndexMu.RUnlock()
	if second == nil || second == first || len(second.rows) != 1 || second.rows[0]["id"] != float64(3) {
		t.Fatalf("updated snapshot = %#v, want one new row", second)
	}
	if len(first.rows) != 2 || first.rows[0]["id"] != float64(1) {
		t.Fatalf("prior snapshot was mutated: %#v", first.rows)
	}
	if &second.rows[0] != &bitmap.rows[0] || &second.rows[0] != &text.rows[0] {
		t.Fatalf("updated indexes did not retain the shared source rows")
	}
}

func TestSQLJSONIndexUsesSourceWriteGeneration(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"name":"Ada"}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "id"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	if _, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", "id", float64(1)); err != nil || !available {
		t.Fatalf("initial ResolveSQLIndexedSource() = available %v, error %v", available, err)
	}

	trie.mu.RLock()
	firstGeneration := trie.sqlJSONIndexSourceGenerations["people"]
	trie.mu.RUnlock()
	trie.sqlIndexMu.RLock()
	firstIndexGeneration := trie.sqlJSONIndexes["people"]["id"].generation
	trie.sqlIndexMu.RUnlock()
	if firstGeneration != firstIndexGeneration {
		t.Fatalf("initial source/index generation = %d/%d, want equal", firstGeneration, firstIndexGeneration)
	}

	trie.UpsertString("people", `[{"id":2,"name":"Grace"}]`)
	trie.mu.RLock()
	secondGeneration := trie.sqlJSONIndexSourceGenerations["people"]
	trie.mu.RUnlock()
	if secondGeneration <= firstGeneration {
		t.Fatalf("source generation after replacement = %d, want greater than %d", secondGeneration, firstGeneration)
	}
	if _, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", "id", float64(2)); err != nil || !available {
		t.Fatalf("updated ResolveSQLIndexedSource() = available %v, error %v", available, err)
	}
	trie.sqlIndexMu.RLock()
	secondIndexGeneration := trie.sqlJSONIndexes["people"]["id"].generation
	trie.sqlIndexMu.RUnlock()
	if secondIndexGeneration != secondGeneration {
		t.Fatalf("updated index generation = %d, want %d", secondIndexGeneration, secondGeneration)
	}
}
