package hatCache

import (
	"context"
	"testing"
)

func TestSQLDirectStringSourcesRefreshAfterReplacement(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)

	if rows, err := trie.ResolveSQLSource("CACHE", "jobs"); err != nil || len(rows) != 1 || rows[0]["id"] != float64(1) {
		t.Fatalf("initial source rows/error = %#v/%v", rows, err)
	}
	if batch, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", []string{"id", "state"}); err != nil || !available || batch.Rows != 1 || batch.Columns["id"][0] != float64(1) {
		t.Fatalf("initial columnar batch/available/error = %#v/%t/%v", batch, available, err)
	}
	var initialIDs []interface{}
	if err := trie.StreamSQLSource(context.Background(), "CACHE", "jobs", func(row SQLRow) error {
		initialIDs = append(initialIDs, row["id"])
		return nil
	}); err != nil || len(initialIDs) != 1 || initialIDs[0] != float64(1) {
		t.Fatalf("initial stream ids/error = %#v/%v", initialIDs, err)
	}

	trie.UpsertString("jobs", `[{"id":2,"state":"running"}]`)
	if rows, err := trie.ResolveSQLSource("CACHE", "jobs"); err != nil || len(rows) != 1 || rows[0]["id"] != float64(2) {
		t.Fatalf("replacement source rows/error = %#v/%v", rows, err)
	}
	if batch, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", []string{"id", "state"}); err != nil || !available || batch.Rows != 1 || batch.Columns["id"][0] != float64(2) {
		t.Fatalf("replacement columnar batch/available/error = %#v/%t/%v", batch, available, err)
	}
	var replacementIDs []interface{}
	if err := trie.StreamSQLSource(context.Background(), "CACHE", "jobs", func(row SQLRow) error {
		replacementIDs = append(replacementIDs, row["id"])
		return nil
	}); err != nil || len(replacementIDs) != 1 || replacementIDs[0] != float64(2) {
		t.Fatalf("replacement stream ids/error = %#v/%v", replacementIDs, err)
	}
}
