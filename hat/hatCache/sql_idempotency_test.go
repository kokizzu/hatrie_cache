package hatCache

import (
	"context"
	"path/filepath"
	"testing"
)

func TestExecuteSQLMutationIdempotentInsertSurvivesJournalReplay(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "commands.journal")
	trie := newTestTrie(t)
	journal, err := OpenCommandJournalWithOptions(journalPath, CommandJournalOptions{
		IdempotencyCapacity: 16,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}

	query := "INSERT INTO cache (key, value) VALUES ('dedup:1', 'first')"
	first, err := ExecuteSQLMutationIdempotent(context.Background(), journal, trie, query, nil, SQLQueryOptions{}, "insert-1")
	if err != nil {
		t.Fatalf("first ExecuteSQLMutationIdempotent() error = %v", err)
	}
	if first.Affected != 1 || !first.Response.OK {
		t.Fatalf("first mutation = %#v, want one applied row", first)
	}
	second, err := ExecuteSQLMutationIdempotent(context.Background(), journal, trie, query, nil, SQLQueryOptions{}, "insert-1")
	if err != nil {
		t.Fatalf("duplicate ExecuteSQLMutationIdempotent() error = %v", err)
	}
	if second.Affected != 1 || !second.Response.OK {
		t.Fatalf("duplicate mutation = %#v, want the original response", second)
	}
	if value := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "dedup:1"}); value.Value != "first" {
		t.Fatalf("duplicate mutation changed stored value: %#v", value)
	}

	if _, err := ExecuteSQLMutationIdempotent(context.Background(), journal, trie, "INSERT INTO cache (key, value) VALUES ('dedup:1', 'different')", nil, SQLQueryOptions{}, "insert-1"); err == nil {
		t.Fatal("idempotency key reuse with a different mutation succeeded")
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("journal.Close() error = %v", err)
	}

	reopened, err := OpenCommandJournalWithOptions(journalPath, CommandJournalOptions{
		IdempotencyCapacity: 16,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("reopen CommandJournal error = %v", err)
	}
	defer reopened.Close()
	replayed := newTestTrie(t)
	if _, err := reopened.Replay(replayed, 0); err != nil {
		t.Fatalf("reopened.Replay() error = %v", err)
	}
	retry, err := ExecuteSQLMutationIdempotent(context.Background(), reopened, replayed, query, nil, SQLQueryOptions{}, "insert-1")
	if err != nil {
		t.Fatalf("post-replay duplicate ExecuteSQLMutationIdempotent() error = %v", err)
	}
	if retry.Affected != 1 || !retry.Response.OK {
		t.Fatalf("post-replay duplicate mutation = %#v, want the original response", retry)
	}
	if value := replayed.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "dedup:1"}); value.Value != "first" {
		t.Fatalf("post-replay duplicate changed stored value: %#v", value)
	}
}

func TestExecuteSQLMutationIdempotentInsertSelectUsesOneToken(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "commands.journal")
	trie := newTestTrie(t)
	if response := trie.ExecuteCommand(CacheCommandRequest{
		Command: "SETSTR",
		Key:     "source:rows",
		Value:   `[{"key":"copied:1","value":"one"},{"key":"copied:2","value":"two"}]`,
	}); !response.OK {
		t.Fatalf("seed source response = %#v", response)
	}
	journal, err := OpenCommandJournalWithOptions(journalPath, CommandJournalOptions{
		IdempotencyCapacity: 16,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()

	query := "INSERT INTO cache (key, value) SELECT key, value FROM CACHE('source:rows')"
	first, err := ExecuteSQLMutationIdempotent(context.Background(), journal, trie, query, nil, SQLQueryOptions{}, "insert-select-1")
	if err != nil {
		t.Fatalf("first INSERT ... SELECT error = %v", err)
	}
	if first.Affected != 2 || !first.Response.OK {
		t.Fatalf("first INSERT ... SELECT = %#v, want two applied rows", first)
	}
	second, err := ExecuteSQLMutationIdempotent(context.Background(), journal, trie, query, nil, SQLQueryOptions{}, "insert-select-1")
	if err != nil {
		t.Fatalf("duplicate INSERT ... SELECT error = %v", err)
	}
	if second.Affected != 2 || !second.Response.OK {
		t.Fatalf("duplicate INSERT ... SELECT = %#v, want the original response", second)
	}
	for key, want := range map[string]string{"copied:1": "one", "copied:2": "two"} {
		response := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: key})
		if response.Value != want {
			t.Fatalf("duplicate INSERT ... SELECT changed %q: %#v", key, response)
		}
	}
}

func TestExecuteSQLMutationIdempotentRejectsUnsupportedOrUnconfiguredUse(t *testing.T) {
	trie := newTestTrie(t)
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		IdempotencyCapacity: 16,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()

	for _, query := range []string{
		"INSERT INTO cache (key, value) VALUES ('returning:1', 'value') RETURNING key",
		"INSERT INTO cache (key, value) VALUES ('conflict:1', 'value') ON CONFLICT DO NOTHING",
		"MERGE INTO cache (key, value) VALUES ('merge:1', 'value') WHEN NOT MATCHED THEN INSERT",
	} {
		if _, err := ExecuteSQLMutationIdempotent(context.Background(), journal, trie, query, nil, SQLQueryOptions{}, "unsupported"); err == nil {
			t.Fatalf("ExecuteSQLMutationIdempotent(%q) succeeded", query)
		}
	}
	if _, err := ExecuteSQLMutationIdempotent(context.Background(), journal, trie, "INSERT INTO cache (key, value) VALUES ('empty-key:1', 'value')", nil, SQLQueryOptions{}, " "); err == nil {
		t.Fatal("empty idempotency key succeeded")
	}
	if _, err := ExecuteSQLMutationIdempotent(context.Background(), journal, trie, "INSERT INTO cache (key, value) VALUES ('long-key:1', 'value')", nil, SQLQueryOptions{}, string(make([]byte, MaxCommandJournalIdempotencyKeyBytes+1))); err == nil {
		t.Fatal("oversized idempotency key succeeded")
	}

	disabled, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("disabled OpenCommandJournalWithOptions() error = %v", err)
	}
	defer disabled.Close()
	if _, err := ExecuteSQLMutationIdempotent(context.Background(), disabled, trie, "INSERT INTO cache (key, value) VALUES ('disabled:1', 'value')", nil, SQLQueryOptions{}, "disabled"); err == nil {
		t.Fatal("idempotency with disabled journal succeeded")
	}
}
