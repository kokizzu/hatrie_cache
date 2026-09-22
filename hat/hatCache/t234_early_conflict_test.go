package hatCache

import (
	"errors"
	"testing"
)

func TestT234SQLTransactionEarlyConflictDetectionAbortsStaleWork(t *testing.T) {
	trie := newTestTrie(t)
	tx, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{EarlyConflictDetection: true})
	if err != nil {
		t.Fatalf("BeginSQLTransactionWithOptions() error = %v", err)
	}
	defer tx.Rollback()

	trie.UpsertString("concurrent", "write")
	if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('stale', 'must-not-stage')"); !errors.Is(err, ErrSQLTransactionConflict) {
		t.Fatalf("early conflict Execute() error = %v, want %v", err, ErrSQLTransactionConflict)
	}
	if trie.Exists("stale") {
		t.Fatal("early conflict leaked a stale write to the live cache")
	}
	if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('after-conflict', 'must-not-run')"); err == nil {
		t.Fatal("transaction remained usable after early conflict")
	}
}

func TestT234SQLTransactionDefaultConflictRemainsLateAndOptInPathCommits(t *testing.T) {
	t.Run("default_late", func(t *testing.T) {
		trie := newTestTrie(t)
		tx, err := BeginSQLTransaction(trie)
		if err != nil {
			t.Fatalf("BeginSQLTransaction() error = %v", err)
		}
		defer tx.Rollback()
		trie.UpsertString("concurrent", "write")
		if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('stale', 'private')"); err != nil {
			t.Fatalf("default Execute() error = %v, want late conflict: %v", err, ErrSQLTransactionConflict)
		}
		if err := tx.Commit(); !errors.Is(err, ErrSQLTransactionConflict) {
			t.Fatalf("default Commit() error = %v, want %v", err, ErrSQLTransactionConflict)
		}
		if trie.Exists("stale") {
			t.Fatal("late conflict leaked a stale write to the live cache")
		}
	})

	t.Run("no_conflict", func(t *testing.T) {
		trie := newTestTrie(t)
		tx, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{EarlyConflictDetection: true})
		if err != nil {
			t.Fatalf("BeginSQLTransactionWithOptions() error = %v", err)
		}
		defer tx.Rollback()
		if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('fresh', 'value')"); err != nil {
			t.Fatalf("fresh Execute() error = %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("fresh Commit() error = %v", err)
		}
		if got := trie.GetString("fresh"); got != "value" {
			t.Fatalf("fresh = %q, want value", got)
		}
	})
}
