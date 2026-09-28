package hatCache

import (
	"context"
	"errors"
	"testing"
)

func TestT234SQLTransactionEarlyConflictCheckRejectsStaleExecute(t *testing.T) {
	trie := newTestTrie(t)
	tx, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{EarlyConflictCheck: true})
	if err != nil {
		t.Fatalf("BeginSQLTransactionWithOptions() error = %v", err)
	}
	defer tx.Rollback()

	trie.UpsertString("concurrent", "write")
	if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('draft', 'private')"); !errors.Is(err, ErrSQLTransactionConflict) {
		t.Fatalf("stale Execute() error = %v, want ErrSQLTransactionConflict", err)
	}
}

func TestT234SQLTransactionCommitConflictUsesSentinel(t *testing.T) {
	trie := newTestTrie(t)
	tx, err := BeginSQLTransaction(trie)
	if err != nil {
		t.Fatalf("BeginSQLTransaction() error = %v", err)
	}

	if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('draft', 'private')"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	trie.UpsertString("concurrent", "write")

	if err := tx.Commit(); !errors.Is(err, ErrSQLTransactionConflict) {
		t.Fatalf("Commit() error = %v, want ErrSQLTransactionConflict", err)
	}
}

func TestT234SQLTransactionEarlyConflictCheckRejectsStaleQuery(t *testing.T) {
	trie := newTestTrie(t)
	tx, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{EarlyConflictCheck: true})
	if err != nil {
		t.Fatalf("BeginSQLTransactionWithOptions() error = %v", err)
	}
	defer tx.Rollback()

	trie.UpsertString("concurrent", "write")
	if _, err := tx.Query(context.Background(), "FROM CACHE('missing') SELECT key", nil, SQLQueryOptions{}); !errors.Is(err, ErrSQLTransactionConflict) {
		t.Fatalf("stale Query() error = %v, want ErrSQLTransactionConflict", err)
	}
}
