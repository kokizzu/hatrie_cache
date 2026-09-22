package hatCache

import (
	"context"
	"errors"
	"testing"
)

func TestT233SQLTransactionYieldKeepsSnapshotTransactionUsable(t *testing.T) {
	trie := newTestTrie(t)
	tx, err := BeginSQLTransaction(trie)
	if err != nil {
		t.Fatalf("BeginSQLTransaction() error = %v", err)
	}
	defer tx.Rollback()

	if err := tx.Yield(nil); err != nil {
		t.Fatalf("Yield(nil) error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tx.Yield(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Yield(canceled) error = %v, want %v", err, context.Canceled)
	}
	if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('yielded', 'still-open')"); err != nil {
		t.Fatalf("Execute() after canceled yield error = %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() after canceled yield error = %v", err)
	}
	if got := trie.GetString("yielded"); got != "still-open" {
		t.Fatalf("yielded = %q, want still-open", got)
	}
}

func TestT233SQLTransactionYieldReportsClosedAndTimeoutTransactions(t *testing.T) {
	trie := newTestTrie(t)
	tx, err := BeginSQLTransaction(trie)
	if err != nil {
		t.Fatalf("BeginSQLTransaction() error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if err := tx.Yield(context.Background()); err == nil {
		t.Fatal("Yield() accepted a closed transaction")
	}

	timedOut, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{Timeout: 1})
	if err != nil {
		t.Fatalf("BeginSQLTransactionWithOptions() error = %v", err)
	}
	defer timedOut.Rollback()
	if err := timedOut.Yield(context.Background()); !errors.Is(err, ErrSQLTransactionTimeout) {
		t.Fatalf("Yield() timeout error = %v, want %v", err, ErrSQLTransactionTimeout)
	}
}
