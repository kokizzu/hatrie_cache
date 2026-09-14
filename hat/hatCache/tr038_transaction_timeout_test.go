package hatCache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTR038ExpiredSQLTransactionRejectsFurtherWorkAndCloses(t *testing.T) {
	trie := newTestTrie(t)
	transaction, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{
		Timeout: time.Hour,
	})
	if err != nil {
		t.Fatalf("BeginSQLTransactionWithOptions() error = %v", err)
	}

	transaction.mu.Lock()
	transaction.deadline = time.Unix(0, 0)
	transaction.mu.Unlock()

	if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('expired', 'value')"); !errors.Is(err, ErrSQLTransactionTimeout) {
		t.Fatalf("Execute() error = %v, want ErrSQLTransactionTimeout", err)
	}
	if err := transaction.Commit(); !errors.Is(err, ErrSQLTransactionTimeout) {
		t.Fatalf("Commit() error = %v, want ErrSQLTransactionTimeout", err)
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatalf("Rollback() after timeout error = %v", err)
	}
	if trie.Exists("expired") {
		t.Fatal("expired transaction published a write")
	}
}

func TestTR038SQLTransactionTimeoutDefaultsOff(t *testing.T) {
	trie := newTestTrie(t)
	transaction, err := BeginSQLTransaction(trie)
	if err != nil {
		t.Fatalf("BeginSQLTransaction() error = %v", err)
	}
	defer transaction.Rollback()

	if transaction.deadline != (time.Time{}) {
		t.Fatalf("default deadline = %v, want disabled", transaction.deadline)
	}
	if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('default', 'works')"); err != nil {
		t.Fatalf("Execute() with default timeout error = %v", err)
	}
}

func TestTR038SQLTransactionTimeoutReleasesSerializableLock(t *testing.T) {
	trie := newTestTrie(t)
	transaction, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{
		Isolation: SQLTransactionIsolationSerializable,
		Timeout:   time.Hour,
	})
	if err != nil {
		t.Fatalf("BeginSQLTransactionWithOptions() error = %v", err)
	}

	transaction.mu.Lock()
	transaction.deadline = time.Unix(0, 0)
	transaction.mu.Unlock()

	if _, err := transaction.Query(context.Background(), "FROM CACHE('missing') SELECT key", nil, SQLQueryOptions{}); !errors.Is(err, ErrSQLTransactionTimeout) {
		t.Fatalf("Query() error = %v, want ErrSQLTransactionTimeout", err)
	}
	if !trie.commandTransactionMu.TryRLock() {
		t.Fatal("timed-out serializable transaction did not release the command lock")
	}
	trie.commandTransactionMu.RUnlock()
}

func TestTR038SQLTransactionRejectsNegativeTimeout(t *testing.T) {
	trie := newTestTrie(t)
	defer trie.Destroy()

	if _, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{Timeout: -time.Second}); err == nil {
		t.Fatal("BeginSQLTransactionWithOptions() accepted a negative timeout")
	}
}

func TestTR038ExpiredReadOnlySQLTransactionReportsTimeout(t *testing.T) {
	trie := newTestTrie(t)
	transaction, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{
		ReadOnly: true,
		Timeout:  time.Hour,
	})
	if err != nil {
		t.Fatalf("BeginSQLTransactionWithOptions() error = %v", err)
	}

	transaction.mu.Lock()
	transaction.deadline = time.Unix(0, 0)
	transaction.mu.Unlock()

	if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('expired-read-only', 'value')"); !errors.Is(err, ErrSQLTransactionTimeout) {
		t.Fatalf("Execute() error = %v, want ErrSQLTransactionTimeout", err)
	}
}
