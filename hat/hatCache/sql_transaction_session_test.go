package hatCache

import (
	"testing"
	"time"
)

func TestSQLTransactionSessionAppliesDefaultsToNewTransactions(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	want := SQLTransactionOptions{
		Isolation: SQLTransactionIsolationSerializable,
		ReadOnly:  true,
		Timeout:   time.Minute,
	}
	session, err := NewSQLTransactionSessionWithOptions(trie, want)
	if err != nil {
		t.Fatalf("NewSQLTransactionSessionWithOptions() error = %v", err)
	}
	if got := session.Options(); got != want {
		t.Fatalf("Options() = %#v, want %#v", got, want)
	}

	transaction, err := session.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	defer transaction.Rollback()
	if transaction.Isolation() != want.Isolation {
		t.Fatalf("Isolation() = %v, want %v", transaction.Isolation(), want.Isolation)
	}
	if !transaction.ReadOnly() {
		t.Fatal("session transaction is writable, want read-only")
	}
	if transaction.deadline.IsZero() {
		t.Fatal("session transaction did not receive timeout")
	}
}

func TestSQLTransactionSessionSetOptionsAffectsFutureTransactions(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	session, err := NewSQLTransactionSession(trie)
	if err != nil {
		t.Fatalf("NewSQLTransactionSession() error = %v", err)
	}
	want := SQLTransactionOptions{ReadOnly: true}
	if err := session.SetOptions(want); err != nil {
		t.Fatalf("SetOptions() error = %v", err)
	}
	transaction, err := session.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	defer transaction.Rollback()
	if !transaction.ReadOnly() {
		t.Fatal("updated session transaction is writable, want read-only")
	}
}

func TestSQLTransactionSessionRejectsInvalidOptionsWithoutChangingDefaults(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	want := SQLTransactionOptions{ReadOnly: true}
	session, err := NewSQLTransactionSessionWithOptions(trie, want)
	if err != nil {
		t.Fatalf("NewSQLTransactionSessionWithOptions() error = %v", err)
	}
	if err := session.SetOptions(SQLTransactionOptions{Timeout: -time.Second}); err == nil {
		t.Fatal("SetOptions() accepted a negative timeout")
	}
	if got := session.Options(); got != want {
		t.Fatalf("Options() after rejected update = %#v, want %#v", got, want)
	}
}
