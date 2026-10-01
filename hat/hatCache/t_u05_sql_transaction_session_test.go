package hatCache

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTU05SQLTransactionSessionDefaults(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	session, err := NewSQLTransactionSession(trie)
	if err != nil {
		t.Fatalf("NewSQLTransactionSession() error = %v", err)
	}
	if got := session.Options(); got != (SQLTransactionOptions{}) {
		t.Fatalf("default Options() = %#v, want zero-value options", got)
	}

	transaction, err := session.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	defer transaction.Rollback()
	if got := transaction.Isolation(); got != SQLTransactionIsolationSnapshot {
		t.Fatalf("default transaction isolation = %v, want snapshot", got)
	}
	if transaction.ReadOnly() {
		t.Fatal("default transaction is read-only, want writable")
	}
}

func TestTU05SQLTransactionSessionSettingsAreInheritedAndReset(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	session, err := NewSQLTransactionSession(trie)
	if err != nil {
		t.Fatalf("NewSQLTransactionSession() error = %v", err)
	}
	want := SQLTransactionOptions{
		Isolation: SQLTransactionIsolationSerializable,
		ReadOnly:  true,
		Timeout:   time.Second,
	}
	if err := session.SetOptions(want); err != nil {
		t.Fatalf("SetOptions() error = %v", err)
	}
	if got := session.Options(); got != want {
		t.Fatalf("Options() = %#v, want %#v", got, want)
	}

	transaction, err := session.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if got := transaction.Isolation(); got != want.Isolation {
		t.Fatalf("transaction isolation = %v, want %v", got, want.Isolation)
	}
	if !transaction.ReadOnly() {
		t.Fatal("transaction ReadOnly() = false, want true")
	}
	if _, err := transaction.Execute("SET session-key session-value"); !errors.Is(err, ErrSQLTransactionReadOnly) {
		t.Fatalf("read-only Execute() error = %v, want %v", err, ErrSQLTransactionReadOnly)
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	session.ResetOptions()
	if got := session.Options(); got != (SQLTransactionOptions{}) {
		t.Fatalf("Options() after ResetOptions = %#v, want zero-value options", got)
	}
}

func TestTU05SQLTransactionSessionRejectsInvalidSettingsWithoutMutation(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	session, err := NewSQLTransactionSession(trie)
	if err != nil {
		t.Fatalf("NewSQLTransactionSession() error = %v", err)
	}
	want := SQLTransactionOptions{ReadOnly: true, Timeout: time.Second}
	if err := session.SetOptions(want); err != nil {
		t.Fatalf("SetOptions() error = %v", err)
	}
	if err := session.SetOptions(SQLTransactionOptions{Isolation: SQLTransactionIsolation(255)}); err == nil {
		t.Fatal("SetOptions() error = nil, want invalid isolation error")
	}
	if got := session.Options(); got != want {
		t.Fatalf("Options() after invalid isolation = %#v, want %#v", got, want)
	}
	if err := session.SetOptions(SQLTransactionOptions{Timeout: -time.Nanosecond}); err == nil {
		t.Fatal("SetOptions() error = nil, want invalid timeout error")
	}
	if got := session.Options(); got != want {
		t.Fatalf("Options() after invalid timeout = %#v, want %#v", got, want)
	}
}

func TestTU05SQLTransactionSessionRejectsNilTrie(t *testing.T) {
	if _, err := NewSQLTransactionSession(nil); !errors.Is(err, ErrNilHatTrie) {
		t.Fatalf("NewSQLTransactionSession(nil) error = %v, want %v", err, ErrNilHatTrie)
	}
}

func TestTU05SQLTransactionSessionOptionsAreRaceSafe(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	session, err := NewSQLTransactionSession(trie)
	if err != nil {
		t.Fatalf("NewSQLTransactionSession() error = %v", err)
	}
	const workers = 4
	const iterations = 100
	var group sync.WaitGroup
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer group.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				if worker%2 == 0 {
					if err := session.SetOptions(SQLTransactionOptions{ReadOnly: iteration%2 == 0}); err != nil {
						t.Errorf("SetOptions() error = %v", err)
						return
					}
				} else {
					_ = session.Options()
				}
			}
		}(worker)
	}
	group.Wait()
}
