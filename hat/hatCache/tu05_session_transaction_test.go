package hatCache

import (
	"errors"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestSQLTransactionSessionDefaultsAndReset(t *testing.T) {
	options := SQLTransactionOptions{
		Isolation: SQLTransactionIsolationSerializable,
		ReadOnly:  true,
		Timeout:   time.Second,
	}
	session, err := NewSQLTransactionSession(options)
	if err != nil {
		t.Fatalf("NewSQLTransactionSession() error = %v", err)
	}
	if got := session.Options(); got != options {
		t.Fatalf("Options() = %#v, want %#v", got, options)
	}

	trie := CreateHatTrie()
	defer trie.Destroy()
	transaction, err := session.Begin(trie)
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if transaction.Isolation() != options.Isolation || !transaction.ReadOnly() {
		t.Fatalf("transaction settings = isolation %v read-only %t, want %#v", transaction.Isolation(), transaction.ReadOnly(), options)
	}
	if err := session.SetOptions(SQLTransactionOptions{}); err != nil {
		t.Fatalf("SetOptions() while transaction is active: %v", err)
	}
	if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('session-snapshot', 'value')"); !errors.Is(err, ErrSQLTransactionReadOnly) {
		t.Fatalf("active transaction after session update = %v, want ErrSQLTransactionReadOnly", err)
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	if got := session.Options(); got != (SQLTransactionOptions{}) {
		t.Fatalf("Options() after SetOptions = %#v, want zero options", got)
	}
	session.Reset()
	if got := session.Options(); got != (SQLTransactionOptions{}) {
		t.Fatalf("Options() after Reset = %#v, want zero options", got)
	}
}

func TestSQLTransactionSessionRejectsInvalidOptions(t *testing.T) {
	if _, err := NewSQLTransactionSession(SQLTransactionOptions{Timeout: -time.Second}); err == nil {
		t.Fatal("NewSQLTransactionSession() accepted a negative timeout")
	}
	session, err := NewSQLTransactionSession(SQLTransactionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.SetOptions(SQLTransactionOptions{Isolation: SQLTransactionIsolation(255)}); err == nil {
		t.Fatal("SetOptions() accepted an unknown isolation")
	}
}

func TestSQLTransactionSessionJournalDurability(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	session, err := NewSQLTransactionSession(SQLTransactionOptions{
		Durability: SQLTransactionDurabilityJournal,
		Journal:    journal,
	})
	if err != nil {
		journal.Close()
		t.Fatalf("NewSQLTransactionSession() error = %v", err)
	}
	trie := CreateHatTrie()
	transaction, err := session.Begin(trie)
	if err != nil {
		journal.Close()
		trie.Destroy()
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('durable-session', 'value')"); err != nil {
		transaction.Rollback()
		journal.Close()
		trie.Destroy()
		t.Fatalf("Execute() error = %v", err)
	}
	if err := transaction.Commit(); err != nil {
		journal.Close()
		trie.Destroy()
		t.Fatalf("Commit() error = %v", err)
	}
	if got := journal.Sequence(); got != 1 {
		journal.Close()
		trie.Destroy()
		t.Fatalf("journal sequence = %d, want 1", got)
	}
	if err := journal.Close(); err != nil {
		trie.Destroy()
		t.Fatalf("journal.Close() error = %v", err)
	}
	trie.Destroy()

	reopened, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("reopen journal error = %v", err)
	}
	defer reopened.Close()
	restored := CreateHatTrie()
	defer restored.Destroy()
	if _, err := reopened.Replay(restored, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	response := restored.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "durable-session"})
	if !response.OK || response.Value != "value" {
		t.Fatalf("restored durable value = %#v, want value", response)
	}
}

func TestSQLTransactionSessionRequiresJournalForJournalDurability(t *testing.T) {
	if _, err := NewSQLTransactionSession(SQLTransactionOptions{Durability: SQLTransactionDurabilityJournal}); !errors.Is(err, ErrSQLTransactionJournalRequired) {
		t.Fatalf("NewSQLTransactionSession() error = %v, want ErrSQLTransactionJournalRequired", err)
	}
}

func BenchmarkTU05DirectTransactionBegin(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	options := SQLTransactionOptions{Isolation: SQLTransactionIsolationSnapshot}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transaction, err := BeginSQLTransactionWithOptions(trie, options)
		if err != nil {
			b.Fatal(err)
		}
		if err := transaction.Rollback(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU05SessionTransactionBegin(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	session, err := NewSQLTransactionSession(SQLTransactionOptions{Isolation: SQLTransactionIsolationSnapshot})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transaction, err := session.Begin(trie)
		if err != nil {
			b.Fatal(err)
		}
		if err := transaction.Rollback(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU05SessionOptions(b *testing.B) {
	session, err := NewSQLTransactionSession(SQLTransactionOptions{Isolation: SQLTransactionIsolationSerializable})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = session.Options()
	}
}

func BenchmarkTU05MemoryTransactionCommit(b *testing.B) {
	benchmarkTU05TransactionCommit(b, SQLTransactionOptions{})
}

func BenchmarkTU05JournalTransactionCommit(b *testing.B) {
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "transactions.journal"))
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	benchmarkTU05TransactionCommit(b, SQLTransactionOptions{
		Durability: SQLTransactionDurabilityJournal,
		Journal:    journal,
	})
}

func benchmarkTU05TransactionCommit(b *testing.B, options SQLTransactionOptions) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	session, err := NewSQLTransactionSession(options)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transaction, err := session.Begin(trie)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('bench-" + strconv.Itoa(i) + "', 'value')"); err != nil {
			b.Fatal(err)
		}
		if err := transaction.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
