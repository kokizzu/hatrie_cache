package hatCache

import (
	"errors"
	"testing"
)

func TestTR036ReadOnlySQLTransactionRejectsMutation(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	transaction, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginSQLTransactionWithOptions() error = %v", err)
	}
	defer transaction.Rollback()

	if !transaction.ReadOnly() {
		t.Fatal("ReadOnly() = false, want true")
	}
	if _, err := transaction.Execute("SET key value"); !errors.Is(err, ErrSQLTransactionReadOnly) {
		t.Fatalf("Execute() error = %v, want ErrSQLTransactionReadOnly", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("Commit() error = %v, want no-op success", err)
	}
}

func TestTR036DefaultSQLTransactionRemainsWritable(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	transaction, err := BeginSQLTransaction(trie)
	if err != nil {
		t.Fatalf("BeginSQLTransaction() error = %v", err)
	}
	defer transaction.Rollback()
	if transaction.ReadOnly() {
		t.Fatal("default ReadOnly() = true, want false")
	}
}

func BenchmarkTR036ReadOnlySQLTransactionGuard(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	transaction, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{ReadOnly: true})
	if err != nil {
		b.Fatalf("BeginSQLTransactionWithOptions() error = %v", err)
	}
	defer transaction.Rollback()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := transaction.Execute("SET key value"); !errors.Is(err, ErrSQLTransactionReadOnly) {
			b.Fatalf("Execute() error = %v, want ErrSQLTransactionReadOnly", err)
		}
	}
}
