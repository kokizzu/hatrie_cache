package hatCache

import (
	"testing"
	"time"
)

var tu05TransactionOptionsSink SQLTransactionOptions

var tu05TransactionSessionOptionsSink SQLTransactionOptions

func BenchmarkTU05DirectTransactionOptionCopy(b *testing.B) {
	options := SQLTransactionOptions{
		Isolation: SQLTransactionIsolationSerializable,
		ReadOnly:  true,
		Timeout:   2 * time.Second,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tu05TransactionOptionsSink = options
	}
}

func BenchmarkTU05SessionOptionsRead(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	session, err := NewSQLTransactionSession(trie)
	if err != nil {
		b.Fatal(err)
	}
	if err := session.SetOptions(SQLTransactionOptions{
		Isolation: SQLTransactionIsolationSerializable,
		ReadOnly:  true,
	}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tu05TransactionSessionOptionsSink = session.Options()
	}
}
