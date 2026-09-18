package hatCache

import (
	"strconv"
	"testing"
)

// BenchmarkTR034SQLTransactionSavepoint measures the current snapshot-clone
// savepoint path against the same transaction without a savepoint.
func BenchmarkTR034SQLTransactionSavepoint(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	for index := 0; index < 128; index++ {
		trie.UpsertString("seed:"+strconv.Itoa(index), "value")
	}

	b.ReportAllocs()
	b.Run("without_savepoint", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			transaction, err := BeginSQLTransaction(trie)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('transaction-key', 'value')"); err != nil {
				b.Fatal(err)
			}
			if err := transaction.Rollback(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("with_savepoint", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			transaction, err := BeginSQLTransaction(trie)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('transaction-key', 'value')"); err != nil {
				b.Fatal(err)
			}
			if err := transaction.Savepoint("before_extra_work"); err != nil {
				b.Fatal(err)
			}
			if err := transaction.Rollback(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
