package hatCache

import (
	"errors"
	"testing"
)

var errT232BenchmarkRollback = errors.New("benchmark scope rollback")

// BenchmarkT232SQLTransactionScope compares the new scoped API with the
// existing manual savepoint sequence. Both paths use the same snapshot clone
// and rollback semantics; the benchmark isolates callback/API overhead.
func BenchmarkT232SQLTransactionScope(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	b.ReportAllocs()

	b.Run("manual_savepoint", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			transaction, err := BeginSQLTransaction(trie)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('scope:outer', 'keep')"); err != nil {
				b.Fatal(err)
			}
			if err := transaction.Savepoint("manual_scope"); err != nil {
				b.Fatal(err)
			}
			if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('scope:inner', 'discard')"); err != nil {
				b.Fatal(err)
			}
			if err := transaction.RollbackTo("manual_scope"); err != nil {
				b.Fatal(err)
			}
			if err := transaction.ReleaseSavepoint("manual_scope"); err != nil {
				b.Fatal(err)
			}
			if err := transaction.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("scoped_callback", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			transaction, err := BeginSQLTransaction(trie)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('scope:outer', 'keep')"); err != nil {
				b.Fatal(err)
			}
			if err := transaction.Scope(func(transaction *SQLTransaction) error {
				if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('scope:inner', 'discard')"); err != nil {
					return err
				}
				return errT232BenchmarkRollback
			}); !errors.Is(err, errT232BenchmarkRollback) {
				b.Fatal(err)
			}
			if err := transaction.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
