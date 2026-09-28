package hatCache

import "testing"

func BenchmarkT234SQLTransactionExecute(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	tx, err := BeginSQLTransaction(trie)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('draft', 'private')"); err != nil {
			b.Fatal(err)
		}
		tx.staged = tx.staged[:0]
	}
}
