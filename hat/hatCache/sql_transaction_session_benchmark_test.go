package hatCache

import "testing"

func BenchmarkSQLTransactionSession(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	options := SQLTransactionOptions{Isolation: SQLTransactionIsolationSnapshot, ReadOnly: true}
	b.ReportAllocs()
	b.Run("direct", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			transaction, err := BeginSQLTransactionWithOptions(trie, options)
			if err != nil {
				b.Fatal(err)
			}
			if err := transaction.Rollback(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("session", func(b *testing.B) {
		session, err := NewSQLTransactionSessionWithOptions(trie, options)
		if err != nil {
			b.Fatal(err)
		}
		for index := 0; index < b.N; index++ {
			transaction, err := session.Begin()
			if err != nil {
				b.Fatal(err)
			}
			if err := transaction.Rollback(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
