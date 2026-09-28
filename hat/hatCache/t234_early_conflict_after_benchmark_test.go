package hatCache

import (
	"errors"
	"testing"
)

func BenchmarkT234SQLTransactionExecuteAfter(b *testing.B) {
	for _, early := range []bool{false, true} {
		name := "Default"
		if early {
			name = "EarlyConflictCheck"
		}
		b.Run(name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			tx, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{EarlyConflictCheck: early})
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
		})
	}
}

func BenchmarkT234SQLTransactionStaleExecute(b *testing.B) {
	for _, early := range []bool{false, true} {
		name := "Default"
		if early {
			name = "EarlyConflictCheck"
		}
		b.Run(name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			tx, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{EarlyConflictCheck: early})
			if err != nil {
				b.Fatal(err)
			}
			defer tx.Rollback()
			trie.UpsertString("concurrent", "write")

			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				_, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('draft', 'private')")
				if early {
					if !errors.Is(err, ErrSQLTransactionConflict) {
						b.Fatalf("Execute() error = %v, want ErrSQLTransactionConflict", err)
					}
				} else if err != nil {
					b.Fatal(err)
				}
				tx.staged = tx.staged[:0]
			}
		})
	}
}
