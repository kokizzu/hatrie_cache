package hatCache

import (
	"context"
	"errors"
	"testing"
)

// BenchmarkT233SQLTransactionYield measures the explicit cooperative safe
// point separately from transaction snapshot creation and commit.
func BenchmarkT233SQLTransactionYield(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	b.ReportAllocs()
	b.Run("ready", func(b *testing.B) {
		transaction, err := BeginSQLTransaction(trie)
		if err != nil {
			b.Fatal(err)
		}
		defer transaction.Rollback()
		ctx := context.Background()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := transaction.Yield(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("canceled", func(b *testing.B) {
		transaction, err := BeginSQLTransaction(trie)
		if err != nil {
			b.Fatal(err)
		}
		defer transaction.Rollback()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := transaction.Yield(ctx); !errors.Is(err, context.Canceled) {
				b.Fatal(err)
			}
		}
	})
}
