package hatCache

import (
	"context"
	"path/filepath"
	"testing"
)

func BenchmarkExecuteSQLMutationRetry(b *testing.B) {
	const query = "INSERT INTO cache (key, value) VALUES ('benchmark:dedup', 'value')"

	b.Run("direct-no-journal", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := ExecuteSQLMutation(context.Background(), trie, query, nil, SQLQueryOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("journal-no-token", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
			GroupCommitMaxBatch: 1,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = journal.Close() })
		request, err := CompileSQL(query)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if response := journal.ExecuteCommand(trie, request); !response.OK {
				b.Fatal(response.Message)
			}
		}
	})

	b.Run("idempotent-retry", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
			IdempotencyCapacity: 16,
			GroupCommitMaxBatch: 1,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = journal.Close() })
		if _, err := ExecuteSQLMutationIdempotent(context.Background(), journal, trie, query, nil, SQLQueryOptions{}, "benchmark-1"); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := ExecuteSQLMutationIdempotent(context.Background(), journal, trie, query, nil, SQLQueryOptions{}, "benchmark-1"); err != nil {
				b.Fatal(err)
			}
		}
	})
}
