package hatCache

import (
	"context"
	"testing"
)

func BenchmarkExecuteSQLMutationSetString(b *testing.B) {
	benchmarkSQLMutation(b, "INSERT INTO cache (key, value) VALUES ('hot', 'next')")
}

func BenchmarkExecuteSQLMutationOnConflictNothingHit(b *testing.B) {
	benchmarkSQLMutation(b, "INSERT INTO cache (key, value) VALUES ('hot', 'next') ON CONFLICT (key) DO NOTHING")
}

func BenchmarkExecuteSQLMutationOnConflictUpdateHit(b *testing.B) {
	benchmarkSQLMutation(b, "INSERT INTO cache (key, value) VALUES ('hot', 'next') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
}

func benchmarkSQLMutation(b *testing.B, source string) {
	b.Helper()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	trie.UpsertString("hot", "old")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ExecuteSQLMutation(context.Background(), trie, source, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
