package hatCache

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkCH013MutationAdmissionOverhead(b *testing.B) {
	b.Run("legacy-default", func(b *testing.B) {
		benchmarkCH013MutationAdmission(b, SQLQueryOptions{})
	})
	b.Run("zero-interval-gate", func(b *testing.B) {
		admission, err := hatSql.NewSQLMutationAdmission(hatSql.SQLMutationAdmissionOptions{})
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCH013MutationAdmission(b, SQLQueryOptions{MutationAdmission: admission})
	})
}

func benchmarkCH013MutationAdmission(b *testing.B, options SQLQueryOptions) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := ExecuteSQLMutation(context.Background(), trie, "INSERT INTO cache (key, value) VALUES ('admission:hot', 'value') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", nil, options); err != nil {
			b.Fatal(err)
		}
	}
}
