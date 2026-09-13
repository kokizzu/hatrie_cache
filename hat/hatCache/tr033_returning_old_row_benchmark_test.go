package hatCache

import (
	"context"
	"testing"
)

func BenchmarkTR033SQLReturningBeforeAfter(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("profile:1", "Ada")
	const source = "UPDATE cache SET value = 'Grace' WHERE key = 'profile:1' RETURNING key, value, exists"

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLMutation(context.Background(), trie, source, nil, SQLQueryOptions{})
		if err != nil || result.Affected != 1 {
			b.Fatalf("ExecuteSQLMutation() = %#v, %v", result, err)
		}
	}
}
