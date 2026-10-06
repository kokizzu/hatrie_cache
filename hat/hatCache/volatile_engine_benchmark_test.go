package hatCache

import "testing"

func BenchmarkTU18BaselineHatTrieBytes(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	value := []byte("volatile benchmark payload")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		trie.UpsertBytes("benchmark-key", value)
		if got := trie.GetBytes("benchmark-key"); len(got) != len(value) {
			b.Fatal("baseline read lost value")
		}
	}
}
