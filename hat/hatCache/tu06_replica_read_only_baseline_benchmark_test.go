package hatCache

import "testing"

func BenchmarkTU06BaselineUpsertStringChecked(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := trie.UpsertStringChecked("tu06-key", "value"); err != nil {
			b.Fatal(err)
		}
	}
}
