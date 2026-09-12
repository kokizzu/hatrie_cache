package hatCache

import "testing"

func BenchmarkTT046MemoryBackingBaseline(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	for index := 0; index < 256; index++ {
		trie.UpsertString("tt046:baseline:"+string(rune(index)), "value")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		trie.mu.RLock()
		bytes := trie.memoryBackingBytesLocked()
		trie.mu.RUnlock()
		if bytes == 0 {
			b.Fatal("baseline accounting returned zero bytes")
		}
	}
}
