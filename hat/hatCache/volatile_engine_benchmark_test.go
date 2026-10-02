package hatCache

import (
	"bytes"
	"testing"
)

func BenchmarkVolatileEngineReadWrite(b *testing.B) {
	value := bytes.Repeat([]byte("x"), DiskBytesThreshold+1)
	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	b.Run("disk_backed", func(b *testing.B) {
		benchmarkVolatileEngineReadWrite(b, CreateHatTrie)
	})
	b.Run("volatile", func(b *testing.B) {
		benchmarkVolatileEngineReadWrite(b, CreateVolatileHatTrie)
	})
}

func benchmarkVolatileEngineReadWrite(b *testing.B, create func() *HatTrie) {
	value := bytes.Repeat([]byte("x"), DiskBytesThreshold+1)
	trie := create()
	defer trie.Destroy()
	trie.UpsertBytes("hot", []byte("warmup"))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		trie.UpsertBytes("hot", value)
		if got := trie.GetBytes("hot"); !bytes.Equal(got, value) {
			b.Fatalf("GetString() = %q, want value", got)
		}
	}
}
