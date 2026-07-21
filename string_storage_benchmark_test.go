package hatriecache

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkStringStorageLayout100k(b *testing.B) {
	keys := stringStorageBenchmarkInt("HATRIE_STRING_STORAGE_KEYS", 100000)
	keyValues := make([]string, keys)
	values := make([]string, keys)
	for index := 0; index < keys; index++ {
		keyValues[index] = fmt.Sprintf("string-layout:%09d", index)
		prefix := fmt.Sprintf("%09d:", index)
		values[index] = prefix + strings.Repeat("x", 256-len(prefix))
	}

	b.Run("Insert256", func(b *testing.B) {
		var retained uint64
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			trie := CreateHatTrie()
			b.StartTimer()
			for index := range keyValues {
				trie.UpsertString(keyValues[index], values[index])
			}
			b.StopTimer()
			runtime.GC()
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			if after.HeapAlloc > before.HeapAlloc {
				retained += after.HeapAlloc - before.HeapAlloc
			}
			if trie.Size() != keys || trie.GetString(keyValues[keys-1]) != values[keys-1] {
				trie.Destroy()
				b.Fatal("inserted string state mismatch")
			}
			trie.Destroy()
			b.StartTimer()
		}
		b.StopTimer()
		b.ReportMetric(float64(keys), "keys/op")
		b.ReportMetric(float64(retained)/float64(b.N*keys), "retained_B/key")
	})

	b.Run("Replace256", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			trie := CreateHatTrie()
			for index := range keyValues {
				trie.UpsertString(keyValues[index], "old")
			}
			b.StartTimer()
			for index := range keyValues {
				trie.UpsertString(keyValues[index], values[index])
			}
			b.StopTimer()
			if trie.GetString(keyValues[keys-1]) != values[keys-1] {
				trie.Destroy()
				b.Fatal("replaced string state mismatch")
			}
			trie.Destroy()
			b.StartTimer()
		}
		b.StopTimer()
		b.ReportMetric(float64(keys), "keys/op")
	})
}

func stringStorageBenchmarkInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
