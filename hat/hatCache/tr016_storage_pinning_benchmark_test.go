package hatCache

import "testing"

func BenchmarkStorageKeyPinningCandidateAdmission(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	entry := Entry{Key: "key"}
	options := LevelDBSpillOptions{MinValueBytes: 1}

	bench := func(b *testing.B, pins map[string]struct{}) {
		trie.storagePinnedKeys = pins
		candidates := make([]levelDBSpillCandidate, 0, 1)
		result := &LevelDBSpillResult{}
		admitted := 0
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			candidates = candidates[:0]
			*result = LevelDBSpillResult{}
			trie.appendLevelDBSpillCandidateLocked(&candidates, entry, 128, options, result, true)
			admitted += len(candidates)
		}
		b.StopTimer()
		b.ReportMetric(float64(admitted)/float64(b.N), "admitted/op")
	}

	b.Run("default-off", func(b *testing.B) {
		bench(b, nil)
	})
	b.Run("active-unrelated-pin", func(b *testing.B) {
		bench(b, map[string]struct{}{"other": {}})
	})
	b.Run("active-matching-pin", func(b *testing.B) {
		bench(b, map[string]struct{}{"key": {}})
	})
}
