package hatDataStructure

import "testing"

var genericMultikeyBenchmarkSink []uint64

func genericMultikeyBenchmarkKeys() []int64 {
	return []int64{1, 5, 9, 13}
}

func genericMultikeyBenchmarkAlternateKeys() []int64 {
	return []int64{2, 6, 10, 14}
}

func genericMultikeyBenchmarkManualSet(byKey map[int64][]uint64, byID map[uint64][]int64, id uint64, keys []int64) {
	normalized := make([]int64, 0, len(keys))
	for _, key := range keys {
		duplicate := false
		for _, existing := range normalized {
			if existing == key {
				duplicate = true
				break
			}
		}
		if !duplicate {
			normalized = append(normalized, key)
		}
	}
	old := byID[id]
	if multikeyBenchmarkKeysEqual(old, normalized) {
		return
	}
	for _, key := range old {
		posting := byKey[key]
		for index, owner := range posting {
			if owner == id {
				copy(posting[index:], posting[index+1:])
				posting = posting[:len(posting)-1]
				break
			}
		}
		if len(posting) == 0 {
			delete(byKey, key)
		} else {
			byKey[key] = posting
		}
	}
	if len(normalized) == 0 {
		delete(byID, id)
		return
	}
	for _, key := range normalized {
		posting := byKey[key]
		position := len(posting)
		for index, owner := range posting {
			if owner > id {
				position = index
				break
			}
		}
		posting = append(posting, 0)
		copy(posting[position+1:], posting[position:])
		posting[position] = id
		byKey[key] = posting
	}
	byID[id] = normalized
}

func genericMultikeyBenchmarkManualLookup(byKey map[int64][]uint64, key int64, dst []uint64) []uint64 {
	dst = dst[:0]
	return append(dst, byKey[key]...)
}

func genericMultikeyBenchmarkIndex(b *testing.B) *MultikeyIndex[int64] {
	b.Helper()
	return NewMultikeyIndex[int64](MultikeyIndexOptions{MaxKeysPerItem: 8, MaxItems: 1024})
}

func genericMultikeyBenchmarkManualFixture() (map[int64][]uint64, map[uint64][]int64) {
	byKey := make(map[int64][]uint64, 4)
	byID := make(map[uint64][]int64, 1024)
	keys := genericMultikeyBenchmarkKeys()
	for id := uint64(1); id <= 1024; id++ {
		genericMultikeyBenchmarkManualSet(byKey, byID, id, keys)
	}
	return byKey, byID
}

func BenchmarkMultikeyIndexManualSet(b *testing.B) {
	byKey, byID := genericMultikeyBenchmarkManualFixture()
	keys := genericMultikeyBenchmarkKeys()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		genericMultikeyBenchmarkManualSet(byKey, byID, uint64(iteration%1024+1), keys)
	}
}

func BenchmarkMultikeyIndexTypedSet(b *testing.B) {
	index := genericMultikeyBenchmarkIndex(b)
	keys := genericMultikeyBenchmarkKeys()
	for id := uint64(1); id <= 1024; id++ {
		if err := index.Set(id, keys); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := index.Set(uint64(iteration%1024+1), keys); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMultikeyIndexManualChangedSet(b *testing.B) {
	byKey, byID := genericMultikeyBenchmarkManualFixture()
	keys := genericMultikeyBenchmarkKeys()
	alternate := genericMultikeyBenchmarkAlternateKeys()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if iteration/1024%2 == 0 {
			genericMultikeyBenchmarkManualSet(byKey, byID, uint64(iteration%1024+1), alternate)
		} else {
			genericMultikeyBenchmarkManualSet(byKey, byID, uint64(iteration%1024+1), keys)
		}
	}
}

func BenchmarkMultikeyIndexTypedChangedSet(b *testing.B) {
	index := genericMultikeyBenchmarkIndex(b)
	keys := genericMultikeyBenchmarkKeys()
	alternate := genericMultikeyBenchmarkAlternateKeys()
	for id := uint64(1); id <= 1024; id++ {
		if err := index.Set(id, keys); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		set := keys
		if iteration/1024%2 == 0 {
			set = alternate
		}
		if err := index.Set(uint64(iteration%1024+1), set); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMultikeyIndexManualLookup(b *testing.B) {
	byKey, _ := genericMultikeyBenchmarkManualFixture()
	dst := make([]uint64, 0, 1024)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		dst = genericMultikeyBenchmarkManualLookup(byKey, 1, dst)
		genericMultikeyBenchmarkSink = dst
	}
}

func BenchmarkMultikeyIndexTypedLookup(b *testing.B) {
	index := genericMultikeyBenchmarkIndex(b)
	keys := genericMultikeyBenchmarkKeys()
	for id := uint64(1); id <= 1024; id++ {
		if err := index.Set(id, keys); err != nil {
			b.Fatal(err)
		}
	}
	dst := make([]uint64, 0, 1024)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		dst = index.Lookup(1, dst)
		genericMultikeyBenchmarkSink = dst
	}
}

func multikeyBenchmarkKeysEqual(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
