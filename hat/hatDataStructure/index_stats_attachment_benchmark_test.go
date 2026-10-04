package hatDataStructure

import "testing"

type indexStatsBenchmarkValue struct {
	Key string
}

func BenchmarkHashIndexExactLookupWithStats(b *testing.B) {
	index, err := NewHashIndex(func(value indexStatsBenchmarkValue) string { return value.Key }, HashIndexOptions{})
	if err != nil {
		b.Fatal(err)
	}
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
		if err := index.Upsert(uint64(i), indexStatsBenchmarkValue{Key: keys[i]}); err != nil {
			b.Fatal(err)
		}
	}
	stats := NewDefaultIndexStats()
	if err := index.AttachStats(stats, indexStatsTestHash); err != nil {
		b.Fatal(err)
	}
	dst := make([]indexStatsBenchmarkValue, 0, 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = index.LookupInto(keys[i%len(keys)], dst)
	}
	if len(dst) == 0 {
		b.Fatal("lookup returned no value")
	}
}

func BenchmarkFunctionalIndexLookupWithStats(b *testing.B) {
	index, err := NewFunctionalIndex(func(value indexStatsBenchmarkValue) string { return value.Key }, 1024)
	if err != nil {
		b.Fatal(err)
	}
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
		if err := index.Upsert(uint64(i), indexStatsBenchmarkValue{Key: keys[i]}); err != nil {
			b.Fatal(err)
		}
	}
	stats := NewDefaultIndexStats()
	if err := index.AttachStats(stats, indexStatsTestHash); err != nil {
		b.Fatal(err)
	}
	dst := make([]indexStatsBenchmarkValue, 0, 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = index.LookupInto(keys[i%len(keys)], dst)
	}
	if len(dst) == 0 {
		b.Fatal("lookup returned no value")
	}
}

func BenchmarkOrderedIndexSeekWithStats(b *testing.B) {
	index, err := NewOrderedIndex(
		func(value indexStatsBenchmarkValue) string { return value.Key },
		func(left, right string) int {
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		},
		1024,
	)
	if err != nil {
		b.Fatal(err)
	}
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
		if err := index.Upsert(uint64(i), indexStatsBenchmarkValue{Key: keys[i]}); err != nil {
			b.Fatal(err)
		}
	}
	stats := NewDefaultIndexStats()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iterator, ok := index.Seek(keys[i%len(keys)])
		if !ok {
			b.Fatal("seek returned no value")
		}
		iterator.Close()
		stats.ObserveLookup(indexStatsTestHash(keys[i%len(keys)]), 1)
	}
}
