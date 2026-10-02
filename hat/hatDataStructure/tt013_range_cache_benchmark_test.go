package hatDataStructure

import "testing"

var tt013RangeCacheBenchmarkSink uint64

func BenchmarkTT013RangeScanBaseline(b *testing.B) {
	index := newTT013BenchmarkIndex(b)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		iterator, ok := index.Range(4500, 5499)
		if !ok {
			b.Fatal("Range() = false")
		}
		var sum uint64
		for {
			entry, next, err := iterator.Next()
			if err != nil {
				b.Fatal(err)
			}
			if !next {
				break
			}
			sum += uint64(entry.Value)
		}
		tt013RangeCacheBenchmarkSink = sum
	}
}

func BenchmarkTT013RangeTupleCacheHit(b *testing.B) {
	index := newTT013BenchmarkIndex(b)
	cache, err := NewOrderedIndexRangeCache(index, 8)
	if err != nil {
		b.Fatal(err)
	}
	destination, found := cache.Range(4500, 5499)
	if !found {
		b.Fatal("initial Range() = false")
	}
	b.ReportMetric(float64(len(destination)), "cached_values")
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		destination, found = cache.RangeInto(4500, 5499, destination[:0])
		if !found {
			b.Fatal("RangeInto() = false")
		}
		var sum uint64
		for _, entry := range destination {
			sum += uint64(entry.Value)
		}
		tt013RangeCacheBenchmarkSink = sum
	}
}

func BenchmarkTT013RangeTupleCacheColdMiss(b *testing.B) {
	index := newTT013BenchmarkIndex(b)
	cache, err := NewOrderedIndexRangeCache(index, 1)
	if err != nil {
		b.Fatal(err)
	}
	destination := make([]OrderedIndexEntry[int, int], 0, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		start := (iteration % 8) * 1000
		var found bool
		destination, found = cache.RangeInto(start, start+999, destination[:0])
		if !found {
			b.Fatal("RangeInto() = false")
		}
		var sum uint64
		for _, entry := range destination {
			sum += uint64(entry.Value)
		}
		tt013RangeCacheBenchmarkSink = sum
	}
}

func newTT013BenchmarkIndex(b *testing.B) *OrderedIndex[int, int] {
	b.Helper()
	index, err := NewOrderedIndex(
		func(value int) int { return value },
		func(left, right int) int {
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		},
		10000,
	)
	if err != nil {
		b.Fatal(err)
	}
	for value := 0; value < 10000; value++ {
		if err := index.Upsert(uint64(value+1), value); err != nil {
			b.Fatal(err)
		}
	}
	return index
}
