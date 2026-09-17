package hatDataStructure

import "testing"

var tt020OrderedIndexBenchmarkSink int

func newTT020OrderedIndexBenchmarkFixture(b *testing.B) *OrderedIndex[int, int] {
	b.Helper()
	index, err := NewOrderedIndex(
		func(value int) int { return value },
		func(left, right int) int {
			switch {
			case left < right:
				return -1
			case left > right:
				return 1
			default:
				return 0
			}
		},
		100_000,
	)
	if err != nil {
		b.Fatal(err)
	}
	for value := 0; value < 100_000; value++ {
		if err := index.Upsert(uint64(value), value); err != nil {
			b.Fatal(err)
		}
	}
	return index
}

// BenchmarkTT020OrderedIndexSeekFilter is the pre-range baseline: callers
// seek to the lower key and inspect every subsequent entry until the upper
// bound is reached.
func BenchmarkTT020OrderedIndexSeekFilter(b *testing.B) {
	index := newTT020OrderedIndexBenchmarkFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		iterator, ok := index.Seek(40_000)
		if !ok {
			b.Fatal("Seek() did not find the lower bound")
		}
		count := 0
		for {
			entry, next, err := iterator.Next()
			if err != nil {
				b.Fatal(err)
			}
			if !next {
				break
			}
			if entry.Key > 60_000 {
				break
			}
			count++
		}
		iterator.Close()
		tt020OrderedIndexBenchmarkSink = count
	}
}

func BenchmarkTT020OrderedIndexRange(b *testing.B) {
	index := newTT020OrderedIndexBenchmarkFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		iterator, ok := index.Range(40_000, 60_000)
		if !ok {
			b.Fatal("Range() did not find the requested bounds")
		}
		count := 0
		for {
			_, next, err := iterator.Next()
			if err != nil {
				b.Fatal(err)
			}
			if !next {
				break
			}
			count++
		}
		tt020OrderedIndexBenchmarkSink = count
	}
}
