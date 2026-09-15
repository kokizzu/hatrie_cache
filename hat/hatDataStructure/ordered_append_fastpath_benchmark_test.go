package hatDataStructure

import (
	"strconv"
	"testing"
)

func BenchmarkC222OrderedIndexMonotonicBuild(b *testing.B) {
	for _, size := range []int{64, 1024, 10000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				index, err := NewOrderedIndex(func(value int) int { return value }, orderedIndexC220Compare, size)
				if err != nil {
					b.Fatal(err)
				}
				for id := 1; id <= size; id++ {
					if err := index.Upsert(uint64(id), id); err != nil {
						b.Fatal(err)
					}
				}
				orderedIndexBenchmarkSink = index.entries[len(index.entries)-1].Value
			}
		})
	}
}
