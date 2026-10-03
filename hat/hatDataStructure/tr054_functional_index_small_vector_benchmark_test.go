package hatDataStructure

import (
	"fmt"
	"testing"
)

type tr054FunctionalBenchValue struct {
	Key int
}

var tr054FunctionalBenchSink uint64

func BenchmarkTR054FunctionalSmallUpsert(b *testing.B) {
	for _, size := range []int{4, 16, 32} {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			index, err := NewFunctionalIndex(func(value tr054FunctionalBenchValue) int { return value.Key }, 256)
			if err != nil {
				b.Fatal(err)
			}
			for id := 0; id < size; id++ {
				if err := index.Upsert(uint64(id), tr054FunctionalBenchValue{Key: id % 4}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				id := uint64(iteration % size)
				if err := index.Upsert(id, tr054FunctionalBenchValue{Key: int(id % 4)}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTR054FunctionalSmallBuild(b *testing.B) {
	for _, size := range []int{4, 16, 32} {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				index, err := NewFunctionalIndex(func(value tr054FunctionalBenchValue) int { return value.Key }, 256)
				if err != nil {
					b.Fatal(err)
				}
				for id := 0; id < size; id++ {
					if err := index.Upsert(uint64(id), tr054FunctionalBenchValue{Key: id % 4}); err != nil {
						b.Fatal(err)
					}
				}
				tr054FunctionalBenchSink += uint64(index.Len())
			}
		})
	}
}

func BenchmarkTR054FunctionalLargeBuild(b *testing.B) {
	const size = 256
	b.ReportAllocs()
	for range b.N {
		index, err := NewFunctionalIndex(func(value tr054FunctionalBenchValue) int { return value.Key }, size)
		if err != nil {
			b.Fatal(err)
		}
		for id := 0; id < size; id++ {
			if err := index.Upsert(uint64(id), tr054FunctionalBenchValue{Key: id % 32}); err != nil {
				b.Fatal(err)
			}
		}
		tr054FunctionalBenchSink += uint64(index.Len())
	}
}

func BenchmarkTR054FunctionalSmallLookupIDs(b *testing.B) {
	for _, size := range []int{4, 16, 32} {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			index, err := NewFunctionalIndex(func(value tr054FunctionalBenchValue) int { return value.Key }, 256)
			if err != nil {
				b.Fatal(err)
			}
			for id := 0; id < size; id++ {
				if err := index.Upsert(uint64(id), tr054FunctionalBenchValue{Key: id % 4}); err != nil {
					b.Fatal(err)
				}
			}
			ids := make([]uint64, 0, size)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ids = index.LookupIDsInto(1, ids)
				tr054FunctionalBenchSink += uint64(len(ids))
			}
		})
	}
}
