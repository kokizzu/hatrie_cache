package hatDataStructure

import (
	"fmt"
	"testing"
)

type tr054BaselineFunctionalValue struct {
	Key int
}

var tr054BaselineFunctionalSink uint64

func BenchmarkTR054BaselineFunctionalSmallUpsert(b *testing.B) {
	for _, size := range []int{4, 16, 32} {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			index, err := NewFunctionalIndex(func(value tr054BaselineFunctionalValue) int { return value.Key }, 256)
			if err != nil {
				b.Fatal(err)
			}
			for id := 0; id < size; id++ {
				if err := index.Upsert(uint64(id), tr054BaselineFunctionalValue{Key: id % 4}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				id := uint64(iteration % size)
				if err := index.Upsert(id, tr054BaselineFunctionalValue{Key: int(id % 4)}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTR054BaselineFunctionalSmallBuild(b *testing.B) {
	for _, size := range []int{4, 16, 32} {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				index, err := NewFunctionalIndex(func(value tr054BaselineFunctionalValue) int { return value.Key }, 256)
				if err != nil {
					b.Fatal(err)
				}
				for id := 0; id < size; id++ {
					if err := index.Upsert(uint64(id), tr054BaselineFunctionalValue{Key: id % 4}); err != nil {
						b.Fatal(err)
					}
				}
				tr054BaselineFunctionalSink += uint64(index.Len())
			}
		})
	}
}

func BenchmarkTR054BaselineFunctionalLargeBuild(b *testing.B) {
	const size = 256
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		index, err := NewFunctionalIndex(func(value tr054BaselineFunctionalValue) int { return value.Key }, size)
		if err != nil {
			b.Fatal(err)
		}
		for id := 0; id < size; id++ {
			if err := index.Upsert(uint64(id), tr054BaselineFunctionalValue{Key: id % 32}); err != nil {
				b.Fatal(err)
			}
		}
		tr054BaselineFunctionalSink += uint64(index.Len())
	}
}

func BenchmarkTR054BaselineFunctionalSmallLookupIDs(b *testing.B) {
	for _, size := range []int{4, 16, 32} {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			index, err := NewFunctionalIndex(func(value tr054BaselineFunctionalValue) int { return value.Key }, 256)
			if err != nil {
				b.Fatal(err)
			}
			for id := 0; id < size; id++ {
				if err := index.Upsert(uint64(id), tr054BaselineFunctionalValue{Key: id % 4}); err != nil {
					b.Fatal(err)
				}
			}
			ids := make([]uint64, 0, size)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				ids = index.LookupIDsInto(1, ids)
				tr054BaselineFunctionalSink += uint64(len(ids))
			}
		})
	}
}
