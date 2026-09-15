package hatDataStructure

import (
	"fmt"
	"testing"
)

func BenchmarkSecondaryIndexPostingBuildC204(b *testing.B) {
	const rowCount = 10000
	for _, distinct := range []int{rowCount, 1000, 100, 10} {
		b.Run(fmt.Sprintf("functional-distinct-%d", distinct), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				index, err := NewFunctionalIndex(func(value int) int { return value % distinct }, rowCount)
				if err != nil {
					b.Fatal(err)
				}
				for value := 0; value < rowCount; value++ {
					if err := index.Upsert(uint64(value+1), value); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
		b.Run(fmt.Sprintf("hash-distinct-%d", distinct), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				index, err := NewHashIndex(func(value int) int { return value % distinct }, HashIndexOptions{Capacity: rowCount})
				if err != nil {
					b.Fatal(err)
				}
				for value := 0; value < rowCount; value++ {
					if err := index.Upsert(uint64(value+1), value); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

var compactPostingC216Sink uint64

func BenchmarkC216PostingValues(b *testing.B) {
	for _, size := range []int{1, 2, 10, 100} {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			list := newU64PostingList(0)
			for id := 1; id < size; id++ {
				list = list.insertSorted(uint64(id))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				values := list.values(nil)
				compactPostingC216Sink += uint64(len(values))
			}
		})
	}
}

func BenchmarkC216FunctionalLookupIDs(b *testing.B) {
	const (
		rowCount  = 10000
		distinct  = 100
		targetKey = 42
	)
	index, err := NewFunctionalIndex(func(value int) int { return value % distinct }, rowCount)
	if err != nil {
		b.Fatal(err)
	}
	for value := 0; value < rowCount; value++ {
		if err := index.Upsert(uint64(value+1), value); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("fresh", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			values := index.LookupIDs(targetKey)
			compactPostingC216Sink += uint64(len(values))
		}
	})
	b.Run("reusable", func(b *testing.B) {
		values := make([]uint64, 0, rowCount/distinct)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			values = index.LookupIDsInto(targetKey, values)
			compactPostingC216Sink += uint64(len(values))
		}
	})
}

var compactPostingC217Sink int

func BenchmarkC217PostingBuild(b *testing.B) {
	for _, size := range []int{256, 4096, 10000} {
		b.Run(fmt.Sprintf("monotonic-size-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				list := newU64PostingList(0)
				for id := 1; id < size; id++ {
					list = list.insertSorted(uint64(id))
				}
				compactPostingC217Sink = len(list.values(nil))
			}
		})
	}
}

func BenchmarkC217HashIndexBuild(b *testing.B) {
	const (
		rowCount = 10000
		distinct = 100
	)
	b.ReportAllocs()
	for range b.N {
		index, err := NewHashIndex(func(value int) int { return value % distinct }, HashIndexOptions{Capacity: rowCount})
		if err != nil {
			b.Fatal(err)
		}
		for value := 0; value < rowCount; value++ {
			if err := index.Upsert(uint64(value+1), value); err != nil {
				b.Fatal(err)
			}
		}
	}
}
