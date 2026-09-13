package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

var orderedReverseBaselineSink hatDataStructure.OrderedIndexEntry[int, int]

func BenchmarkOrderedIndexDescendingMaterializeBaseline(b *testing.B) {
	index := newOrderedReverseBenchmarkIndex(b)
	entries := make([]hatDataStructure.OrderedIndexEntry[int, int], 0, index.Len())
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		entries = index.SnapshotInto(entries[:0])
		for position := len(entries); position > 0; position-- {
			orderedReverseBaselineSink = entries[position-1]
		}
	}
}

func BenchmarkOrderedIndexDescendingMaterializeAllocBaseline(b *testing.B) {
	index := newOrderedReverseBenchmarkIndex(b)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		entries := index.SnapshotInto(nil)
		for position := len(entries); position > 0; position-- {
			orderedReverseBaselineSink = entries[position-1]
		}
	}
}

func BenchmarkOrderedIndexDescendingReverseIterator(b *testing.B) {
	index := newOrderedReverseBenchmarkIndex(b)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		iterator, ok := index.Last()
		if !ok {
			b.Fatal("Last() returned no iterator")
		}
		for {
			entry, ok, err := iterator.Next()
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				break
			}
			orderedReverseBaselineSink = entry
		}
	}
}

func newOrderedReverseBenchmarkIndex(b testing.TB) *hatDataStructure.OrderedIndex[int, int] {
	b.Helper()
	index, err := hatDataStructure.NewOrderedIndex(
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
		1024,
	)
	if err != nil {
		b.Fatalf("NewOrderedIndex() error = %v", err)
	}
	for value := 0; value < 1024; value++ {
		if err := index.Upsert(uint64(value+1), value); err != nil {
			b.Fatalf("Upsert(%d) error = %v", value, err)
		}
	}
	return index
}
