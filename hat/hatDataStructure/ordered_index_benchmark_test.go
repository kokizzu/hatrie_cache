package hatDataStructure

import (
	"errors"
	"sort"
	"testing"
)

var orderedIndexBenchmarkSink int

func BenchmarkOrderedIndexTraversal(b *testing.B) {
	index, err := NewOrderedIndex(func(value int) int { return value }, func(left, right int) int {
		if left < right {
			return -1
		}
		if left > right {
			return 1
		}
		return 0
	}, 10000)
	if err != nil {
		b.Fatal(err)
	}
	for value := 0; value < 10000; value++ {
		if err := index.Upsert(uint64(value+1), value); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("iterator", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			iterator, ok := index.First()
			if !ok {
				b.Fatal("First() = false")
			}
			sum := 0
			for {
				entry, next, err := iterator.Next()
				if err != nil {
					b.Fatal(err)
				}
				if !next {
					break
				}
				sum += entry.Value
			}
			orderedIndexBenchmarkSink = sum
		}
	})
	b.Run("snapshot-reuse", func(b *testing.B) {
		scratch := make([]OrderedIndexEntry[int, int], 0, index.Len())
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			snapshot := index.SnapshotInto(scratch)
			sum := 0
			for _, entry := range snapshot {
				sum += entry.Value
			}
			orderedIndexBenchmarkSink = sum
			scratch = snapshot[:0]
		}
	})
	b.Run("snapshot-alloc", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			snapshot := index.SnapshotInto(nil)
			sum := 0
			for _, entry := range snapshot {
				sum += entry.Value
			}
			orderedIndexBenchmarkSink = sum
		}
	})
}

func BenchmarkOrderedIndexMutation(b *testing.B) {
	b.Run("current", func(b *testing.B) {
		index, err := NewOrderedIndex(func(value int) int { return value }, func(left, right int) int {
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		}, 10000)
		if err != nil {
			b.Fatal(err)
		}
		for value := 0; value < 10000; value++ {
			if err := index.Upsert(uint64(value+1), value); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			id := uint64(iteration%10000 + 1)
			if err := index.Upsert(id, iteration%10000); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("flat-in-place-baseline", func(b *testing.B) {
		index := newOrderedIndexFlatMutationBaseline(10000)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			index.Upsert(uint64(iteration%10000+1), iteration%10000)
		}
	})
	b.Run("live-iterator-copy-on-write", func(b *testing.B) {
		index, err := NewOrderedIndex(func(value int) int { return value }, func(left, right int) int {
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		}, 10000)
		if err != nil {
			b.Fatal(err)
		}
		for value := 0; value < 10000; value++ {
			if err := index.Upsert(uint64(value+1), value); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			iterator, ok := index.First()
			if !ok {
				b.Fatal("First() = false")
			}
			if err := index.Upsert(uint64(iteration%10000+1), iteration%10000); err != nil {
				b.Fatal(err)
			}
			if _, next, err := iterator.Next(); !errors.Is(err, ErrOrderedIndexIteratorInvalidated) || next {
				b.Fatalf("invalidated Next() = next=%v err=%v", next, err)
			}
		}
	})
}

type orderedIndexFlatMutationBaseline struct {
	entries   []OrderedIndexEntry[int, int]
	positions map[uint64]int
}

func newOrderedIndexFlatMutationBaseline(size int) *orderedIndexFlatMutationBaseline {
	index := &orderedIndexFlatMutationBaseline{
		entries:   make([]OrderedIndexEntry[int, int], 0, size),
		positions: make(map[uint64]int, size),
	}
	for value := 0; value < size; value++ {
		index.entries = append(index.entries, OrderedIndexEntry[int, int]{ID: uint64(value + 1), Key: value, Value: value})
		index.positions[uint64(value+1)] = value
	}
	return index
}

func (index *orderedIndexFlatMutationBaseline) Upsert(id uint64, value int) {
	if position, exists := index.positions[id]; exists {
		copy(index.entries[position:], index.entries[position+1:])
		index.entries = index.entries[:len(index.entries)-1]
		delete(index.positions, id)
		for current := position; current < len(index.entries); current++ {
			index.positions[index.entries[current].ID] = current
		}
	}
	position := sort.Search(len(index.entries), func(position int) bool {
		return index.entries[position].Key >= value
	})
	index.entries = append(index.entries, OrderedIndexEntry[int, int]{})
	copy(index.entries[position+1:], index.entries[position:])
	index.entries[position] = OrderedIndexEntry[int, int]{ID: id, Key: value, Value: value}
	for current := position; current < len(index.entries); current++ {
		index.positions[index.entries[current].ID] = current
	}
}
