package hatDataStructure

import "testing"

var hashIndexBenchmarkSink int

func BenchmarkHashIndexExactLookup(b *testing.B) {
	const size = 10000
	unique, err := NewHashIndex(func(value int) int { return value }, HashIndexOptions{Unique: true, Capacity: size})
	if err != nil {
		b.Fatal(err)
	}
	functional, err := NewFunctionalIndex(func(value int) int { return value }, size)
	if err != nil {
		b.Fatal(err)
	}
	for value := 0; value < size; value++ {
		if err := unique.Upsert(uint64(value+1), value); err != nil {
			b.Fatal(err)
		}
		if err := functional.Upsert(uint64(value+1), value); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("hash-unique-lookup-one", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			entry, ok := unique.LookupOne(iteration % size)
			if !ok {
				b.Fatal("LookupOne() = false")
			}
			hashIndexBenchmarkSink = entry.Value
		}
	})
	b.Run("functional-reusable-values", func(b *testing.B) {
		scratch := make([]int, 0, 1)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			scratch = functional.LookupInto(iteration%size, scratch)
			if len(scratch) != 1 {
				b.Fatal("LookupInto() returned no value")
			}
			hashIndexBenchmarkSink = scratch[0]
		}
	})
	b.Run("hash-nonunique-reusable-ids", func(b *testing.B) {
		nonUnique, err := NewHashIndex(func(value int) int { return value % 100 }, HashIndexOptions{Capacity: size})
		if err != nil {
			b.Fatal(err)
		}
		for value := 0; value < size; value++ {
			if err := nonUnique.Upsert(uint64(value+1), value); err != nil {
				b.Fatal(err)
			}
		}
		scratch := make([]uint64, 0, size/100)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			scratch = nonUnique.LookupIDsInto(iteration%100, scratch)
			hashIndexBenchmarkSink = len(scratch)
		}
	})
	b.Run("functional-reusable-ids", func(b *testing.B) {
		nonUnique, err := NewFunctionalIndex(func(value int) int { return value % 100 }, size)
		if err != nil {
			b.Fatal(err)
		}
		for value := 0; value < size; value++ {
			if err := nonUnique.Upsert(uint64(value+1), value); err != nil {
				b.Fatal(err)
			}
		}
		scratch := make([]uint64, 0, size/100)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			scratch = nonUnique.LookupIDsInto(iteration%100, scratch)
			hashIndexBenchmarkSink = len(scratch)
		}
	})
}

func BenchmarkHashIndexBuild(b *testing.B) {
	const size = 10000
	b.Run("hash-unique", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			index, err := NewHashIndex(func(value int) int { return value }, HashIndexOptions{Unique: true, Capacity: size})
			if err != nil {
				b.Fatal(err)
			}
			for value := 0; value < size; value++ {
				if err := index.Upsert(uint64(value+1), value); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("functional", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			index, err := NewFunctionalIndex(func(value int) int { return value }, size)
			if err != nil {
				b.Fatal(err)
			}
			for value := 0; value < size; value++ {
				if err := index.Upsert(uint64(value+1), value); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
