package hatDataStructure

import "testing"

type tr053HashBenchmarkValue struct {
	Key     int
	Payload int
}

func newTR053BenchmarkIndex(b *testing.B, size int) *HashIndex[tr053HashBenchmarkValue, int] {
	b.Helper()
	index, err := NewHashIndex(func(value tr053HashBenchmarkValue) int { return value.Key }, HashIndexOptions{Unique: true, Capacity: size})
	if err != nil {
		b.Fatal(err)
	}
	for id := 0; id < size; id++ {
		if err := index.Upsert(uint64(id), tr053HashBenchmarkValue{Key: id, Payload: id}); err != nil {
			b.Fatal(err)
		}
	}
	return index
}

func BenchmarkTR053UniqueSmallUpsert(b *testing.B) {
	index := newTR053BenchmarkIndex(b, 16)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		id := uint64(iteration & 15)
		if err := index.Upsert(id, tr053HashBenchmarkValue{Key: int(id), Payload: iteration}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR053UniqueLargeUpsert(b *testing.B) {
	index := newTR053BenchmarkIndex(b, 512)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		id := uint64(iteration & 511)
		if err := index.Upsert(id, tr053HashBenchmarkValue{Key: int(id), Payload: iteration}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR053UniqueSmallBuild(b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		_ = newTR053BenchmarkIndex(b, 16)
	}
}
