package hatDataStructure

import "testing"

type tr053BaselineHashValue struct {
	Key     int
	Payload int
}

func newTR053BaselineHashIndex(b *testing.B, size int) *HashIndex[tr053BaselineHashValue, int] {
	b.Helper()
	index, err := NewHashIndex(func(value tr053BaselineHashValue) int { return value.Key }, HashIndexOptions{Unique: true, Capacity: size})
	if err != nil {
		b.Fatal(err)
	}
	for id := 0; id < size; id++ {
		if err := index.Upsert(uint64(id), tr053BaselineHashValue{Key: id, Payload: id}); err != nil {
			b.Fatal(err)
		}
	}
	return index
}

func BenchmarkTR053BaselineUniqueSmallUpsert(b *testing.B) {
	index := newTR053BaselineHashIndex(b, 16)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		id := uint64(iteration & 15)
		if err := index.Upsert(id, tr053BaselineHashValue{Key: int(id), Payload: iteration}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR053BaselineUniqueLargeUpsert(b *testing.B) {
	index := newTR053BaselineHashIndex(b, 512)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		id := uint64(iteration & 511)
		if err := index.Upsert(id, tr053BaselineHashValue{Key: int(id), Payload: iteration}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR053BaselineUniqueSmallBuild(b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		_ = newTR053BaselineHashIndex(b, 16)
	}
}
