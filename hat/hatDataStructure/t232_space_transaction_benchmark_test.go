package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

var t232BenchmarkKeys = []string{
	"key-0",
	"key-1",
	"key-2",
	"key-3",
	"key-4",
	"key-5",
	"key-6",
	"key-7",
}

func BenchmarkT232SpacePutBaseline(b *testing.B) {
	space, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
		Memtx: hatDataStructure.MemtxSpaceOptions{MaxRecords: 16, MaxValueBytes: 64},
	})
	if err != nil {
		b.Fatal(err)
	}
	value := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := space.Put("key", value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT232SequentialBatchBaseline(b *testing.B) {
	space, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
		Memtx: hatDataStructure.MemtxSpaceOptions{MaxRecords: len(t232BenchmarkKeys), MaxValueBytes: 64},
	})
	if err != nil {
		b.Fatal(err)
	}
	value := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		for _, key := range t232BenchmarkKeys {
			if err := space.Put(key, value); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkT232SpaceTransaction(b *testing.B) {
	space, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
		Memtx: hatDataStructure.MemtxSpaceOptions{MaxRecords: 16, MaxValueBytes: 64},
	})
	if err != nil {
		b.Fatal(err)
	}
	value := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tx, err := space.BeginTransaction()
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.Put("key", value); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT232TransactionalBatch(b *testing.B) {
	space, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
		Memtx: hatDataStructure.MemtxSpaceOptions{MaxRecords: len(t232BenchmarkKeys), MaxValueBytes: 64},
	})
	if err != nil {
		b.Fatal(err)
	}
	value := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tx, err := space.BeginTransaction()
		if err != nil {
			b.Fatal(err)
		}
		for _, key := range t232BenchmarkKeys {
			if err := tx.Put(key, value); err != nil {
				b.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
