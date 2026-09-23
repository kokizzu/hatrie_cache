package hatDataStructure

import "testing"

func BenchmarkT215StorageSpace(b *testing.B) {
	b.Run("MemtxPutGet", func(b *testing.B) {
		space, err := NewSpace(SpaceOptions{Memtx: MemtxSpaceOptions{MaxRecords: 1024}})
		if err != nil {
			b.Fatal(err)
		}
		value := []byte("value")
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			key := benchmarkT215SpaceKey(index)
			if err := space.Put(key, value); err != nil {
				b.Fatal(err)
			}
			_, _ = space.Get(key)
		}
	})
	b.Run("VinylPutGet", func(b *testing.B) {
		space, err := NewSpace(SpaceOptions{
			Engine: SpaceEngineVinyl,
			Vinyl: LSMTableOptions{
				MemtableMaxRecords: 1 << 20,
				RunOptions:         SealedUpsertRunOptions{MaxRecords: 1 << 20},
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		value := []byte("value")
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			key := benchmarkT215SpaceKey(index)
			if err := space.Put(key, value); err != nil {
				b.Fatal(err)
			}
			_, _ = space.Get(key)
		}
	})
}
