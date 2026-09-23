package hatDataStructure

import (
	"sync"
	"testing"
)

func BenchmarkT215StorageSpaceBaseline(b *testing.B) {
	b.Run("MapMemtxPutGet", func(b *testing.B) {
		values := make(map[string][]byte, 1024)
		value := []byte("value")
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			key := benchmarkT215SpaceKey(index)
			values[key] = append([]byte(nil), value...)
			_ = values[key]
		}
	})
	b.Run("LockedMapMemtxPutGet", func(b *testing.B) {
		values := make(map[string][]byte, 1024)
		var mu sync.RWMutex
		value := []byte("value")
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			key := benchmarkT215SpaceKey(index)
			mu.Lock()
			values[key] = append([]byte(nil), value...)
			mu.Unlock()
			mu.RLock()
			_ = append([]byte(nil), values[key]...)
			mu.RUnlock()
		}
	})
	b.Run("LSMVinylPutGet", func(b *testing.B) {
		table, err := NewLSMTable(LSMTableOptions{
			MemtableMaxRecords: 1 << 20,
			RunOptions:         SealedUpsertRunOptions{MaxRecords: 1 << 20},
		})
		if err != nil {
			b.Fatal(err)
		}
		value := []byte("value")
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			key := benchmarkT215SpaceKey(index)
			if err := table.Put(key, value); err != nil {
				b.Fatal(err)
			}
			_, _ = table.Get(key)
		}
	})
}

func benchmarkT215SpaceKey(index int) string {
	return "key-" + string(rune('a'+index&31))
}
