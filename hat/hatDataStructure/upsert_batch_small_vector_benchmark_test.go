package hatDataStructure

import (
	"fmt"
	"testing"
)

func BenchmarkUpsertBatchSmallVectorC210(b *testing.B) {
	for _, size := range []int{1, 4, 8, 16, 32, 64, 1000} {
		b.Run(fmt.Sprintf("batch_%d", size), func(b *testing.B) {
			keys := make([]string, size)
			for index := range keys {
				keys[index] = fmt.Sprintf("key-%d", index)
			}
			batch := NewUpsertBatch[int](size)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				batch.Reset()
				for index, key := range keys {
					if err := batch.Upsert(key, index+iteration); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkUpsertBatchSmallVectorFreshC210(b *testing.B) {
	for _, size := range []int{1, 4, 16, 32} {
		b.Run(fmt.Sprintf("batch_%d", size), func(b *testing.B) {
			keys := make([]string, size)
			for index := range keys {
				keys[index] = fmt.Sprintf("key-%d", index)
			}
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				batch := NewUpsertBatch[int](size)
				for index, key := range keys {
					if err := batch.Upsert(key, index+iteration); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
