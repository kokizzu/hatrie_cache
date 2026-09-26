package hatSql

import "testing"

func benchmarkMZ038DistinctSmallBatchFixture() []DifferentialRow {
	updates := make([]DifferentialRow, 0, 8)
	for index, key := range []string{"a", "b", "c", "d"} {
		row := Row{"id": int64(index), "value": key}
		updates = append(updates,
			DifferentialRow{Key: key, Time: uint64(index*2 + 1), Diff: 1, Row: row},
			DifferentialRow{Key: key, Time: uint64(index*2 + 2), Diff: -1, Row: row},
		)
	}
	return updates
}

func BenchmarkMZ038IncrementalDistinctSmallBatch(b *testing.B) {
	updates := benchmarkMZ038DistinctSmallBatchFixture()
	b.SetBytes(int64(len(updates)))
	b.Run("GenericMap", func(b *testing.B) {
		b.ReportAllocs()
		distinct := NewIncrementalDistinct()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			if _, err := distinct.applyGenericBatch(updates); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("SmallSlice", func(b *testing.B) {
		b.ReportAllocs()
		distinct := NewIncrementalDistinct()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			if _, err := distinct.applySmallBatch(updates); err != nil {
				b.Fatal(err)
			}
		}
	})
}
