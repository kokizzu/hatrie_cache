package hatDataStructure

import "testing"

var tupleFieldUpdateBenchmarkSink TupleFieldOffsetCache

func BenchmarkTupleFieldUpdate(b *testing.B) {
	const fieldCount = 512
	fields := make([][]byte, fieldCount)
	for index := range fields {
		fields[index] = make([]byte, 8)
	}
	cache, err := NewPackedTuple(fields)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("apply-fixed-width", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			updated, err := cache.ApplyUpdates([]TupleFieldUpdate{{Index: fieldCount / 2, Kind: TupleFieldAddInt64, Delta: int64(iteration + 1)}})
			if err != nil {
				b.Fatal(err)
			}
			tupleFieldUpdateBenchmarkSink = updated
		}
	})
	b.Run("repack-naive", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			working := make([][]byte, len(fields))
			copy(working, fields)
			working[fieldCount/2] = append([]byte(nil), fields[fieldCount/2]...)
			working[fieldCount/2][0] = byte(iteration)
			updated, err := NewPackedTuple(working)
			if err != nil {
				b.Fatal(err)
			}
			tupleFieldUpdateBenchmarkSink = updated
		}
	})
}
