package hatDataStructure

import "testing"

var sparseBitsetValuesBenchmarkSink []uint64

func BenchmarkSparseBitsetValues(b *testing.B) {
	bitset := sparseBitsetValuesFixture()
	b.Run("values", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			sparseBitsetValuesBenchmarkSink = bitset.Values()
		}
	})
	b.Run("values_into", func(b *testing.B) {
		destination := make([]uint64, 0, bitset.Count())
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			destination = bitset.ValuesInto(destination[:0])
			sparseBitsetValuesBenchmarkSink = destination
		}
	})
}

func sparseBitsetValuesFixture() SparseBitset {
	var bitset SparseBitset
	for value := uint64(0); value < 100000; value++ {
		bitset.Add(value)
	}
	return bitset
}
