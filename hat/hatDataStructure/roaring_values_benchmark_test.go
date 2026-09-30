package hatDataStructure

import "testing"

func BenchmarkRoaringBitmapValues(b *testing.B) {
	bitmap := roaringBitmapValuesFixture()
	b.ReportAllocs()
	b.Run("values", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			roaringBitmapValuesSink = bitmap.Values()
		}
	})
	b.Run("values_into", func(b *testing.B) {
		destination := make([]uint32, 0, int(bitmap.Count()))
		for index := 0; index < b.N; index++ {
			destination = bitmap.ValuesInto(destination)
			roaringBitmapValuesIntoSink = destination
		}
	})
}

var roaringBitmapValuesSink []uint32
var roaringBitmapValuesIntoSink []uint32

func roaringBitmapValuesFixture() RoaringBitmap {
	bitmap := NewRoaringBitmap()
	for value := uint32(0); value < 100_000; value++ {
		bitmap.Add(value)
	}
	return bitmap
}
