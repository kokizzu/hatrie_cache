package hatDataStructure

import "testing"

var roaringLookupBenchmarkSink bool

func BenchmarkRoaringBitmapLookupFastPathC211(b *testing.B) {
	b.Run("sparse_container_hit", func(b *testing.B) {
		var bitmap RoaringBitmap
		for index := uint32(0); index < 2048; index++ {
			bitmap.Add(index<<roaringBitmapContainerBits | 7)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			roaringLookupBenchmarkSink = bitmap.Contains(uint32(index&2047)<<roaringBitmapContainerBits | 7)
		}
	})
	b.Run("sparse_container_miss", func(b *testing.B) {
		var bitmap RoaringBitmap
		for index := uint32(0); index < 2048; index++ {
			bitmap.Add(index<<roaringBitmapContainerBits | 7)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			roaringLookupBenchmarkSink = bitmap.Contains(uint32(index&2047)<<roaringBitmapContainerBits | 8)
		}
	})
	b.Run("array_container_hit", func(b *testing.B) {
		var bitmap RoaringBitmap
		for index := uint32(0); index < 1024; index++ {
			bitmap.Add(index)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			roaringLookupBenchmarkSink = bitmap.Contains(uint32(index & 1023))
		}
	})
	b.Run("array_container_miss", func(b *testing.B) {
		var bitmap RoaringBitmap
		for index := uint32(0); index < 1024; index++ {
			bitmap.Add(index * 2)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			roaringLookupBenchmarkSink = bitmap.Contains(uint32(index&1023)*2 + 1)
		}
	})
}
