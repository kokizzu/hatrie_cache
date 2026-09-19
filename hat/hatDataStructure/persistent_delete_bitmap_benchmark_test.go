package hatDataStructure

import (
	"testing"
)

var persistentDeleteBitmapBenchmarkSink *PersistentDeleteBitmap
var persistentDeleteBitmapBenchmarkBytesSink []byte

func BenchmarkPersistentDeleteBitmapPackedEncode(b *testing.B) {
	bitmap := persistentDeleteBitmapBenchmarkFixture()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := bitmap.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		persistentDeleteBitmapBenchmarkBytesSink = encoded
	}
}

func BenchmarkPersistentDeleteBitmapPackedDecode(b *testing.B) {
	bitmap := persistentDeleteBitmapBenchmarkFixture()
	encoded, err := bitmap.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decoded, err := DecodePersistentDeleteBitmap(encoded)
		if err != nil {
			b.Fatal(err)
		}
		persistentDeleteBitmapBenchmarkSink = decoded
	}
}

func persistentDeleteBitmapBenchmarkFixture() *PersistentDeleteBitmap {
	bitmap, err := NewPersistentDeleteBitmap(100_000)
	if err != nil {
		panic(err)
	}
	for row := uint64(0); row < bitmap.Rows(); row += 3 {
		if _, err := bitmap.Delete(row); err != nil {
			panic(err)
		}
	}
	return bitmap
}
