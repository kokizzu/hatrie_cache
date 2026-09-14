package hatDataStructure

import "testing"

const tr026BitmapIndexBenchmarkRows = 100_000

var tr026BitmapIndexLinearSink uint64
var tr026BitmapIndexRawStorageSink []uint8

func BenchmarkTR026LinearScan(b *testing.B) {
	values := tr026BitmapIndexBenchmarkValues()
	needle := uint8(7)
	b.SetBytes(int64(len(values)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var matches uint64
		for _, value := range values {
			if value == needle {
				matches++
			}
		}
		tr026BitmapIndexLinearSink = matches
	}
}

func BenchmarkTR026RawColumnCopy(b *testing.B) {
	values := tr026BitmapIndexBenchmarkValues()
	b.SetBytes(int64(len(values)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		tr026BitmapIndexRawStorageSink = append([]uint8(nil), values...)
	}
}

func tr026BitmapIndexBenchmarkValues() []uint8 {
	values := make([]uint8, tr026BitmapIndexBenchmarkRows)
	for index := range values {
		values[index] = uint8(index % 16)
	}
	return values
}
