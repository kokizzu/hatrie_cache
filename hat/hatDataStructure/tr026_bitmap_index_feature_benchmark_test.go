package hatDataStructure

import "testing"

var tr026BitmapIndexVisitSink uint64
var tr026BitmapIndexBuildSink *BitmapIndex[uint8]

func BenchmarkTR026BitmapVisit(b *testing.B) {
	values := tr026BitmapIndexBenchmarkValues()
	index := NewBitmapIndex[uint8]()
	for row, value := range values {
		if !index.Add(value, uint32(row)) {
			b.Fatalf("duplicate benchmark membership at row %d", row)
		}
	}
	needle := uint8(7)
	b.SetBytes(int64(len(values)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var matches uint64
		if !index.Visit(needle, func(uint32) bool {
			matches++
			return true
		}) {
			b.Fatal("bitmap visit stopped unexpectedly")
		}
		tr026BitmapIndexVisitSink = matches
	}
}

func BenchmarkTR026BitmapBuild(b *testing.B) {
	values := tr026BitmapIndexBenchmarkValues()
	probe := NewBitmapIndex[uint8]()
	for row, value := range values {
		probe.Add(value, uint32(row))
	}
	b.SetBytes(int64(len(values)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		index := NewBitmapIndex[uint8]()
		for row, value := range values {
			index.Add(value, uint32(row))
		}
		tr026BitmapIndexBuildSink = index
	}
	b.StopTimer()
	b.ReportMetric(float64(probe.Info().EncodedBytes), "bitmap-bytes")
}
