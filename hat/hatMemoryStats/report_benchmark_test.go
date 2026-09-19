package hatMemoryStats

import "testing"

var benchmarkReport Report

func BenchmarkCompute(b *testing.B) {
	values := Values{
		HeapObjectsBytes:  64 << 20,
		HeapFreeBytes:     16 << 20,
		HeapReleasedBytes: 4 << 20,
		HeapStacksBytes:   2 << 20,
		TotalBytes:        128 << 20,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkReport = Compute(values)
	}
}

func BenchmarkSnapshot(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkReport = Snapshot()
	}
}
