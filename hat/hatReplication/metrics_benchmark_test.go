package hatReplication

import "testing"

func BenchmarkObserveTargetWireBytes(b *testing.B) {
	var metrics Metrics
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		metrics.ObserveTargetWireBytes("node-b", "gzip", 1024)
	}
}

func BenchmarkObserveTargetBatchItems(b *testing.B) {
	var metrics Metrics
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		metrics.ObserveTargetBatchItems("node-b", 32)
	}
}

func BenchmarkSnapshotWithWireBytes(b *testing.B) {
	var metrics Metrics
	metrics.ObserveTargetWireBytes("node-b", "gzip", 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = metrics.Snapshot()
	}
}
