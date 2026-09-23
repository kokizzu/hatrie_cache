package hatReplication

import (
	"testing"
	"time"
)

func BenchmarkT205MetricsObserveExisting(b *testing.B) {
	metrics := &Metrics{}
	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		metrics.ObserveTargetBatchItems("node-b", 16)
	}
}

func BenchmarkT205MetricsSnapshotExisting(b *testing.B) {
	metrics := &Metrics{}
	metrics.ObserveTargetBatchItems("node-b", 16)
	metrics.ObserveTargetLatency("node-b", 2*time.Millisecond)
	metrics.ObserveTargetWireBytes("node-b", "identity", 256)
	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		_ = metrics.Snapshot()
	}
}

func BenchmarkT205MetricsObserveApply(b *testing.B) {
	metrics := &Metrics{}
	at := time.Unix(100, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		metrics.ObserveTargetApply("node-b", uint64(idx+1), 16, 256, at)
	}
}

func BenchmarkT205MetricsSnapshotApply(b *testing.B) {
	metrics := &Metrics{}
	at := time.Unix(100, 0)
	metrics.ObserveTargetApply("node-b", 1, 16, 256, at)
	metrics.ObserveTargetApply("node-b", 2, 16, 256, at.Add(time.Second))
	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		_ = metrics.Snapshot()
	}
}
