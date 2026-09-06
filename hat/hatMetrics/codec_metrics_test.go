package hatMetrics_test

import (
	"testing"
	"time"

	hatMetrics "hatrie_cache/hat/hatMetrics"
)

func TestCodecMetricsRecordsBytesAndCPUSeparately(t *testing.T) {
	metrics := hatMetrics.NewCodecMetrics()
	metrics.RecordCompression(1000, 400, 10*time.Microsecond)
	metrics.RecordCompression(500, 250, 20*time.Microsecond)
	metrics.RecordDecompression(400, 1000, 5*time.Microsecond)
	metrics.RecordDecompression(250, 500, 7*time.Microsecond)

	snapshot := metrics.Snapshot()
	if snapshot.CompressionInputBytes != 1500 || snapshot.CompressionOutputBytes != 650 {
		t.Fatalf("compression bytes = %#v", snapshot)
	}
	if snapshot.CompressionCPUNanoseconds != uint64(30*time.Microsecond) {
		t.Fatalf("compression cpu = %d, want %d", snapshot.CompressionCPUNanoseconds, 30*time.Microsecond)
	}
	if snapshot.DecompressionInputBytes != 650 || snapshot.DecompressionOutputBytes != 1500 {
		t.Fatalf("decompression bytes = %#v", snapshot)
	}
	if snapshot.DecompressionCPUNanoseconds != uint64(12*time.Microsecond) {
		t.Fatalf("decompression cpu = %d, want %d", snapshot.DecompressionCPUNanoseconds, 12*time.Microsecond)
	}
	if got := snapshot.CompressionRatio(); got != 650.0/1500.0 {
		t.Fatalf("CompressionRatio() = %v, want %v", got, 650.0/1500.0)
	}
}

func TestCodecMetricsNilAndZeroSnapshotsAreSafe(t *testing.T) {
	var metrics *hatMetrics.CodecMetrics
	metrics.RecordCompression(1, 1, time.Second)
	metrics.RecordDecompression(1, 1, time.Second)

	snapshot := metrics.Snapshot()
	if snapshot != (hatMetrics.CodecMetricsSnapshot{}) {
		t.Fatalf("nil Snapshot() = %#v", snapshot)
	}
	if got := snapshot.CompressionRatio(); got != 0 {
		t.Fatalf("zero CompressionRatio() = %v, want 0", got)
	}
}

func BenchmarkCodecMetricsRecordCompression(b *testing.B) {
	metrics := hatMetrics.NewCodecMetrics()
	b.ReportAllocs()
	for range b.N {
		metrics.RecordCompression(1024, 512, time.Microsecond)
	}
}
