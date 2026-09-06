package hatMetrics

import (
	"sync/atomic"
	"time"
)

// CodecMetrics tracks codec byte work and CPU time without retaining samples.
// Compression and decompression totals are kept separately because a stream
// may be written once and read many times.
type CodecMetrics struct {
	compressionInputBytes    atomic.Uint64
	compressionOutputBytes   atomic.Uint64
	compressionCPUNanos      atomic.Uint64
	decompressionInputBytes  atomic.Uint64
	decompressionOutputBytes atomic.Uint64
	decompressionCPUNanos    atomic.Uint64
}

// CodecMetricsSnapshot is a point-in-time copy of codec accounting totals.
type CodecMetricsSnapshot struct {
	CompressionInputBytes       uint64
	CompressionOutputBytes      uint64
	CompressionCPUNanoseconds   uint64
	DecompressionInputBytes     uint64
	DecompressionOutputBytes    uint64
	DecompressionCPUNanoseconds uint64
}

// NewCodecMetrics creates empty codec accounting.
func NewCodecMetrics() *CodecMetrics {
	return &CodecMetrics{}
}

// RecordCompression records uncompressed input bytes, compressed output
// bytes, and the CPU time spent producing that output.
func (metrics *CodecMetrics) RecordCompression(inputBytes, outputBytes uint64, cpu time.Duration) {
	if metrics == nil {
		return
	}
	metrics.compressionInputBytes.Add(inputBytes)
	metrics.compressionOutputBytes.Add(outputBytes)
	if cpu > 0 {
		metrics.compressionCPUNanos.Add(uint64(cpu))
	}
}

// RecordDecompression records compressed input bytes, uncompressed output
// bytes, and the CPU time spent producing that output.
func (metrics *CodecMetrics) RecordDecompression(inputBytes, outputBytes uint64, cpu time.Duration) {
	if metrics == nil {
		return
	}
	metrics.decompressionInputBytes.Add(inputBytes)
	metrics.decompressionOutputBytes.Add(outputBytes)
	if cpu > 0 {
		metrics.decompressionCPUNanos.Add(uint64(cpu))
	}
}

// Snapshot returns the current totals. Individual fields may come from
// adjacent updates when writers record concurrently, as with other atomic
// counter snapshots in this package.
func (metrics *CodecMetrics) Snapshot() CodecMetricsSnapshot {
	if metrics == nil {
		return CodecMetricsSnapshot{}
	}
	return CodecMetricsSnapshot{
		CompressionInputBytes:       metrics.compressionInputBytes.Load(),
		CompressionOutputBytes:      metrics.compressionOutputBytes.Load(),
		CompressionCPUNanoseconds:   metrics.compressionCPUNanos.Load(),
		DecompressionInputBytes:     metrics.decompressionInputBytes.Load(),
		DecompressionOutputBytes:    metrics.decompressionOutputBytes.Load(),
		DecompressionCPUNanoseconds: metrics.decompressionCPUNanos.Load(),
	}
}

// CompressionRatio returns compressed bytes divided by uncompressed bytes.
// Zero is returned when no compression input has been recorded.
func (snapshot CodecMetricsSnapshot) CompressionRatio() float64 {
	if snapshot.CompressionInputBytes == 0 {
		return 0
	}
	return float64(snapshot.CompressionOutputBytes) / float64(snapshot.CompressionInputBytes)
}
