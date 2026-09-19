package hatPipeline

import (
	"context"
	"strconv"
	"testing"
)

var sinkBackpressureBenchmarkSink uint64

func BenchmarkSinkBackpressureDirectFrontier(b *testing.B) {
	var emitted, acknowledged uint64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		emitted++
		acknowledged = emitted - 1
		sinkBackpressureBenchmarkSink = emitted - acknowledged
	}
}

func BenchmarkSinkBackpressureRecord(b *testing.B) {
	registry := benchmarkSinkBackpressureRegistry(b, SinkBackpressureSinkOptions{HighWatermark: MaxSinkBackpressureWatermark, LowWatermark: MaxSinkBackpressureWatermark / 2})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := registry.Record("orders", uint64(i+1), uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkBackpressureAdvanceAcknowledge(b *testing.B) {
	registry := benchmarkSinkBackpressureRegistry(b, SinkBackpressureSinkOptions{HighWatermark: MaxSinkBackpressureWatermark, LowWatermark: MaxSinkBackpressureWatermark / 2})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frontier := uint64(i + 1)
		if err := registry.Advance("orders", frontier); err != nil {
			b.Fatal(err)
		}
		if err := registry.Acknowledge("orders", frontier); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkBackpressureWaitReady(b *testing.B) {
	registry := benchmarkSinkBackpressureRegistry(b, SinkBackpressureSinkOptions{})
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := registry.WaitUntilWritable(ctx, "orders"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkBackpressureSnapshot128(b *testing.B) {
	registry, err := NewSinkBackpressureRegistry(SinkBackpressureRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 128; i++ {
		if err := registry.Register("sink-"+strconv.Itoa(i), SinkBackpressureSinkOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot := registry.Snapshot()
		sinkBackpressureBenchmarkSink = uint64(len(snapshot))
	}
}

func benchmarkSinkBackpressureRegistry(b *testing.B, options SinkBackpressureSinkOptions) *SinkBackpressureRegistry {
	b.Helper()
	registry, err := NewSinkBackpressureRegistry(SinkBackpressureRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register("orders", options); err != nil {
		b.Fatal(err)
	}
	return registry
}
