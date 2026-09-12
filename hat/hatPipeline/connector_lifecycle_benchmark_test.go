package hatPipeline

import (
	"context"
	"sync/atomic"
	"testing"
)

type benchmarkLifecycleConnector struct{}

var benchmarkLifecycleCalls uint64

func (benchmarkLifecycleConnector) Start(context.Context) error {
	atomic.AddUint64(&benchmarkLifecycleCalls, 1)
	return nil
}

func (benchmarkLifecycleConnector) Pause(context.Context) error {
	atomic.AddUint64(&benchmarkLifecycleCalls, 1)
	return nil
}

func (benchmarkLifecycleConnector) Resume(context.Context) error {
	atomic.AddUint64(&benchmarkLifecycleCalls, 1)
	return nil
}

func (benchmarkLifecycleConnector) Stop(context.Context) error {
	atomic.AddUint64(&benchmarkLifecycleCalls, 1)
	return nil
}

func BenchmarkConnectorRegistryLifecycle(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry, err := NewConnectorRegistry(ConnectorRegistryOptions{HistoryLimit: 1})
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Register("source", benchmarkLifecycleConnector{}); err != nil {
			b.Fatal(err)
		}
		if err := registry.Start(ctx, "source"); err != nil {
			b.Fatal(err)
		}
		if err := registry.Pause(ctx, "source"); err != nil {
			b.Fatal(err)
		}
		if err := registry.Resume(ctx, "source"); err != nil {
			b.Fatal(err)
		}
		if err := registry.Stop(ctx, "source"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnectorDirectLifecycle(b *testing.B) {
	ctx := context.Background()
	connector := benchmarkLifecycleConnector{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := connector.Start(ctx); err != nil {
			b.Fatal(err)
		}
		if err := connector.Pause(ctx); err != nil {
			b.Fatal(err)
		}
		if err := connector.Resume(ctx); err != nil {
			b.Fatal(err)
		}
		if err := connector.Stop(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnectorRegistrySnapshot(b *testing.B) {
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 128; i++ {
		if err := registry.Register(string(rune('a'+i%26))+string(rune(i)), benchmarkLifecycleConnector{}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = registry.Snapshot()
	}
}
