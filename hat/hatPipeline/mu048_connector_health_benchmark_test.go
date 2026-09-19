package hatPipeline

import (
	"context"
	"testing"
)

type mu048BenchmarkConnector struct{}

func (mu048BenchmarkConnector) Start(context.Context) error  { return nil }
func (mu048BenchmarkConnector) Pause(context.Context) error  { return nil }
func (mu048BenchmarkConnector) Resume(context.Context) error { return nil }
func (mu048BenchmarkConnector) Stop(context.Context) error   { return nil }

func BenchmarkMU048Start(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		registry, err := NewConnectorRegistry(ConnectorRegistryOptions{HistoryLimit: 1})
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Register("source", mu048BenchmarkConnector{}); err != nil {
			b.Fatal(err)
		}
		if err := registry.Start(ctx, "source"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU048StartWithHealthPolicy(b *testing.B) {
	ctx := context.Background()
	policy := ConnectorHealthPolicy{MaxAttempts: 1, QuarantineAfter: 1, InitialBackoff: 1, MaxBackoff: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		registry, err := NewConnectorRegistry(ConnectorRegistryOptions{HistoryLimit: 1})
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Register("source", mu048BenchmarkConnector{}); err != nil {
			b.Fatal(err)
		}
		if _, err := registry.StartWithHealthPolicy(ctx, "source", policy); err != nil {
			b.Fatal(err)
		}
	}
}
