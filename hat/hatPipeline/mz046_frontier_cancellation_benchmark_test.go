package hatPipeline

import (
	"context"
	"testing"
)

func BenchmarkMZ046FrontierCancellationLifecycle(b *testing.B) {
	b.Run("WaitUntil", func(b *testing.B) {
		registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Register("source"); err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := registry.WaitUntil(ctx, "source", 0); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("FrontierCancellation", func(b *testing.B) {
		registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Register("source"); err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			watcher, err := NewFrontierCancellation(ctx, registry, "source", 0)
			if err != nil {
				b.Fatal(err)
			}
			if err := watcher.Wait(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("FrontierCancellationPending", func(b *testing.B) {
		registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Register("source"); err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			watcher, err := NewFrontierCancellation(ctx, registry, "source", 1)
			if err != nil {
				b.Fatal(err)
			}
			watcher.Cancel()
			if err := watcher.Wait(); err != context.Canceled {
				b.Fatalf("Wait() error = %v, want context.Canceled", err)
			}
		}
	})
}
