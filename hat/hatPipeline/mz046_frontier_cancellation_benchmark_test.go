package hatPipeline

import (
	"context"
	"errors"
	"testing"
)

func BenchmarkMZ046FrontierCancellation(b *testing.B) {
	for b.Loop() {
		registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 1})
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Register("source"); err != nil {
			b.Fatal(err)
		}
		cancellation, err := NewFrontierCancellation(context.Background(), registry, "source", 1)
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Advance("source", 1, 1); err != nil {
			b.Fatal(err)
		}
		if err := cancellation.Wait(); !errors.Is(err, ErrFrontierTargetReached) {
			b.Fatal(err)
		}
		_ = registry.Close()
	}
}

func BenchmarkMZ046FrontierCancellationAlreadyReached(b *testing.B) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register("source"); err != nil {
		b.Fatal(err)
	}
	if err := registry.Advance("source", 1, 1); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = registry.Close() })

	for b.Loop() {
		cancellation, err := NewFrontierCancellation(context.Background(), registry, "source", 1)
		if err != nil {
			b.Fatal(err)
		}
		if err := cancellation.Wait(); !errors.Is(err, ErrFrontierTargetReached) {
			b.Fatal(err)
		}
	}
}
