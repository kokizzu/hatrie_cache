package hatPipeline

import (
	"context"
	"testing"
)

func BenchmarkMZ046ManualWaitAndCancel(b *testing.B) {
	for b.Loop() {
		registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 1})
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Register("source"); err != nil {
			b.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			if err := registry.WaitUntil(ctx, "source", 1); err != nil {
				b.Errorf("wait: %v", err)
			}
			cancel()
			close(done)
		}()
		if err := registry.Advance("source", 1, 1); err != nil {
			b.Fatal(err)
		}
		<-done
		_ = registry.Close()
	}
}

func BenchmarkMZ046ManualWaitAndCancelCause(b *testing.B) {
	for b.Loop() {
		registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 1})
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Register("source"); err != nil {
			b.Fatal(err)
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		done := make(chan struct{})
		go func() {
			if err := registry.WaitUntil(ctx, "source", 1); err != nil {
				b.Errorf("wait: %v", err)
			}
			cancel(ErrFrontierTargetReached)
			close(done)
		}()
		if err := registry.Advance("source", 1, 1); err != nil {
			b.Fatal(err)
		}
		<-done
		_ = registry.Close()
	}
}
