package hatPipeline

import (
	"context"
	"testing"
)

func BenchmarkFrontierRegistryAdvanceNoWaiter(b *testing.B) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register("source"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		if err := registry.Advance("source", uint64(i), uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrontierRegistryWaitReady(b *testing.B) {
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
	for i := 0; i < b.N; i++ {
		if err := registry.WaitUntil(ctx, "source", 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrontierRegistrySnapshotAll128(b *testing.B) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 128})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 128; i++ {
		if err := registry.Register(string(rune('a'+i%26)) + string(rune(i))); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = registry.SnapshotAll()
	}
}
