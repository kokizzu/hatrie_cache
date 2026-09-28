package hatPipeline

import "testing"

func BenchmarkM246FrontierRetentionPolicy(b *testing.B) {
	benchmarkM246FrontierRetentionAcquireRelease(b, true)
}

func benchmarkM246FrontierRetentionAcquireRelease(b *testing.B, withPolicy bool) {
	b.Helper()
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Register("orders"); err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Advance("orders", 0, 100); err != nil {
		b.Fatal(err)
	}
	registry, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if withPolicy {
		if err := registry.SetPolicy("orders", FrontierRetentionPolicy{MaxAge: 20, MaxBytes: 1 << 20}); err != nil {
			b.Fatal(err)
		}
		if err := registry.ObserveStorage("orders", 1<<16); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, err := registry.Acquire("orders", 95)
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Release(lease); err != nil {
			b.Fatal(err)
		}
	}
}
