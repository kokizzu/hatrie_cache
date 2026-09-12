package hatPipeline

import "testing"

func BenchmarkFrontierRetentionSafeCompactionBeforeNoLease(b *testing.B) {
	frontiers := benchmarkRetentionFrontiers(b)
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := retention.SafeCompactionBefore("events"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrontierRetentionSafeCompactionBeforeWithLease(b *testing.B) {
	frontiers := benchmarkRetentionFrontiers(b)
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := retention.Acquire("events", 90); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := retention.SafeCompactionBefore("events"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrontierRetentionAcquireRelease(b *testing.B) {
	frontiers := benchmarkRetentionFrontiers(b)
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{MaxLeases: 1})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, err := retention.Acquire("events", 90)
		if err != nil {
			b.Fatal(err)
		}
		if err := retention.Release(lease); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkRetentionFrontiers(b *testing.B) *FrontierRegistry {
	b.Helper()
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Register("events"); err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Advance("events", 80, 100); err != nil {
		b.Fatal(err)
	}
	return frontiers
}
