package hatPipeline

import "testing"

func BenchmarkMZ027RetentionSnapshotSummary(b *testing.B) {
	retention := benchmarkMZ027Retention(b, 64)
	defer func() { _ = retention.Close() }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := retention.Snapshot("events"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ027ActiveLeases(b *testing.B) {
	retention := benchmarkMZ027Retention(b, 64)
	defer func() { _ = retention.Close() }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		leases, err := retention.ActiveLeases("events")
		if err != nil {
			b.Fatal(err)
		}
		if len(leases) != 64 {
			b.Fatalf("ActiveLeases() length = %d, want 64", len(leases))
		}
	}
}

func BenchmarkMZ027ActiveLeasesNoHolds(b *testing.B) {
	retention := benchmarkMZ027Retention(b, 0)
	defer func() { _ = retention.Close() }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		leases, err := retention.ActiveLeases("events")
		if err != nil {
			b.Fatal(err)
		}
		if len(leases) != 0 {
			b.Fatalf("ActiveLeases() length = %d, want 0", len(leases))
		}
	}
}

func benchmarkMZ027Retention(b *testing.B, leaseCount int) *FrontierRetentionRegistry {
	b.Helper()
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Register("events"); err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Advance("events", 1, 1000); err != nil {
		b.Fatal(err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{MaxLeases: 128})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < leaseCount; i++ {
		if _, err := retention.Acquire("events", uint64(i+1)); err != nil {
			_ = retention.Close()
			_ = frontiers.Close()
			b.Fatal(err)
		}
	}
	return retention
}
