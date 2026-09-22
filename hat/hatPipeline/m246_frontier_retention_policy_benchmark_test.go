package hatPipeline

import "testing"

var m246FrontierRetentionSnapshotSink FrontierRetentionSnapshot
var m246FrontierRetentionPolicySnapshotSink FrontierRetentionPolicySnapshot
var m246FrontierRetentionErrorSink error

func benchmarkM246Retention(b *testing.B, policy bool) *FrontierRetentionRegistry {
	b.Helper()
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		b.Fatalf("NewFrontierRegistry() error = %v", err)
	}
	if err := frontiers.Register("events"); err != nil {
		b.Fatalf("Register() error = %v", err)
	}
	if err := frontiers.Advance("events", 10, 100); err != nil {
		b.Fatalf("Advance() error = %v", err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		b.Fatalf("NewFrontierRetentionRegistry() error = %v", err)
	}
	if policy {
		if err := retention.SetPolicy("events", FrontierRetentionPolicy{MaxHistory: 90, MaxBytes: 4096}); err != nil {
			retention.Close()
			b.Fatalf("SetPolicy() error = %v", err)
		}
		if err := retention.SetUsage("events", FrontierRetentionUsage{RetainedHistory: 32, RetainedBytes: 2048}); err != nil {
			retention.Close()
			b.Fatalf("SetUsage() error = %v", err)
		}
	}
	b.Cleanup(func() { _ = retention.Close() })
	return retention
}

func BenchmarkM246FrontierRetentionSnapshotNoPolicy(b *testing.B) {
	retention := benchmarkM246Retention(b, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot, err := retention.Snapshot("events")
		if err != nil {
			b.Fatalf("Snapshot() error = %v", err)
		}
		m246FrontierRetentionSnapshotSink = snapshot
	}
}

func BenchmarkM246FrontierRetentionSnapshotWithPolicy(b *testing.B) {
	retention := benchmarkM246Retention(b, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot, err := retention.PolicySnapshot("events")
		if err != nil {
			b.Fatalf("Snapshot() error = %v", err)
		}
		m246FrontierRetentionPolicySnapshotSink = snapshot
	}
}

func BenchmarkM246FrontierRetentionSetUsage(b *testing.B) {
	retention := benchmarkM246Retention(b, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m246FrontierRetentionErrorSink = retention.SetUsage("events", FrontierRetentionUsage{RetainedHistory: uint64(i), RetainedBytes: uint64(i * 64)})
	}
}
