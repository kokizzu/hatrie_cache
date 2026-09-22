package hatReplication

import (
	"testing"
	"time"
)

func BenchmarkT205ObserveExistingTarget(b *testing.B) {
	metrics, err := NewReplicationProgressMetrics(ReplicationProgressMetricsOptions{
		Enabled:    true,
		MaxTargets: 1,
	})
	if err != nil {
		b.Fatalf("NewReplicationProgressMetrics() error = %v", err)
	}
	start := time.Unix(500, 0)
	if err := metrics.Observe(ReplicationProgressObservation{
		TargetID:     "replica-a",
		SourceLSN:    100,
		AppliedLSN:   90,
		AppliedBytes: 900,
		ObservedAt:   start,
	}); err != nil {
		b.Fatalf("initial Observe() error = %v", err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if err := metrics.Observe(ReplicationProgressObservation{
			TargetID:     "replica-a",
			SourceLSN:    uint64(index) + 101,
			AppliedLSN:   uint64(index) + 91,
			AppliedBytes: uint64(index) + 1000,
			ObservedAt:   start.Add(time.Duration(index+1) * time.Second),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT205SnapshotOneTarget(b *testing.B) {
	metrics, err := NewReplicationProgressMetrics(ReplicationProgressMetricsOptions{Enabled: true})
	if err != nil {
		b.Fatalf("NewReplicationProgressMetrics() error = %v", err)
	}
	if err := metrics.Observe(ReplicationProgressObservation{
		TargetID:   "replica-a",
		SourceLSN:  100,
		AppliedLSN: 90,
		ObservedAt: time.Unix(600, 0),
	}); err != nil {
		b.Fatalf("Observe() error = %v", err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		snapshot := metrics.Snapshot()
		if len(snapshot.Targets) != 1 {
			b.Fatal("snapshot lost target")
		}
	}
}
