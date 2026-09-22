package hatReplication

import (
	"errors"
	"testing"
	"time"
)

func TestReplicationProgressMetricsReportsLagAndApplyRates(t *testing.T) {
	metrics, err := NewReplicationProgressMetrics(ReplicationProgressMetricsOptions{
		Enabled:    true,
		MaxTargets: 2,
	})
	if err != nil {
		t.Fatalf("NewReplicationProgressMetrics() error = %v", err)
	}
	start := time.Unix(100, 0)
	if err := metrics.Observe(ReplicationProgressObservation{
		TargetID:     "replica-a",
		SourceLSN:    100,
		AppliedLSN:   90,
		AppliedBytes: 900,
		ObservedAt:   start,
	}); err != nil {
		t.Fatalf("first Observe() error = %v", err)
	}
	if err := metrics.Observe(ReplicationProgressObservation{
		TargetID:     "replica-a",
		SourceLSN:    110,
		AppliedLSN:   100,
		AppliedBytes: 1100,
		ObservedAt:   start.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("second Observe() error = %v", err)
	}

	snapshot := metrics.Snapshot()
	if !snapshot.Enabled || snapshot.SourceLSN != 110 || len(snapshot.Targets) != 1 {
		t.Fatalf("snapshot = %#v, want enabled/source 110/one target", snapshot)
	}
	progress := snapshot.Targets[0]
	if progress.TargetID != "replica-a" || progress.AppliedLSN != 100 || progress.LagLSN != 10 || progress.AppliedBytes != 1100 {
		t.Fatalf("progress = %#v, want applied 100/lag 10/bytes 1100", progress)
	}
	if progress.ApplyLSNPerSecond != 5 || progress.ApplyBytesPerSecond != 100 {
		t.Fatalf("progress rates = %#v, want 5 LSN/s and 100 bytes/s", progress)
	}
}

func TestReplicationProgressMetricsUsesGlobalSourceAndSortedDetachedSnapshot(t *testing.T) {
	metrics, err := NewReplicationProgressMetrics(ReplicationProgressMetricsOptions{Enabled: true})
	if err != nil {
		t.Fatalf("NewReplicationProgressMetrics() error = %v", err)
	}
	start := time.Unix(200, 0)
	observations := []ReplicationProgressObservation{
		{TargetID: "replica-z", SourceLSN: 90, AppliedLSN: 80, ObservedAt: start},
		{TargetID: "replica-a", SourceLSN: 100, AppliedLSN: 100, ObservedAt: start},
	}
	for _, observation := range observations {
		if err := metrics.Observe(observation); err != nil {
			t.Fatalf("Observe(%+v) error = %v", observation, err)
		}
	}

	snapshot := metrics.Snapshot()
	if snapshot.SourceLSN != 100 || len(snapshot.Targets) != 2 {
		t.Fatalf("snapshot = %#v, want source 100 and two targets", snapshot)
	}
	if snapshot.Targets[0].TargetID != "replica-a" || snapshot.Targets[1].TargetID != "replica-z" {
		t.Fatalf("target order = %#v, want replica-a then replica-z", snapshot.Targets)
	}
	if snapshot.Targets[1].LagLSN != 20 {
		t.Fatalf("replica-z lag = %d, want 20", snapshot.Targets[1].LagLSN)
	}
	snapshot.Targets[0].TargetID = "tampered"
	if metrics.Snapshot().Targets[0].TargetID != "replica-a" {
		t.Fatal("Snapshot() exposed mutable internal target state")
	}
}

func TestReplicationProgressMetricsRejectsInvalidProgressAndBoundsTargets(t *testing.T) {
	metrics, err := NewReplicationProgressMetrics(ReplicationProgressMetricsOptions{
		Enabled:    true,
		MaxTargets: 1,
	})
	if err != nil {
		t.Fatalf("NewReplicationProgressMetrics() error = %v", err)
	}
	start := time.Unix(300, 0)
	if err := metrics.Observe(ReplicationProgressObservation{
		TargetID:   "replica-a",
		SourceLSN:  10,
		AppliedLSN: 10,
		ObservedAt: start,
	}); err != nil {
		t.Fatalf("initial Observe() error = %v", err)
	}
	if err := metrics.Observe(ReplicationProgressObservation{
		TargetID:   "replica-b",
		SourceLSN:  10,
		AppliedLSN: 10,
		ObservedAt: start,
	}); !errors.Is(err, ErrReplicationProgressMetricsTargetLimit) {
		t.Fatalf("new target error = %v, want target limit", err)
	}
	if err := metrics.Observe(ReplicationProgressObservation{
		TargetID:   "replica-a",
		SourceLSN:  11,
		AppliedLSN: 9,
		ObservedAt: start.Add(time.Second),
	}); !errors.Is(err, ErrReplicationProgressMetricsRegressed) {
		t.Fatalf("LSN regression error = %v, want regression", err)
	}
	if err := metrics.Observe(ReplicationProgressObservation{
		TargetID:   "replica-a",
		SourceLSN:  11,
		AppliedLSN: 10,
		ObservedAt: start.Add(-time.Second),
	}); !errors.Is(err, ErrReplicationProgressMetricsTimestamp) {
		t.Fatalf("timestamp regression error = %v, want timestamp error", err)
	}
}

func TestReplicationProgressMetricsCanBeDisabled(t *testing.T) {
	metrics, err := NewReplicationProgressMetrics(ReplicationProgressMetricsOptions{})
	if err != nil {
		t.Fatalf("NewReplicationProgressMetrics() error = %v", err)
	}
	if err := metrics.Observe(ReplicationProgressObservation{
		TargetID:   "replica-a",
		SourceLSN:  1,
		AppliedLSN: 1,
		ObservedAt: time.Unix(400, 0),
	}); !errors.Is(err, ErrReplicationProgressMetricsDisabled) {
		t.Fatalf("disabled Observe() error = %v, want disabled", err)
	}
	snapshot := metrics.Snapshot()
	if snapshot.Enabled || snapshot.SourceLSN != 0 || len(snapshot.Targets) != 0 {
		t.Fatalf("disabled snapshot = %#v, want empty disabled snapshot", snapshot)
	}
}

func TestReplicationProgressMetricsRejectsInvalidOptionsAndSaturatesRates(t *testing.T) {
	for _, maxTargets := range []int{-1, maxReplicationProgressMetricsTargets + 1} {
		if _, err := NewReplicationProgressMetrics(ReplicationProgressMetricsOptions{
			Enabled:    true,
			MaxTargets: maxTargets,
		}); !errors.Is(err, ErrReplicationProgressMetricsInvalidOptions) {
			t.Fatalf("MaxTargets=%d error = %v, want invalid options", maxTargets, err)
		}
	}
	metrics, err := NewReplicationProgressMetrics(ReplicationProgressMetricsOptions{
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("NewReplicationProgressMetrics() error = %v", err)
	}
	start := time.Unix(500, 0)
	maxUint64 := ^uint64(0)
	if err := metrics.Observe(ReplicationProgressObservation{
		TargetID:   "replica-a",
		SourceLSN:  maxUint64,
		AppliedLSN: 0,
		ObservedAt: start,
	}); err != nil {
		t.Fatalf("initial Observe() error = %v", err)
	}
	if err := metrics.Observe(ReplicationProgressObservation{
		TargetID:     "replica-a",
		SourceLSN:    maxUint64,
		AppliedLSN:   maxUint64,
		AppliedBytes: maxUint64,
		ObservedAt:   start.Add(time.Nanosecond),
	}); err != nil {
		t.Fatalf("overflow Observe() error = %v", err)
	}
	progress := metrics.Snapshot().Targets[0]
	if progress.ApplyLSNPerSecond != maxUint64 || progress.ApplyBytesPerSecond != maxUint64 {
		t.Fatalf("saturated rates = %#v, want uint64 saturation", progress)
	}
}
