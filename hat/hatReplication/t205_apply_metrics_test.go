package hatReplication

import (
	"math"
	"testing"
	"time"
)

func TestT205TargetApplyMetricsAreMonotonicAndRateBased(t *testing.T) {
	metrics := &Metrics{}
	base := time.Unix(100, 0)

	metrics.ObserveTargetApply("node-b", 10, 5, 100, base)
	metrics.ObserveTargetApply("node-b", 9, 99, 999, base.Add(time.Second))
	metrics.ObserveTargetApply("node-b", 12, 3, 60, base.Add(2*time.Second))

	snapshot := metrics.Snapshot()
	apply, ok := snapshot.TargetApply["node-b"]
	if !ok {
		t.Fatalf("TargetApply does not contain node-b: %#v", snapshot.TargetApply)
	}
	if apply.LastAppliedSequence != 12 {
		t.Fatalf("LastAppliedSequence = %d, want 12", apply.LastAppliedSequence)
	}
	if apply.AppliedBatches != 2 || apply.AppliedEntries != 8 || apply.AppliedPayloadBytes != 160 {
		t.Fatalf("apply totals = %#v, want batches=2 entries=8 bytes=160", apply)
	}
	if math.Abs(apply.AppliedEntriesPerSecond-4) > 1e-9 {
		t.Fatalf("AppliedEntriesPerSecond = %v, want 4", apply.AppliedEntriesPerSecond)
	}
	if math.Abs(apply.AppliedPayloadBytesPerSecond-80) > 1e-9 {
		t.Fatalf("AppliedPayloadBytesPerSecond = %v, want 80", apply.AppliedPayloadBytesPerSecond)
	}
}

func TestT205TargetApplyMetricsIgnoreInvalidObservations(t *testing.T) {
	metrics := &Metrics{}
	at := time.Unix(100, 0)
	metrics.ObserveTargetApply("", 1, 1, 1, at)
	metrics.ObserveTargetApply("node-b", 0, 1, 1, at)
	metrics.ObserveTargetApply("node-b", 1, 0, 1, at)
	metrics.ObserveTargetApply("node-b", 1, 1, 1, at)

	snapshot := metrics.Snapshot()
	apply := snapshot.TargetApply["node-b"]
	if apply.AppliedBatches != 1 || apply.AppliedEntries != 1 || apply.AppliedPayloadBytes != 1 {
		t.Fatalf("apply totals = %#v, want one valid observation", apply)
	}
}
