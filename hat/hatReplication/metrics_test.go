package hatReplication

import (
	"testing"
	"time"
)

func TestMetricsRecordsIndependentHistogramsAndTransitions(t *testing.T) {
	var metrics Metrics
	metrics.ObserveTargetLatency("node-b", 12*time.Millisecond)
	metrics.ObserveTargetBatchItems("node-b", 3)
	metrics.ObserveRetryDelay(25 * time.Millisecond)
	metrics.RecordCircuitTransition("node-b", "open")

	snapshot := metrics.Snapshot()
	if got := snapshot.TargetLatencyMillis["node-b"]; got.Count != 1 || got.Sum != 12 {
		t.Fatalf("latency = %#v, want one 12ms observation", got)
	}
	if got := snapshot.TargetBatchItems["node-b"]; got.Count != 1 || got.Sum != 3 {
		t.Fatalf("batch = %#v, want one three-item observation", got)
	}
	if got := snapshot.RetryDelayMillis; got.Count != 1 || got.Sum != 25 {
		t.Fatalf("retry = %#v, want one 25ms observation", got)
	}
	if got := snapshot.CircuitBreakerTransitions["node-b"]["open"]; got != 1 {
		t.Fatalf("transitions = %#v, want node-b/open=1", snapshot.CircuitBreakerTransitions)
	}
	delete(snapshot.TargetLatencyMillis, "node-b")
	if _, exists := metrics.Snapshot().TargetLatencyMillis["node-b"]; !exists {
		t.Fatal("snapshot aliases metrics state")
	}
}
