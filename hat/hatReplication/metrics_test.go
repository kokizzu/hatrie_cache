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

func TestMetricsRecordsWireBytesByEncoding(t *testing.T) {
	var metrics Metrics
	metrics.ObserveTargetWireBytes("node-b", "gzip", 17)
	metrics.ObserveTargetWireBytes("node-b", "gzip", 3)
	metrics.ObserveTargetWireBytes("node-b", "", 11)

	snapshot := metrics.Snapshot()
	if got := snapshot.TargetWireBytes["node-b"]["gzip"]; got != 20 {
		t.Fatalf("gzip wire bytes = %d, want 20", got)
	}
	if got := snapshot.TargetWireBytes["node-b"]["identity"]; got != 11 {
		t.Fatalf("identity wire bytes = %d, want 11", got)
	}
	if got := snapshot.TargetWireRequests["node-b"]["gzip"]; got != 2 {
		t.Fatalf("gzip wire requests = %d, want 2", got)
	}
	if got := snapshot.TargetWireRequests["node-b"]["identity"]; got != 1 {
		t.Fatalf("identity wire requests = %d, want 1", got)
	}

	delete(snapshot.TargetWireBytes["node-b"], "gzip")
	if got := metrics.Snapshot().TargetWireBytes["node-b"]["gzip"]; got != 20 {
		t.Fatal("wire byte snapshot aliases metrics state")
	}
}

func TestMetricsRecordsQueueTiming(t *testing.T) {
	var metrics Metrics
	metrics.ObserveQueueWait(7 * time.Millisecond)
	metrics.ObserveQueueService(19 * time.Millisecond)

	snapshot := metrics.Snapshot()
	if got := snapshot.QueueWaitMillis; got.Count != 1 || got.Sum != 7 {
		t.Fatalf("queue wait = %#v, want one 7ms observation", got)
	}
	if got := snapshot.QueueServiceMillis; got.Count != 1 || got.Sum != 19 {
		t.Fatalf("queue service = %#v, want one 19ms observation", got)
	}
}

func BenchmarkMetricsObserveQueueTiming(b *testing.B) {
	var metrics Metrics
	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		metrics.ObserveQueueWait(time.Microsecond)
		metrics.ObserveQueueService(2 * time.Microsecond)
	}
}
