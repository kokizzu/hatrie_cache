package hatDataStructure

import (
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestT249PriorityVisibilityQueueMetricsTracksCapacityAgeAndRetries(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	queue := NewPriorityVisibilityQueueWithOptions[int](PriorityVisibilityQueueOptions{
		Capacity:          3,
		VisibilityTimeout: time.Minute,
		EnableMetrics:     true,
	})
	if !queue.EnqueueAt(1, now.Add(-5*time.Second), 10) {
		t.Fatal("EnqueueAt(old) failed")
	}
	if !queue.EnqueueAt(2, now.Add(10*time.Second), 20) {
		t.Fatal("EnqueueAt(future) failed")
	}

	metrics := queue.Metrics(now)
	if !metrics.Enabled {
		t.Fatal("metrics are not enabled")
	}
	if metrics.Capacity != 3 || metrics.Used != 2 || metrics.Available != 1 {
		t.Fatalf("capacity metrics = %#v", metrics)
	}
	if metrics.Pending != 2 || metrics.Ready != 1 || metrics.Delayed != 1 || metrics.Leased != 0 {
		t.Fatalf("queue counts = %#v", metrics)
	}
	if metrics.OldestReadyAge != 5*time.Second || metrics.ConsumerLag != 5*time.Second {
		t.Fatalf("age metrics = %#v", metrics)
	}
	if metrics.TotalLeases != 0 || metrics.TotalRetries != 0 {
		t.Fatalf("initial retry metrics = %#v", metrics)
	}

	item, ok := queue.Lease(now)
	if !ok || item.Value != 10 || item.Attempts != 1 {
		t.Fatalf("Lease() = %#v, %v", item, ok)
	}
	metrics = queue.Metrics(now.Add(2 * time.Second))
	if metrics.Pending != 1 || metrics.Leased != 1 || metrics.OldestLeaseAge != 2*time.Second {
		t.Fatalf("leased metrics = %#v", metrics)
	}
	if !queue.Nack(item.ID, now.Add(2*time.Second)) {
		t.Fatal("Nack() failed")
	}
	item, ok = queue.Lease(now.Add(2 * time.Second))
	if !ok || item.Attempts != 2 {
		t.Fatalf("retry Lease() = %#v, %v", item, ok)
	}
	if !queue.Ack(item.ID) {
		t.Fatal("Ack() failed")
	}
	metrics = queue.Metrics(now.Add(2 * time.Second))
	if metrics.TotalLeases != 2 || metrics.TotalRetries != 1 {
		t.Fatalf("retry metrics = %#v", metrics)
	}
}

func TestT249PriorityVisibilityQueueMetricsDisabledByDefault(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	queue := NewPriorityVisibilityQueue[int](2, time.Minute)
	if !queue.EnqueueAt(0, now.Add(-time.Second), 1) {
		t.Fatal("EnqueueAt() failed")
	}
	metrics := queue.Metrics(now)
	if metrics.Enabled {
		t.Fatal("metrics enabled by default")
	}
	if metrics.Used != 1 || metrics.Pending != 1 || metrics.Available != 1 {
		t.Fatalf("disabled structural metrics = %#v", metrics)
	}
	if metrics.OldestReadyAge != 0 || metrics.ConsumerLag != 0 {
		t.Fatalf("disabled age metrics = %#v", metrics)
	}
}

func TestT249PriorityVisibilityQueueMetricsRestoreKeepsOptIn(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	queue := NewPriorityVisibilityQueueWithOptions[int](PriorityVisibilityQueueOptions{
		Capacity:      2,
		EnableMetrics: true,
	})
	if !queue.EnqueueAt(0, now.Add(-3*time.Second), 7) {
		t.Fatal("EnqueueAt() failed")
	}
	restored, err := RestorePriorityVisibilityQueue(queue.Snapshot(), 9)
	if err != nil {
		t.Fatalf("RestorePriorityVisibilityQueue() error = %v", err)
	}
	metrics := restored.Metrics(now)
	if !metrics.Enabled || metrics.Pending != 1 || metrics.ConsumerLag != 3*time.Second {
		t.Fatalf("restored metrics = %#v", metrics)
	}
}

func TestT249PriorityVisibilityQueueMetricsBinaryRestoreKeepsOptIn(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	queue := NewPriorityVisibilityQueueWithOptions[int](PriorityVisibilityQueueOptions{
		EnableMetrics: true,
	})
	if !queue.EnqueueAt(0, now.Add(-4*time.Second), 8) {
		t.Fatal("EnqueueAt() failed")
	}
	codec := PriorityVisibilityQueueCodec[int]{
		Encode: func(value int) ([]byte, error) { return []byte(strconv.Itoa(value)), nil },
		Decode: func(data []byte) (int, error) { return strconv.Atoi(string(data)) },
	}
	data, err := queue.MarshalSnapshot(codec)
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	restored, err := UnmarshalPriorityVisibilityQueue(data, codec)
	if err != nil {
		t.Fatalf("UnmarshalPriorityVisibilityQueue() error = %v", err)
	}
	metrics := restored.Metrics(now)
	if !metrics.Enabled || metrics.ConsumerLag != 4*time.Second {
		t.Fatalf("binary-restored metrics = %#v", metrics)
	}
}

func TestT249RetryingQueueMetricsIncludesDeadLetters(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	queue := NewRetryingDeduplicatingPriorityVisibilityQueueWithOptions[int](RetryingDeduplicatingPriorityVisibilityQueueOptions{
		PriorityVisibilityQueueOptions: PriorityVisibilityQueueOptions{
			EnableMetrics: true,
		},
		MaxAttempts: 1,
	})
	if !queue.Enqueue("job-1", 0, 7) {
		t.Fatal("Enqueue() failed")
	}
	item, ok := queue.Lease(now)
	if !ok {
		t.Fatal("Lease() failed")
	}
	if !queue.Nack(item.ID, now) {
		t.Fatal("Nack() failed")
	}
	metrics := queue.Metrics(now)
	if metrics.DeadLetters != 1 || metrics.Pending != 0 || metrics.Leased != 0 {
		t.Fatalf("dead-letter metrics = %#v", metrics)
	}
}

func TestT249RetryingQueueMetricsRestoreKeepsOptIn(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	queue := NewRetryingDeduplicatingPriorityVisibilityQueueWithOptions[int](RetryingDeduplicatingPriorityVisibilityQueueOptions{
		PriorityVisibilityQueueOptions: PriorityVisibilityQueueOptions{
			Capacity:      2,
			EnableMetrics: true,
		},
	})
	if !queue.EnqueueAt("job-1", 0, now.Add(-6*time.Second), 7) {
		t.Fatal("EnqueueAt() failed")
	}
	restored, err := RestoreRetryingDeduplicatingPriorityVisibilityQueue(queue.Snapshot(), 9)
	if err != nil {
		t.Fatalf("RestoreRetryingDeduplicatingPriorityVisibilityQueue() error = %v", err)
	}
	metrics := restored.Metrics(now)
	if !metrics.Enabled || metrics.Pending != 1 || metrics.ConsumerLag != 6*time.Second {
		t.Fatalf("restored retrying metrics = %#v", metrics)
	}
}

func TestT249PriorityVisibilityQueueMetricsRetainedHeap(t *testing.T) {
	const itemCount = 10_000
	withoutMetrics := t249RetainedHeap(itemCount, false)
	withMetrics := t249RetainedHeap(itemCount, true)
	t.Logf("retained heap: default=%d bytes (%.1f/item), metrics=%d bytes (%.1f/item), ratio=%.2fx", withoutMetrics, float64(withoutMetrics)/itemCount, withMetrics, float64(withMetrics)/itemCount, float64(withMetrics)/float64(withoutMetrics))
	if withoutMetrics == 0 || withMetrics <= withoutMetrics {
		t.Fatalf("unexpected retained heap default=%d metrics=%d", withoutMetrics, withMetrics)
	}
}

func t249RetainedHeap(itemCount int, enableMetrics bool) uint64 {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	queue := NewPriorityVisibilityQueueWithOptions[int](PriorityVisibilityQueueOptions{
		Capacity:      itemCount,
		EnableMetrics: enableMetrics,
	})
	now := time.Unix(1_700_000_000, 0).UTC()
	for value := 0; value < itemCount; value++ {
		queue.EnqueueAt(0, now, value)
	}
	for value := 0; value < itemCount; value++ {
		if _, ok := queue.Lease(now); !ok {
			panic("metrics heap measurement lease failed")
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(queue)
	if after.HeapAlloc <= before.HeapAlloc {
		return 0
	}
	return after.HeapAlloc - before.HeapAlloc
}
