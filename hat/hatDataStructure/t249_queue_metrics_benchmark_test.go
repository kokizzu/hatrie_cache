package hatDataStructure

import (
	"testing"
	"time"
)

var t249MetricsSink PriorityVisibilityQueueMetrics

func BenchmarkT249PriorityQueueLeaseAckDefault(b *testing.B) {
	benchmarkT249PriorityQueueLeaseAck(b, false)
}

func BenchmarkT249PriorityQueueLeaseAckMetrics(b *testing.B) {
	benchmarkT249PriorityQueueLeaseAck(b, true)
}

func benchmarkT249PriorityQueueLeaseAck(b *testing.B, enableMetrics bool) {
	queue := NewPriorityVisibilityQueueWithOptions[int](PriorityVisibilityQueueOptions{
		Capacity:          257,
		VisibilityTimeout: time.Minute,
		EnableMetrics:     enableMetrics,
	})
	now := time.Unix(1_700_000_000, 0).UTC()
	for value := 0; value < 256; value++ {
		if !queue.Enqueue(0, value) {
			b.Fatalf("initial Enqueue(%d) failed", value)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for value := 0; value < b.N; value++ {
		item, ok := queue.Lease(now)
		if !ok {
			if !queue.Enqueue(0, value) {
				b.Fatal("refill Enqueue() failed")
			}
			item, ok = queue.Lease(now)
		}
		if !ok || !queue.Ack(item.ID) {
			b.Fatal("Lease/Ack cycle failed")
		}
	}
}

func BenchmarkT249PriorityQueueMetrics256(b *testing.B) {
	queue := NewPriorityVisibilityQueueWithOptions[int](PriorityVisibilityQueueOptions{
		Capacity:          256,
		VisibilityTimeout: time.Minute,
		EnableMetrics:     true,
	})
	now := time.Unix(1_700_000_000, 0).UTC()
	for value := 0; value < 256; value++ {
		if !queue.Enqueue(0, value) {
			b.Fatalf("initial Enqueue(%d) failed", value)
		}
	}
	for range 256 {
		if _, ok := queue.Lease(now); !ok {
			b.Fatal("initial Lease() failed")
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		t249MetricsSink = queue.Metrics(now)
	}
}
