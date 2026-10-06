package hatPipeline

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatDataStructure"
)

func BenchmarkM14BaselineVisibilityQueueLeaseAck(b *testing.B) {
	queue := hatDataStructure.NewVisibilityQueue[int](1, time.Minute)
	now := time.Unix(600, 0)
	if !queue.Enqueue(0) {
		b.Fatal("baseline Enqueue() failed")
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		item, ok := queue.Lease(now)
		if !ok || !queue.Ack(item.ID) || !queue.Enqueue(index) {
			b.Fatal("baseline lease/ack cycle failed")
		}
	}
}

func BenchmarkM14BaselineVisibilityQueueRetry(b *testing.B) {
	queue := hatDataStructure.NewVisibilityQueue[int](1, time.Minute)
	now := time.Unix(600, 0)
	if !queue.Enqueue(0) {
		b.Fatal("baseline Enqueue() failed")
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		item, ok := queue.Lease(now)
		if !ok || !queue.Nack(item.ID, now) {
			b.Fatal("baseline lease/retry cycle failed")
		}
	}
}

func BenchmarkM14SinkRetryQueueLeaseAck(b *testing.B) {
	queue, err := NewSinkRetryQueue[int](SinkRetryQueueOptions{Capacity: 1})
	if err != nil {
		b.Fatal(err)
	}
	now := time.Unix(600, 0)
	if !queue.Enqueue("orders", 0) {
		b.Fatal("Enqueue() failed")
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, ok := queue.Lease("orders", now)
		if !ok || !queue.Ack(lease) || !queue.Enqueue("orders", index) {
			b.Fatal("lease/ack cycle failed")
		}
	}
}

func BenchmarkM14SinkRetryQueueRetry(b *testing.B) {
	queue, err := NewSinkRetryQueue[int](SinkRetryQueueOptions{Capacity: 1, MaxAttempts: ^uint32(0)})
	if err != nil {
		b.Fatal(err)
	}
	now := time.Unix(600, 0)
	if !queue.Enqueue("orders", 0) {
		b.Fatal("Enqueue() failed")
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, ok := queue.Lease("orders", now)
		if !ok {
			b.Fatal("Lease() failed")
		}
		outcome, _, valid := queue.Retry(lease, now, "temporary")
		if !valid || outcome != SinkRetryOutcomeRequeued {
			b.Fatal("retry cycle failed")
		}
	}
}
