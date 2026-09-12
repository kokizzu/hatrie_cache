package hatDataStructure

import (
	"testing"
	"time"
)

func BenchmarkVisibilityQueueLeaseAck(b *testing.B) {
	queue := NewVisibilityQueue[int](1024, time.Minute)
	now := time.Unix(500, 0).UTC()
	if !queue.Enqueue(0) {
		b.Fatal("initial Enqueue returned false")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item, ok := queue.Lease(now)
		if !ok {
			b.Fatal("Lease returned no item")
		}
		if !queue.Ack(item.ID) {
			b.Fatal("Ack returned false")
		}
		if !queue.Enqueue(i) {
			b.Fatal("Enqueue returned false")
		}
	}
}

func BenchmarkVisibilityQueueLeaseAckResident256(b *testing.B) {
	queue := NewVisibilityQueue[int](512, time.Minute)
	now := time.Unix(550, 0).UTC()
	items := make([]VisibilityQueueItem[int], 256)
	for i := range items {
		if !queue.Enqueue(i) {
			b.Fatal("initial Enqueue returned false")
		}
	}
	for i := range items {
		item, ok := queue.Lease(now)
		if !ok {
			b.Fatal("initial Lease returned no item")
		}
		items[i] = item
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := i & (len(items) - 1)
		if !queue.Ack(items[index].ID) {
			b.Fatal("Ack returned false")
		}
		if !queue.Enqueue(i) {
			b.Fatal("Enqueue returned false")
		}
		item, ok := queue.Lease(now)
		if !ok {
			b.Fatal("Lease returned no item")
		}
		items[index] = item
	}
}

func BenchmarkVisibilityQueueRequeueExpired(b *testing.B) {
	queue := NewVisibilityQueue[int](1024, time.Second)
	now := time.Unix(600, 0).UTC()
	if !queue.Enqueue(0) {
		b.Fatal("initial Enqueue returned false")
	}
	leaseNow := now
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := queue.Lease(leaseNow); !ok {
			b.Fatal("Lease returned no item")
		}
		leaseNow = leaseNow.Add(time.Second)
		if got := queue.RequeueExpired(leaseNow); got != 1 {
			b.Fatalf("RequeueExpired = %d, want 1", got)
		}
	}
}
