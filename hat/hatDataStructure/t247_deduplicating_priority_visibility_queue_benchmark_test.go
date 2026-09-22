package hatDataStructure

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkT247PriorityVisibilityQueueLeaseAck(b *testing.B) {
	queue := NewPriorityVisibilityQueue[int](0, time.Minute)
	now := time.Unix(100, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !queue.Enqueue(1, index) {
			b.Fatal("enqueue failed")
		}
		item, ok := queue.Lease(now)
		if !ok || !queue.Ack(item.ID) {
			b.Fatal("lease/ack failed")
		}
	}
}

func BenchmarkT247DeduplicatingPriorityVisibilityQueueLeaseAck(b *testing.B) {
	queue := NewDeduplicatingPriorityVisibilityQueue[int](0, time.Minute)
	now := time.Unix(100, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !queue.Enqueue("bench-key", 1, index) {
			b.Fatal("enqueue failed")
		}
		item, ok := queue.Lease(now)
		if !ok || !queue.Ack(item.ID) {
			b.Fatal("lease/ack failed")
		}
	}
}

func BenchmarkT247PriorityVisibilityQueueLeaseAckResident256(b *testing.B) {
	queue := NewPriorityVisibilityQueue[int](256, time.Minute)
	now := time.Unix(100, 0).UTC()
	for index := 0; index < 256; index++ {
		if !queue.Enqueue(int64(index), index) {
			b.Fatal("fixture enqueue failed")
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		item, ok := queue.Lease(now)
		if !ok || !queue.Ack(item.ID) || !queue.Enqueue(item.Priority, item.Value) {
			b.Fatal("resident lease/ack/requeue failed")
		}
	}
}

func BenchmarkT247DeduplicatingPriorityVisibilityQueueLeaseAckResident256(b *testing.B) {
	queue := NewDeduplicatingPriorityVisibilityQueue[int](256, time.Minute)
	now := time.Unix(100, 0).UTC()
	for index := 0; index < 256; index++ {
		if !queue.Enqueue(strconv.Itoa(index), int64(index), index) {
			b.Fatal("fixture enqueue failed")
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		item, ok := queue.Lease(now)
		if !ok || !queue.Ack(item.ID) || !queue.Enqueue(item.Key, item.Priority, item.Value) {
			b.Fatal("resident lease/ack/requeue failed")
		}
	}
}
