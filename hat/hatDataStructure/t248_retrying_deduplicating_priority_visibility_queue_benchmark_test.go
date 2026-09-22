package hatDataStructure

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkT248BaseNackCycle(b *testing.B) {
	queue := NewDeduplicatingPriorityVisibilityQueue[int](0, time.Minute)
	now := time.Unix(100, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !queue.Enqueue("retry-key", 1, index) {
			b.Fatal("enqueue failed")
		}
		first, ok := queue.Lease(now)
		if !ok || !queue.Nack(first.ID, time.Time{}) {
			b.Fatal("first lease/nack failed")
		}
		second, ok := queue.Lease(now)
		if !ok || !queue.Ack(second.ID) {
			b.Fatal("second lease/ack failed")
		}
	}
}

func BenchmarkT248RetryingNackCycle(b *testing.B) {
	queue := NewRetryingDeduplicatingPriorityVisibilityQueue[int](0, time.Minute, 0)
	now := time.Unix(100, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !queue.Enqueue("retry-key", 1, index) {
			b.Fatal("enqueue failed")
		}
		first, ok := queue.Lease(now)
		if !ok || !queue.Nack(first.ID, time.Time{}) {
			b.Fatal("first lease/nack failed")
		}
		second, ok := queue.Lease(now)
		if !ok || !queue.Ack(second.ID) {
			b.Fatal("second lease/ack failed")
		}
	}
}

func BenchmarkT248RetryingDeadLetterCycle(b *testing.B) {
	queue := NewRetryingDeduplicatingPriorityVisibilityQueue[int](0, time.Minute, 1)
	now := time.Unix(100, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !queue.Enqueue("dead-key", 1, index) {
			b.Fatal("enqueue failed")
		}
		item, ok := queue.Lease(now)
		if !ok || !queue.Nack(item.ID, time.Time{}) {
			b.Fatal("dead-letter nack failed")
		}
		if _, ok := queue.PopDeadLetter(); !ok {
			b.Fatal("dead-letter pop failed")
		}
	}
}

func BenchmarkT248BaseActive256LeaseAck(b *testing.B) {
	queue := NewDeduplicatingPriorityVisibilityQueue[int](257, time.Hour)
	now := time.Unix(100, 0).UTC()
	for index := 0; index < 256; index++ {
		key := "active-" + strconv.Itoa(index)
		if !queue.Enqueue(key, int64(index), index) {
			b.Fatal("fixture enqueue failed")
		}
	}
	for index := 0; index < 256; index++ {
		if _, ok := queue.Lease(now); !ok {
			b.Fatal("fixture lease failed")
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !queue.Enqueue("cycle", 1, index) {
			b.Fatal("cycle enqueue failed")
		}
		item, ok := queue.Lease(now)
		if !ok || !queue.Ack(item.ID) {
			b.Fatal("cycle lease/ack failed")
		}
	}
}

func BenchmarkT248RetryingActive256LeaseAck(b *testing.B) {
	queue := NewRetryingDeduplicatingPriorityVisibilityQueue[int](257, time.Hour, 0)
	now := time.Unix(100, 0).UTC()
	for index := 0; index < 256; index++ {
		key := "active-" + strconv.Itoa(index)
		if !queue.Enqueue(key, int64(index), index) {
			b.Fatal("fixture enqueue failed")
		}
	}
	for index := 0; index < 256; index++ {
		if _, ok := queue.Lease(now); !ok {
			b.Fatal("fixture lease failed")
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !queue.Enqueue("cycle", 1, index) {
			b.Fatal("cycle enqueue failed")
		}
		item, ok := queue.Lease(now)
		if !ok || !queue.Ack(item.ID) {
			b.Fatal("cycle lease/ack failed")
		}
	}
}
