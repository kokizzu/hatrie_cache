package hatDataStructure

import (
	"testing"
	"time"
)

func benchmarkT246PriorityVisibilityQueueReadySet() *PriorityVisibilityQueue[string] {
	queue := NewPriorityVisibilityQueue[string](2048, time.Minute)
	for index := 0; index < 1024; index++ {
		priority := int64(1)
		value := "high"
		if index == 0 {
			priority = 100
			value = "low"
		}
		if !queue.Enqueue(priority, value) {
			panic("priority visibility queue setup enqueue failed")
		}
	}
	return queue
}

func benchmarkT246PriorityVisibilityQueueRun(b *testing.B, bound int) {
	queue := benchmarkT246PriorityVisibilityQueueReadySet()
	queue.SetStarvationBound(bound)
	now := time.Unix(700, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		item, ok := queue.Lease(now)
		if !ok || !queue.Ack(item.ID) {
			b.Fatal("priority visibility queue lease/ack failed")
		}
		priority := int64(1)
		value := "high"
		if item.Priority == 100 {
			priority = 100
			value = "low"
		}
		if !queue.Enqueue(priority, value) {
			b.Fatal("priority visibility queue refill failed")
		}
	}
}

func BenchmarkT246PriorityVisibilityQueueStrict1024(b *testing.B) {
	benchmarkT246PriorityVisibilityQueueRun(b, 0)
}

func BenchmarkT246PriorityVisibilityQueueBounded1024(b *testing.B) {
	benchmarkT246PriorityVisibilityQueueRun(b, 16)
}
