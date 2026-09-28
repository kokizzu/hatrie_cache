package hatDataStructure

import (
	"testing"
	"time"
)

func BenchmarkT248VisibilityQueueNack(b *testing.B) {
	queue := NewVisibilityQueue[int](1, time.Minute)
	now := time.Unix(100, 0)
	if !queue.Enqueue(0) {
		b.Fatal("initial Enqueue() rejected item")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item, ok := queue.Lease(now)
		if !ok || !queue.Nack(item.ID, now) {
			b.Fatal("Lease/Nack failed")
		}
	}
}
