package hatDataStructure

import (
	"testing"
	"time"
)

func BenchmarkT248VisibilityQueueTerminalNack(b *testing.B) {
	queue, err := NewVisibilityQueueWithRetryPolicy[int](1, time.Minute, 1, VisibilityQueueRetryOptions{
		MaxAttempts:    1,
		MaxDeadLetters: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	now := time.Unix(100, 0)
	if !queue.Enqueue(0) {
		b.Fatal("initial Enqueue() rejected item")
	}
	item, ok := queue.Lease(now)
	if !ok || !queue.Nack(item.ID, now) {
		b.Fatal("initial terminal route failed")
	}
	if _, ok := queue.PopDeadLetter(); !ok {
		b.Fatal("initial PopDeadLetter() failed")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !queue.Enqueue(i) {
			b.Fatal("Enqueue() rejected item")
		}
		item, ok := queue.Lease(now)
		if !ok || !queue.Nack(item.ID, now) {
			b.Fatal("terminal route failed")
		}
		if _, ok := queue.PopDeadLetter(); !ok {
			b.Fatal("PopDeadLetter() failed")
		}
	}
}
