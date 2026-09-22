package hatDataStructure

import (
	"testing"
	"time"
)

func BenchmarkT246PriorityVisibilityQueueStarvation(b *testing.B) {
	for _, test := range []struct {
		name            string
		starvationAfter uint32
	}{
		{name: "strict-priority", starvationAfter: 0},
		{name: "bounded-after-64", starvationAfter: 64},
	} {
		b.Run(test.name, func(b *testing.B) {
			queue := NewPriorityVisibilityQueueWithOptions[int](PriorityVisibilityQueueOptions{
				VisibilityTimeout: time.Minute,
				StarvationAfter:   test.starvationAfter,
			})
			if !queue.Enqueue(1000, -1) {
				b.Fatal("enqueue low-priority item failed")
			}
			now := time.Unix(500, 0).UTC()
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if !queue.Enqueue(1, index) {
					b.Fatal("enqueue urgent item failed")
				}
				item, ok := queue.Lease(now)
				if !ok || !queue.Ack(item.ID) {
					b.Fatal("lease/ack failed")
				}
			}
		})
	}
}

func BenchmarkT246PriorityVisibilityQueueResident(b *testing.B) {
	for _, test := range []struct {
		name            string
		starvationAfter uint32
	}{
		{name: "strict-priority", starvationAfter: 0},
		{name: "bounded-after-64", starvationAfter: 64},
	} {
		b.Run(test.name, func(b *testing.B) {
			queue := NewPriorityVisibilityQueueWithOptions[int](PriorityVisibilityQueueOptions{
				Capacity:          512,
				VisibilityTimeout: time.Minute,
				StarvationAfter:   test.starvationAfter,
			})
			for index := 0; index < 256; index++ {
				if !queue.Enqueue(int64(index&31), index) {
					b.Fatal("initial enqueue failed")
				}
			}
			now := time.Unix(500, 0).UTC()
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if !queue.Enqueue(1, index) {
					b.Fatal("resident enqueue failed")
				}
				item, ok := queue.Lease(now)
				if !ok || !queue.Ack(item.ID) {
					b.Fatal("resident lease/ack failed")
				}
			}
		})
	}
}
