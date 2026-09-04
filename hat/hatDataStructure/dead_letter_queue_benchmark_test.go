package hatDataStructure

import (
	"testing"
	"time"
)

const deadLetterQueueBenchmarkSize = 128

var deadLetterQueueBenchmarkSink int

func BenchmarkDeadLetterQueueReplay(b *testing.B) {
	now := time.Unix(9000, 0)
	b.Run("dead_letter_queue", func(b *testing.B) {
		queue := NewDeadLetterQueue[int](deadLetterQueueBenchmarkSize+1, deadLetterQueueBenchmarkSize)
		ids := make([]uint64, deadLetterQueueBenchmarkSize)
		for index := range ids {
			ids[index] = queue.FailAt(
				DelayQueueItem[int]{ReadyAt: now, Value: index},
				now,
				1,
				"retry",
			)
		}
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			slot := index % deadLetterQueueBenchmarkSize
			id := ids[slot]
			if !queue.ReplayAt(id, now) {
				b.Fatal("ReplayAt returned false")
			}
			nextID := queue.FailAt(
				DelayQueueItem[int]{ReadyAt: now, Value: index},
				now,
				1,
				"retry",
			)
			ids[slot] = nextID
			deadLetterQueueBenchmarkSink = slot
		}
	})

	b.Run("slice_reference", func(b *testing.B) {
		pending := NewDelayQueue[int](deadLetterQueueBenchmarkSize + 1)
		dead := make([]DeadLetterItem[int], deadLetterQueueBenchmarkSize)
		ids := make([]uint64, deadLetterQueueBenchmarkSize)
		for index := range dead {
			ids[index] = uint64(index + 1)
			dead[index] = DeadLetterItem[int]{
				ID:       ids[index],
				ReadyAt:  now,
				Value:    index,
				FailedAt: now,
				Attempts: 1,
				Reason:   "retry",
			}
		}
		nextID := uint64(deadLetterQueueBenchmarkSize)
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			slot := index % deadLetterQueueBenchmarkSize
			id := ids[slot]
			pending.Push(now, index)
			found := -1
			for deadIndex := range dead {
				if dead[deadIndex].ID == id {
					found = deadIndex
					break
				}
			}
			if found < 0 {
				b.Fatal("reference replay did not find ID")
			}
			var zero DeadLetterItem[int]
			dead[found] = zero
			copy(dead[found:], dead[found+1:])
			dead = dead[:len(dead)-1]
			nextID++
			dead = append(dead, DeadLetterItem[int]{
				ID:       nextID,
				ReadyAt:  now,
				Value:    index,
				FailedAt: now,
				Attempts: 1,
				Reason:   "retry",
			})
			ids[slot] = nextID
			deadLetterQueueBenchmarkSink = slot
		}
	})
}
