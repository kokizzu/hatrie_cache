package hatDataStructure

import (
	"testing"
	"time"
)

const deadLetterQueueDeadLettersBenchmarkSize = 4096

var deadLetterQueueDeadLettersSink []DeadLetterItem[int]
var deadLetterQueueDeadLettersIntoSink []DeadLetterItem[int]

func BenchmarkDeadLetterQueueDeadLetters(b *testing.B) {
	queue := deadLetterQueueDeadLettersFixture()
	b.ReportAllocs()
	b.Run("dead_letters", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			deadLetterQueueDeadLettersSink = queue.DeadLetters()
		}
	})
	b.Run("dead_letters_into", func(b *testing.B) {
		destination := make([]DeadLetterItem[int], 0, queue.DeadLetterLen())
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			destination = queue.DeadLettersInto(destination)
			deadLetterQueueDeadLettersIntoSink = destination
		}
	})
}

func deadLetterQueueDeadLettersFixture() *DeadLetterQueue[int] {
	now := time.Unix(5000, 0)
	queue := NewDeadLetterQueue[int](0, deadLetterQueueDeadLettersBenchmarkSize)
	for index := 0; index < deadLetterQueueDeadLettersBenchmarkSize; index++ {
		queue.FailAt(DelayQueueItem[int]{ReadyAt: now, Value: index}, now, 1, "retry")
	}
	return queue
}
