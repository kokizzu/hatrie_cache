package hatDataStructure

import (
	"container/heap"
	"testing"
	"time"
)

const delayQueueBenchmarkSize = 256

var delayQueueBenchmarkSink int

func BenchmarkDelayQueuePushPop(b *testing.B) {
	base := time.Unix(500, 0)
	b.Run("delay_queue", func(b *testing.B) {
		queue := NewDelayQueue[int](delayQueueBenchmarkSize + 1)
		for index := 0; index < delayQueueBenchmarkSize; index++ {
			queue.Push(base.Add(time.Duration(index)*time.Second), index)
		}
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			queue.Push(base.Add(time.Duration(index%delayQueueBenchmarkSize)*time.Second), index)
			item, ok := queue.Pop()
			if !ok {
				b.Fatal("delay queue Pop returned empty")
			}
			delayQueueBenchmarkSink = item.Value
		}
	})

	b.Run("container_heap_reference", func(b *testing.B) {
		queue := make(delayQueueReferenceHeap, 0, delayQueueBenchmarkSize+1)
		for index := 0; index < delayQueueBenchmarkSize; index++ {
			heap.Push(&queue, delayQueueReferenceItem{readyAt: base.Add(time.Duration(index) * time.Second), value: index, sequence: uint64(index)})
		}
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			heap.Push(&queue, delayQueueReferenceItem{readyAt: base.Add(time.Duration(index%delayQueueBenchmarkSize) * time.Second), value: index, sequence: uint64(delayQueueBenchmarkSize + index)})
			item := heap.Pop(&queue).(delayQueueReferenceItem)
			delayQueueBenchmarkSink = item.value
		}
	})
}

type delayQueueReferenceItem struct {
	readyAt  time.Time
	value    int
	sequence uint64
}

type delayQueueReferenceHeap []delayQueueReferenceItem

func (queue delayQueueReferenceHeap) Len() int { return len(queue) }

func (queue delayQueueReferenceHeap) Less(left, right int) bool {
	if queue[left].readyAt.Equal(queue[right].readyAt) {
		return queue[left].sequence < queue[right].sequence
	}
	return queue[left].readyAt.Before(queue[right].readyAt)
}

func (queue delayQueueReferenceHeap) Swap(left, right int) {
	queue[left], queue[right] = queue[right], queue[left]
}

func (queue *delayQueueReferenceHeap) Push(value interface{}) {
	*queue = append(*queue, value.(delayQueueReferenceItem))
}

func (queue *delayQueueReferenceHeap) Pop() interface{} {
	items := *queue
	last := len(items) - 1
	value := items[last]
	*queue = items[:last]
	return value
}
