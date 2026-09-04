package hatDataStructure

import "time"

const delayQueueHeapArity = 4

// DelayQueueItem is one value ordered by its readiness deadline. Items with
// equal deadlines are returned in insertion order.
type DelayQueueItem[T any] struct {
	ReadyAt time.Time
	Value   T

	sequence uint64
}

// DelayQueue is a non-thread-safe min-heap for values that become ready at a
// specific time. Its zero value is ready to use.
type DelayQueue[T any] struct {
	items        []DelayQueueItem[T]
	nextSequence uint64
}

// NewDelayQueue creates a delay queue with optional initial capacity.
func NewDelayQueue[T any](capacity int) *DelayQueue[T] {
	if capacity < 0 {
		capacity = 0
	}
	return &DelayQueue[T]{items: make([]DelayQueueItem[T], 0, capacity)}
}

// Len returns the number of queued values.
func (queue *DelayQueue[T]) Len() int {
	if queue == nil {
		return 0
	}
	return len(queue.items)
}

// Push adds a value that becomes ready at readyAt.
func (queue *DelayQueue[T]) Push(readyAt time.Time, value T) {
	if queue == nil {
		return
	}
	item := DelayQueueItem[T]{ReadyAt: readyAt, Value: value, sequence: queue.nextSequence}
	queue.nextSequence++
	queue.items = append(queue.items, item)
	queue.siftUp(len(queue.items) - 1)
}

// PushAfter adds a value that becomes ready after delay from now. Negative
// delays make the value immediately ready.
func (queue *DelayQueue[T]) PushAfter(now time.Time, delay time.Duration, value T) {
	queue.Push(now.Add(delay), value)
}

// Peek returns the earliest queued item without removing it, whether or not
// it is ready yet.
func (queue *DelayQueue[T]) Peek() (DelayQueueItem[T], bool) {
	if queue == nil || len(queue.items) == 0 {
		return DelayQueueItem[T]{}, false
	}
	return queue.items[0], true
}

// NextReadyAt returns the earliest deadline in the queue.
func (queue *DelayQueue[T]) NextReadyAt() (time.Time, bool) {
	item, ok := queue.Peek()
	if !ok {
		return time.Time{}, false
	}
	return item.ReadyAt, true
}

// Pop removes and returns the earliest queued item, even if its deadline has
// not arrived.
func (queue *DelayQueue[T]) Pop() (DelayQueueItem[T], bool) {
	if queue == nil || len(queue.items) == 0 {
		return DelayQueueItem[T]{}, false
	}
	root := queue.items[0]
	last := len(queue.items) - 1
	lastItem := queue.items[last]
	var zero T
	queue.items[last].Value = zero
	queue.items = queue.items[:last]
	if len(queue.items) > 0 {
		queue.items[0] = lastItem
		queue.siftDown(0)
	}
	return root, true
}

// PopReady removes and returns the earliest item only when its deadline is at
// or before now.
func (queue *DelayQueue[T]) PopReady(now time.Time) (T, bool) {
	var zero T
	item, ok := queue.Peek()
	if !ok || item.ReadyAt.After(now) {
		return zero, false
	}
	item, _ = queue.Pop()
	return item.Value, true
}

// Clear removes all queued values and releases references held by them.
func (queue *DelayQueue[T]) Clear() {
	if queue == nil {
		return
	}
	var zero T
	for index := range queue.items {
		queue.items[index].Value = zero
	}
	queue.items = queue.items[:0]
	queue.nextSequence = 0
}

func (queue *DelayQueue[T]) siftUp(index int) {
	for index > 0 {
		parent := (index - 1) / delayQueueHeapArity
		if !delayQueueItemBefore(queue.items[index], queue.items[parent]) {
			return
		}
		queue.items[index], queue.items[parent] = queue.items[parent], queue.items[index]
		index = parent
	}
}

func (queue *DelayQueue[T]) siftDown(index int) {
	for {
		left := index*delayQueueHeapArity + 1
		if left >= len(queue.items) {
			return
		}
		smallest := left
		lastChild := left + delayQueueHeapArity
		if lastChild > len(queue.items) {
			lastChild = len(queue.items)
		}
		for child := left + 1; child < lastChild; child++ {
			if delayQueueItemBefore(queue.items[child], queue.items[smallest]) {
				smallest = child
			}
		}
		if !delayQueueItemBefore(queue.items[smallest], queue.items[index]) {
			return
		}
		queue.items[index], queue.items[smallest] = queue.items[smallest], queue.items[index]
		index = smallest
	}
}

func delayQueueItemBefore[T any](left, right DelayQueueItem[T]) bool {
	if left.ReadyAt.Before(right.ReadyAt) {
		return true
	}
	if !left.ReadyAt.Equal(right.ReadyAt) {
		return false
	}
	return left.sequence < right.sequence
}
