package hatDataStructure

// DeduplicatingQueueItem is one queued key and payload.
type DeduplicatingQueueItem[K comparable, T any] struct {
	Key   K
	Value T
}

// DeduplicatingQueue is a FIFO queue that admits at most one active task for
// each key. A key becomes eligible again after its task is dequeued or the
// queue is cleared. It is not safe for concurrent use.
type DeduplicatingQueue[K comparable, T any] struct {
	items  []DeduplicatingQueueItem[K, T]
	head   int
	active map[K]struct{}
}

// NewDeduplicatingQueue creates a queue with an optional initial capacity.
func NewDeduplicatingQueue[K comparable, T any](capacity int) *DeduplicatingQueue[K, T] {
	if capacity < 0 {
		capacity = 0
	}
	return &DeduplicatingQueue[K, T]{
		items:  make([]DeduplicatingQueueItem[K, T], 0, capacity),
		active: make(map[K]struct{}, capacity),
	}
}

// Enqueue adds a task when key is not already active. It returns false for an
// active duplicate and does not replace the original payload.
func (queue *DeduplicatingQueue[K, T]) Enqueue(key K, value T) bool {
	if queue == nil {
		return false
	}
	if _, exists := queue.active[key]; exists {
		return false
	}
	queue.active[key] = struct{}{}
	queue.items = append(queue.items, DeduplicatingQueueItem[K, T]{Key: key, Value: value})
	return true
}

// Dequeue removes the oldest active task.
func (queue *DeduplicatingQueue[K, T]) Dequeue() (K, T, bool) {
	var zeroKey K
	var zeroValue T
	if queue == nil || queue.head >= len(queue.items) {
		return zeroKey, zeroValue, false
	}
	item := queue.items[queue.head]
	delete(queue.active, item.Key)
	queue.items[queue.head] = DeduplicatingQueueItem[K, T]{Key: zeroKey, Value: zeroValue}
	queue.head++
	if queue.head == len(queue.items) {
		queue.items = queue.items[:0]
		queue.head = 0
	} else if queue.head >= 1024 && queue.head*2 >= len(queue.items) {
		copy(queue.items, queue.items[queue.head:])
		remaining := len(queue.items) - queue.head
		clear(queue.items[remaining:])
		queue.items = queue.items[:remaining]
		queue.head = 0
	}
	return item.Key, item.Value, true
}

// Len returns the number of active queued tasks.
func (queue *DeduplicatingQueue[K, T]) Len() int {
	if queue == nil {
		return 0
	}
	return len(queue.items) - queue.head
}

// Contains reports whether key currently has an active queued task.
func (queue *DeduplicatingQueue[K, T]) Contains(key K) bool {
	if queue == nil {
		return false
	}
	_, exists := queue.active[key]
	return exists
}

// Clear removes all tasks and releases their key and payload references.
func (queue *DeduplicatingQueue[K, T]) Clear() {
	if queue == nil {
		return
	}
	clear(queue.items)
	queue.items = queue.items[:0]
	queue.head = 0
	clear(queue.active)
}
