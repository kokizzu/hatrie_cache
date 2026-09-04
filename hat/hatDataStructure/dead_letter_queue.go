package hatDataStructure

import "time"

// DeadLetterItem records a failed value retained for inspection or replay.
type DeadLetterItem[T any] struct {
	ID       uint64
	ReadyAt  time.Time
	Value    T
	FailedAt time.Time
	Attempts uint
	Reason   string
}

// DeadLetterQueue combines a delay queue with a bounded failure queue. It is
// not thread-safe; callers sharing one queue must provide synchronization.
type DeadLetterQueue[T any] struct {
	pending   DelayQueue[T]
	dead      []DeadLetterItem[T]
	nextID    uint64
	deadLimit int
}

// NewDeadLetterQueue creates a queue with the requested pending and
// dead-letter capacities. A non-positive dead-letter limit retains no failures.
func NewDeadLetterQueue[T any](capacity int, deadLetterLimit int) *DeadLetterQueue[T] {
	if deadLetterLimit < 0 {
		deadLetterLimit = 0
	}
	return &DeadLetterQueue[T]{
		pending:   *NewDelayQueue[T](capacity),
		deadLimit: deadLetterLimit,
	}
}

// SetDeadLetterLimit changes the maximum number of retained failures. A
// non-positive limit discards existing and future failures.
func (queue *DeadLetterQueue[T]) SetDeadLetterLimit(limit int) {
	if queue == nil {
		return
	}
	if limit < 0 {
		limit = 0
	}
	queue.deadLimit = limit
	if limit == 0 {
		queue.clearDeadLetters()
		return
	}
	queue.trimDeadLetters()
}

// EnqueueAt adds a value that becomes ready at readyAt.
func (queue *DeadLetterQueue[T]) EnqueueAt(readyAt time.Time, value T) {
	if queue == nil {
		return
	}
	queue.pending.Push(readyAt, value)
}

// EnqueueAfter adds a value that becomes ready after delay from now.
func (queue *DeadLetterQueue[T]) EnqueueAfter(now time.Time, delay time.Duration, value T) {
	if queue == nil {
		return
	}
	queue.pending.PushAfter(now, delay, value)
}

// Len returns the number of pending values.
func (queue *DeadLetterQueue[T]) Len() int {
	if queue == nil {
		return 0
	}
	return queue.pending.Len()
}

// DeadLetterLen returns the number of retained failures.
func (queue *DeadLetterQueue[T]) DeadLetterLen() int {
	if queue == nil {
		return 0
	}
	return len(queue.dead)
}

// Peek returns the earliest pending item without removing it.
func (queue *DeadLetterQueue[T]) Peek() (DelayQueueItem[T], bool) {
	if queue == nil {
		return DelayQueueItem[T]{}, false
	}
	return queue.pending.Peek()
}

// NextReadyAt returns the earliest pending deadline.
func (queue *DeadLetterQueue[T]) NextReadyAt() (time.Time, bool) {
	if queue == nil {
		return time.Time{}, false
	}
	return queue.pending.NextReadyAt()
}

// PopReady removes the earliest pending item when it is ready.
func (queue *DeadLetterQueue[T]) PopReady(now time.Time) (DelayQueueItem[T], bool) {
	if queue == nil {
		return DelayQueueItem[T]{}, false
	}
	item, ok := queue.pending.Peek()
	if !ok || item.ReadyAt.After(now) {
		return DelayQueueItem[T]{}, false
	}
	return queue.pending.Pop()
}

// Pop removes and returns the earliest pending item regardless of readiness.
func (queue *DeadLetterQueue[T]) Pop() (DelayQueueItem[T], bool) {
	if queue == nil {
		return DelayQueueItem[T]{}, false
	}
	return queue.pending.Pop()
}

// Fail retains a pending item as a dead letter using the current UTC time.
func (queue *DeadLetterQueue[T]) Fail(item DelayQueueItem[T], attempts uint, reason string) uint64 {
	return queue.FailAt(item, time.Now().UTC(), attempts, reason)
}

// FailAt retains a pending item as a dead letter with an explicit failure
// time. It returns zero when dead-letter retention is disabled.
func (queue *DeadLetterQueue[T]) FailAt(item DelayQueueItem[T], failedAt time.Time, attempts uint, reason string) uint64 {
	if queue == nil || queue.deadLimit <= 0 {
		return 0
	}
	queue.nextID++
	if queue.nextID == 0 {
		queue.nextID = 1
	}
	queue.dead = append(queue.dead, DeadLetterItem[T]{
		ID:       queue.nextID,
		ReadyAt:  item.ReadyAt,
		Value:    item.Value,
		FailedAt: failedAt,
		Attempts: attempts,
		Reason:   reason,
	})
	queue.trimDeadLetters()
	return queue.nextID
}

// DeadLetter returns a retained failure by ID.
func (queue *DeadLetterQueue[T]) DeadLetter(id uint64) (DeadLetterItem[T], bool) {
	if queue == nil || id == 0 {
		return DeadLetterItem[T]{}, false
	}
	for _, item := range queue.dead {
		if item.ID == id {
			return item, true
		}
	}
	return DeadLetterItem[T]{}, false
}

// DeadLetters returns an independent slice of retained failures in failure
// order.
func (queue *DeadLetterQueue[T]) DeadLetters() []DeadLetterItem[T] {
	if queue == nil || len(queue.dead) == 0 {
		return nil
	}
	out := make([]DeadLetterItem[T], len(queue.dead))
	copy(out, queue.dead)
	return out
}

// ReplayAt removes a failure and puts its value back into the pending queue at
// readyAt.
func (queue *DeadLetterQueue[T]) ReplayAt(id uint64, readyAt time.Time) bool {
	if queue == nil || id == 0 {
		return false
	}
	for index, item := range queue.dead {
		if item.ID != id {
			continue
		}
		queue.pending.Push(readyAt, item.Value)
		queue.removeDeadLetter(index)
		return true
	}
	return false
}

// Discard removes a retained failure without replaying it.
func (queue *DeadLetterQueue[T]) Discard(id uint64) bool {
	if queue == nil || id == 0 {
		return false
	}
	for index, item := range queue.dead {
		if item.ID == id {
			queue.removeDeadLetter(index)
			return true
		}
	}
	return false
}

// Clear removes pending values and retained failures.
func (queue *DeadLetterQueue[T]) Clear() {
	if queue == nil {
		return
	}
	queue.pending.Clear()
	queue.clearDeadLetters()
}

func (queue *DeadLetterQueue[T]) trimDeadLetters() {
	if queue.deadLimit <= 0 || len(queue.dead) <= queue.deadLimit {
		return
	}
	drop := len(queue.dead) - queue.deadLimit
	copy(queue.dead, queue.dead[drop:])
	for index := queue.deadLimit; index < len(queue.dead); index++ {
		var zero T
		queue.dead[index].Value = zero
	}
	queue.dead = queue.dead[:queue.deadLimit]
}

func (queue *DeadLetterQueue[T]) removeDeadLetter(index int) {
	last := len(queue.dead) - 1
	var zero T
	queue.dead[index].Value = zero
	copy(queue.dead[index:], queue.dead[index+1:])
	queue.dead[last] = DeadLetterItem[T]{}
	queue.dead = queue.dead[:last]
}

func (queue *DeadLetterQueue[T]) clearDeadLetters() {
	var zero T
	for index := range queue.dead {
		queue.dead[index].Value = zero
	}
	queue.dead = queue.dead[:0]
}
