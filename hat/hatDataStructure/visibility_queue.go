package hatDataStructure

import "time"

// DefaultVisibilityQueueTimeout is used by a zero-value queue and by
// constructors that receive a non-positive timeout.
const DefaultVisibilityQueueTimeout = time.Minute

// VisibilityQueueItem is a leased queue item returned by VisibilityQueue.Lease.
// The ID remains stable when the item is nacked or requeued after expiry.
type VisibilityQueueItem[T any] struct {
	ID         uint64
	Value      T
	Attempts   uint32
	LeaseUntil time.Time
}

type visibilityQueueEntry[T any] struct {
	id       uint64
	value    T
	attempts uint32
}

type visibilityQueueLease[T any] struct {
	entry       visibilityQueueEntry[T]
	until       time.Time
	expiryIndex int
}

type visibilityQueueExpiry struct {
	id    uint64
	until time.Time
	index int
}

type visibilityQueueExpiryHeap struct {
	items []visibilityQueueExpiry
}

// VisibilityQueue is a non-thread-safe queue with visibility timeouts.
//
// A leased item is hidden from other consumers until it is acknowledged,
// negatively acknowledged, or its visibility timeout expires. Capacity counts
// both pending and leased items. The zero value is ready for use and has an
// unbounded capacity with DefaultVisibilityQueueTimeout.
type VisibilityQueue[T any] struct {
	pending           *DelayQueue[visibilityQueueEntry[T]]
	expirations       visibilityQueueExpiryHeap
	leases            map[uint64]visibilityQueueLease[T]
	nextID            uint64
	capacity          int
	visibilityTimeout time.Duration
}

// NewVisibilityQueue creates a visibility queue. A non-positive capacity means
// unbounded, and a non-positive timeout selects DefaultVisibilityQueueTimeout.
func NewVisibilityQueue[T any](capacity int, visibilityTimeout time.Duration) *VisibilityQueue[T] {
	if capacity < 0 {
		capacity = 0
	}
	if visibilityTimeout <= 0 {
		visibilityTimeout = DefaultVisibilityQueueTimeout
	}
	return &VisibilityQueue[T]{
		pending:           NewDelayQueue[visibilityQueueEntry[T]](capacity),
		leases:            make(map[uint64]visibilityQueueLease[T]),
		capacity:          capacity,
		visibilityTimeout: visibilityTimeout,
	}
}

func (queue *VisibilityQueue[T]) pendingQueue() *DelayQueue[visibilityQueueEntry[T]] {
	if queue.pending == nil {
		queue.pending = NewDelayQueue[visibilityQueueEntry[T]](queue.capacity)
	}
	return queue.pending
}

func (queue *VisibilityQueue[T]) leaseMap() map[uint64]visibilityQueueLease[T] {
	if queue.leases == nil {
		queue.leases = make(map[uint64]visibilityQueueLease[T])
	}
	return queue.leases
}

func (queue *VisibilityQueue[T]) timeout() time.Duration {
	if queue.visibilityTimeout <= 0 {
		return DefaultVisibilityQueueTimeout
	}
	return queue.visibilityTimeout
}

// Len returns the number of pending and leased items.
func (queue *VisibilityQueue[T]) Len() int {
	if queue == nil {
		return 0
	}
	return queue.PendingLen() + queue.LeaseLen()
}

// PendingLen returns the number of items currently available to lease.
func (queue *VisibilityQueue[T]) PendingLen() int {
	if queue == nil || queue.pending == nil {
		return 0
	}
	return queue.pending.Len()
}

// LeaseLen returns the number of items currently hidden by an active lease.
func (queue *VisibilityQueue[T]) LeaseLen() int {
	if queue == nil {
		return 0
	}
	return len(queue.leases)
}

// Enqueue adds an immediately available item. It returns false if a positive
// capacity has already been reached.
func (queue *VisibilityQueue[T]) Enqueue(value T) bool {
	return queue.EnqueueAt(time.Time{}, value)
}

// EnqueueAt adds an item that becomes available at readyAt.
func (queue *VisibilityQueue[T]) EnqueueAt(readyAt time.Time, value T) bool {
	if queue == nil || !queue.hasCapacity() {
		return false
	}
	queue.pendingQueue().Push(readyAt, visibilityQueueEntry[T]{value: value})
	return true
}

// EnqueueAfter adds an item after delay has elapsed from now.
func (queue *VisibilityQueue[T]) EnqueueAfter(now time.Time, delay time.Duration, value T) bool {
	return queue.EnqueueAt(now.Add(delay), value)
}

func (queue *VisibilityQueue[T]) hasCapacity() bool {
	return queue.capacity <= 0 || queue.Len() < queue.capacity
}

func (queue *VisibilityQueue[T]) nextLeaseID() (uint64, bool) {
	leases := queue.leaseMap()
	for {
		queue.nextID++
		if queue.nextID == 0 {
			queue.nextID++
		}
		if _, exists := leases[queue.nextID]; !exists {
			return queue.nextID, true
		}
	}
}

// Lease returns the next ready item and hides it until the queue's configured
// visibility timeout expires.
func (queue *VisibilityQueue[T]) Lease(now time.Time) (VisibilityQueueItem[T], bool) {
	return queue.LeaseFor(now, queue.timeout())
}

// LeaseFor returns the next ready item and uses timeout for this lease. A
// non-positive timeout uses the queue's configured timeout.
func (queue *VisibilityQueue[T]) LeaseFor(now time.Time, timeout time.Duration) (VisibilityQueueItem[T], bool) {
	var item VisibilityQueueItem[T]
	if queue == nil {
		return item, false
	}
	queue.RequeueExpired(now)
	entry, ok := queue.pendingQueue().PopReady(now)
	if !ok {
		return item, false
	}
	if entry.id == 0 {
		entry.id, ok = queue.nextLeaseID()
		if !ok {
			queue.pendingQueue().Push(now, entry)
			return item, false
		}
	}
	if entry.attempts != ^uint32(0) {
		entry.attempts++
	}
	if timeout <= 0 {
		timeout = queue.timeout()
	}
	until := now.Add(timeout)
	lease := visibilityQueueLease[T]{
		entry:       entry,
		until:       until,
		expiryIndex: len(queue.expirations.items),
	}
	queue.leaseMap()[entry.id] = lease
	queue.expiryPush(visibilityQueueExpiry{id: entry.id, until: until})
	item = VisibilityQueueItem[T]{
		ID:         entry.id,
		Value:      entry.value,
		Attempts:   entry.attempts,
		LeaseUntil: until,
	}
	return item, true
}

// Ack permanently removes an active lease. It returns false for an unknown or
// already completed lease.
func (queue *VisibilityQueue[T]) Ack(id uint64) bool {
	if queue == nil || id == 0 {
		return false
	}
	lease, ok := queue.leases[id]
	if !ok {
		return false
	}
	queue.expiryRemove(lease.expiryIndex)
	delete(queue.leases, id)
	return true
}

// Nack makes an active lease available again at readyAt. The item ID and
// attempt count are retained for retry tracking.
func (queue *VisibilityQueue[T]) Nack(id uint64, readyAt time.Time) bool {
	if queue == nil || id == 0 {
		return false
	}
	lease, ok := queue.leases[id]
	if !ok {
		return false
	}
	queue.expiryRemove(lease.expiryIndex)
	delete(queue.leases, id)
	queue.pendingQueue().Push(readyAt, lease.entry)
	return true
}

// RequeueExpired makes every lease whose deadline is at or before now
// available immediately and returns the number of leases recovered.
func (queue *VisibilityQueue[T]) RequeueExpired(now time.Time) int {
	if queue == nil {
		return 0
	}
	recovered := 0
	for len(queue.expirations.items) > 0 {
		expiry := queue.expirations.items[0]
		if expiry.until.After(now) {
			break
		}
		expiry = queue.expiryPop()
		lease, ok := queue.leases[expiry.id]
		if !ok || !lease.until.Equal(expiry.until) {
			continue
		}
		delete(queue.leases, expiry.id)
		queue.pendingQueue().Push(now, lease.entry)
		recovered++
	}
	return recovered
}

// Clear removes pending items and active leases without reusing lease IDs.
func (queue *VisibilityQueue[T]) Clear() {
	if queue == nil {
		return
	}
	if queue.pending != nil {
		queue.pending.Clear()
	}
	queue.expirations.items = nil
	queue.leases = nil
}

func visibilityQueueExpiryBefore(left, right visibilityQueueExpiry) bool {
	if left.until.Equal(right.until) {
		return left.id < right.id
	}
	return left.until.Before(right.until)
}

func (queue *VisibilityQueue[T]) updateExpiryIndex(id uint64, index int) {
	if lease, ok := queue.leases[id]; ok {
		lease.expiryIndex = index
		queue.leases[id] = lease
	}
}

func (queue *VisibilityQueue[T]) expirySwap(left, right int) {
	queue.expirations.items[left], queue.expirations.items[right] = queue.expirations.items[right], queue.expirations.items[left]
	queue.expirations.items[left].index = left
	queue.expirations.items[right].index = right
	queue.updateExpiryIndex(queue.expirations.items[left].id, left)
	queue.updateExpiryIndex(queue.expirations.items[right].id, right)
}

func (queue *VisibilityQueue[T]) expiryPush(expiry visibilityQueueExpiry) {
	expiry.index = len(queue.expirations.items)
	queue.expirations.items = append(queue.expirations.items, expiry)
	queue.expirySiftUp(expiry.index)
}

func (queue *VisibilityQueue[T]) expirySiftUp(index int) bool {
	moved := false
	for index > 0 {
		parent := (index - 1) / 4
		if !visibilityQueueExpiryBefore(queue.expirations.items[index], queue.expirations.items[parent]) {
			break
		}
		queue.expirySwap(index, parent)
		index = parent
		moved = true
	}
	return moved
}

func (queue *VisibilityQueue[T]) expirySiftDown(index int) {
	items := queue.expirations.items
	for {
		firstChild := index*4 + 1
		if firstChild >= len(items) {
			return
		}
		best := firstChild
		lastChild := firstChild + 4
		if lastChild > len(items) {
			lastChild = len(items)
		}
		for child := firstChild + 1; child < lastChild; child++ {
			if visibilityQueueExpiryBefore(items[child], items[best]) {
				best = child
			}
		}
		if !visibilityQueueExpiryBefore(items[best], items[index]) {
			return
		}
		queue.expirySwap(index, best)
		index = best
		items = queue.expirations.items
	}
}

func (queue *VisibilityQueue[T]) expiryFix(index int) {
	if !queue.expirySiftUp(index) {
		queue.expirySiftDown(index)
	}
}

func (queue *VisibilityQueue[T]) expiryPop() visibilityQueueExpiry {
	last := len(queue.expirations.items) - 1
	root := queue.expirations.items[0]
	if last == 0 {
		queue.expirations.items = queue.expirations.items[:0]
		root.index = -1
		return root
	}
	queue.expirations.items[0] = queue.expirations.items[last]
	queue.expirations.items[0].index = 0
	queue.updateExpiryIndex(queue.expirations.items[0].id, 0)
	queue.expirations.items = queue.expirations.items[:last]
	root.index = -1
	queue.expirySiftDown(0)
	return root
}

func (queue *VisibilityQueue[T]) expiryRemove(index int) {
	if index < 0 || index >= len(queue.expirations.items) {
		return
	}
	last := len(queue.expirations.items) - 1
	removed := queue.expirations.items[index]
	if index == last {
		queue.expirations.items = queue.expirations.items[:last]
		removed.index = -1
		return
	}
	queue.expirations.items[index] = queue.expirations.items[last]
	queue.expirations.items[index].index = index
	queue.updateExpiryIndex(queue.expirations.items[index].id, index)
	queue.expirations.items = queue.expirations.items[:last]
	removed.index = -1
	queue.expiryFix(index)
}
