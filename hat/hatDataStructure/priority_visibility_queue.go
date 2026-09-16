package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	priorityVisibilityQueueFormatVersion    byte = 1
	priorityVisibilityQueueHeaderSize            = 52
	priorityVisibilityQueueChecksumSize          = 4
	priorityVisibilityQueueMaxEntries            = 1_000_000
	priorityVisibilityQueueMaxValueBytes         = 64 << 20
	priorityVisibilityQueueMaxSnapshotBytes      = 512 << 20
)

var priorityVisibilityQueueMagic = [4]byte{'H', 'P', 'Q', '1'}

var (
	errPriorityVisibilityQueueNilCodec       = errors.New("hatriecache: priority visibility queue codec is incomplete")
	errPriorityVisibilityQueueInvalidFormat  = errors.New("hatriecache: invalid priority visibility queue snapshot")
	errPriorityVisibilityQueueSnapshotTooBig = errors.New("hatriecache: priority visibility queue snapshot is too large")
)

// PriorityVisibilityQueueCodec encodes queue values for binary snapshots.
// The codec is caller-owned so applications can use a compact format such as
// protobuf or a fixed-width binary representation instead of JSON.
type PriorityVisibilityQueueCodec[T any] struct {
	Encode func(T) ([]byte, error)
	Decode func([]byte) (T, error)
}

// PriorityVisibilityQueueLeaseToken identifies a lease across a process or
// storage handoff. It is an alias of VisibilityQueueLeaseToken so callers can
// share epoch-fencing metadata between the two queue types.
type PriorityVisibilityQueueLeaseToken = VisibilityQueueLeaseToken

// PriorityVisibilityQueueItem is returned by Lease and LeaseFor.
// Lower numeric priorities are leased first; equal priorities are FIFO.
type PriorityVisibilityQueueItem[T any] struct {
	ID         uint64
	Priority   int64
	Value      T
	Attempts   uint32
	LeaseUntil time.Time
}

// PriorityVisibilityQueueLease is returned by LeaseWithToken and carries an
// epoch-fenced token for AckToken and NackToken.
type PriorityVisibilityQueueLease[T any] struct {
	Token      PriorityVisibilityQueueLeaseToken
	Priority   int64
	Value      T
	Attempts   uint32
	LeaseUntil time.Time
}

type priorityVisibilityQueueEntry[T any] struct {
	id       uint64
	priority int64
	sequence uint64
	value    T
	attempts uint32
}

type priorityVisibilityQueueLease[T any] struct {
	entry       priorityVisibilityQueueEntry[T]
	until       time.Time
	expiryIndex int
}

type priorityVisibilityQueueExpiry struct {
	id    uint64
	until time.Time
	index int
}

type priorityVisibilityQueueExpiryHeap struct {
	items []priorityVisibilityQueueExpiry
}

// PriorityVisibilityQueueSnapshotItem is one pending item in a queue
// snapshot. A zero ReadyAt means the item is immediately ready.
type PriorityVisibilityQueueSnapshotItem[T any] struct {
	ID       uint64
	Priority int64
	ReadyAt  time.Time
	Sequence uint64
	Value    T
	Attempts uint32
}

// PriorityVisibilityQueueLeaseSnapshot is one active lease in a queue
// snapshot.
type PriorityVisibilityQueueLeaseSnapshot[T any] struct {
	ID         uint64
	Priority   int64
	Sequence   uint64
	Value      T
	Attempts   uint32
	LeaseUntil time.Time
}

// PriorityVisibilityQueueSnapshot is an in-memory checkpoint of a
// PriorityVisibilityQueue. It includes pending work and active leases.
type PriorityVisibilityQueueSnapshot[T any] struct {
	Capacity          int
	VisibilityTimeout time.Duration
	Epoch             uint64
	NextID            uint64
	NextSequence      uint64
	Pending           []PriorityVisibilityQueueSnapshotItem[T]
	Leases            []PriorityVisibilityQueueLeaseSnapshot[T]
}

// PriorityVisibilityQueue is a non-thread-safe priority queue with delayed
// readiness, visibility timeouts, retry attempts, and explicit snapshots.
// Lower numeric priorities are leased first and equal priorities preserve FIFO
// insertion order. Capacity counts pending and leased items.
//
// The zero value is ready for use with unbounded capacity,
// DefaultVisibilityQueueTimeout, and DefaultVisibilityQueueEpoch.
type PriorityVisibilityQueue[T any] struct {
	delayed           *DelayQueue[priorityVisibilityQueueEntry[T]]
	ready             []priorityVisibilityQueueEntry[T]
	expirations       priorityVisibilityQueueExpiryHeap
	leases            map[uint64]priorityVisibilityQueueLease[T]
	nextID            uint64
	nextSequence      uint64
	capacity          int
	visibilityTimeout time.Duration
	epoch             uint64
}

// NewPriorityVisibilityQueue creates a priority visibility queue. A
// non-positive capacity means unbounded and a non-positive timeout selects
// DefaultVisibilityQueueTimeout.
func NewPriorityVisibilityQueue[T any](capacity int, visibilityTimeout time.Duration) *PriorityVisibilityQueue[T] {
	return NewPriorityVisibilityQueueWithEpoch[T](capacity, visibilityTimeout, DefaultVisibilityQueueEpoch)
}

// NewPriorityVisibilityQueueWithEpoch creates a priority visibility queue with
// an explicit lease epoch. Restore a queue with a new epoch when old process
// or storage tokens must be rejected.
func NewPriorityVisibilityQueueWithEpoch[T any](capacity int, visibilityTimeout time.Duration, epoch uint64) *PriorityVisibilityQueue[T] {
	if capacity < 0 {
		capacity = 0
	}
	if visibilityTimeout <= 0 {
		visibilityTimeout = DefaultVisibilityQueueTimeout
	}
	if epoch == 0 {
		epoch = DefaultVisibilityQueueEpoch
	}
	return &PriorityVisibilityQueue[T]{
		delayed:           NewDelayQueue[priorityVisibilityQueueEntry[T]](capacity),
		leases:            make(map[uint64]priorityVisibilityQueueLease[T]),
		capacity:          capacity,
		visibilityTimeout: visibilityTimeout,
		epoch:             epoch,
	}
}

func (queue *PriorityVisibilityQueue[T]) delayedQueue() *DelayQueue[priorityVisibilityQueueEntry[T]] {
	if queue.delayed == nil {
		queue.delayed = NewDelayQueue[priorityVisibilityQueueEntry[T]](queue.capacity)
	}
	return queue.delayed
}

func (queue *PriorityVisibilityQueue[T]) leaseMap() map[uint64]priorityVisibilityQueueLease[T] {
	if queue.leases == nil {
		queue.leases = make(map[uint64]priorityVisibilityQueueLease[T])
	}
	return queue.leases
}

func (queue *PriorityVisibilityQueue[T]) timeout() time.Duration {
	if queue.visibilityTimeout <= 0 {
		queue.visibilityTimeout = DefaultVisibilityQueueTimeout
	}
	return queue.visibilityTimeout
}

// Epoch returns the queue's lease epoch.
func (queue *PriorityVisibilityQueue[T]) Epoch() uint64 {
	if queue == nil {
		return 0
	}
	if queue.epoch == 0 {
		queue.epoch = DefaultVisibilityQueueEpoch
	}
	return queue.epoch
}

// Len returns the number of pending and leased items.
func (queue *PriorityVisibilityQueue[T]) Len() int {
	if queue == nil {
		return 0
	}
	return queue.PendingLen() + queue.LeaseLen()
}

// PendingLen returns the number of queued items, including delayed items that
// are not ready yet.
func (queue *PriorityVisibilityQueue[T]) PendingLen() int {
	if queue == nil {
		return 0
	}
	if queue.delayed == nil {
		return len(queue.ready)
	}
	return len(queue.ready) + queue.delayed.Len()
}

// ReadyLen returns the number of items currently ready to lease. Delayed items
// are promoted when a lease operation supplies the current time.
func (queue *PriorityVisibilityQueue[T]) ReadyLen() int {
	if queue == nil {
		return 0
	}
	return len(queue.ready)
}

// LeaseLen returns the number of items currently hidden by active leases.
func (queue *PriorityVisibilityQueue[T]) LeaseLen() int {
	if queue == nil {
		return 0
	}
	return len(queue.leases)
}

// Enqueue adds an immediately ready item.
func (queue *PriorityVisibilityQueue[T]) Enqueue(priority int64, value T) bool {
	return queue.EnqueueAt(priority, time.Time{}, value)
}

// EnqueueAt adds an item that becomes ready at readyAt. A zero time means the
// item is immediately ready.
func (queue *PriorityVisibilityQueue[T]) EnqueueAt(priority int64, readyAt time.Time, value T) bool {
	if queue == nil || !queue.hasCapacity() {
		return false
	}
	if queue.nextID == ^uint64(0) || queue.nextSequence == ^uint64(0) {
		return false
	}
	id, ok := queue.nextEntryID()
	if !ok {
		return false
	}
	sequence, ok := queue.nextEntrySequence()
	if !ok {
		return false
	}
	entry := priorityVisibilityQueueEntry[T]{
		id:       id,
		priority: priority,
		sequence: sequence,
		value:    value,
	}
	if readyAt.IsZero() {
		queue.readyPush(entry)
	} else {
		queue.delayedQueue().Push(readyAt, entry)
	}
	return true
}

// EnqueueAfter adds an item after delay has elapsed from now.
func (queue *PriorityVisibilityQueue[T]) EnqueueAfter(priority int64, now time.Time, delay time.Duration, value T) bool {
	return queue.EnqueueAt(priority, now.Add(delay), value)
}

func (queue *PriorityVisibilityQueue[T]) hasCapacity() bool {
	return queue.capacity <= 0 || queue.Len() < queue.capacity
}

func (queue *PriorityVisibilityQueue[T]) nextEntryID() (uint64, bool) {
	if queue.nextID == ^uint64(0) {
		return 0, false
	}
	queue.nextID++
	return queue.nextID, true
}

func (queue *PriorityVisibilityQueue[T]) nextEntrySequence() (uint64, bool) {
	if queue.nextSequence == ^uint64(0) {
		return 0, false
	}
	queue.nextSequence++
	return queue.nextSequence, true
}

// Lease returns the highest-priority ready item and hides it until the queue's
// configured visibility timeout expires.
func (queue *PriorityVisibilityQueue[T]) Lease(now time.Time) (PriorityVisibilityQueueItem[T], bool) {
	return queue.LeaseFor(now, queue.timeout())
}

// LeaseWithToken returns the next ready item with an epoch-fenced token.
func (queue *PriorityVisibilityQueue[T]) LeaseWithToken(now time.Time) (PriorityVisibilityQueueLease[T], bool) {
	item, ok := queue.Lease(now)
	if !ok {
		return PriorityVisibilityQueueLease[T]{}, false
	}
	return PriorityVisibilityQueueLease[T]{
		Token:      PriorityVisibilityQueueLeaseToken{Epoch: queue.Epoch(), ID: item.ID},
		Priority:   item.Priority,
		Value:      item.Value,
		Attempts:   item.Attempts,
		LeaseUntil: item.LeaseUntil,
	}, true
}

// LeaseFor returns the next ready item and uses timeout for this lease. A
// non-positive timeout uses the queue's configured timeout.
func (queue *PriorityVisibilityQueue[T]) LeaseFor(now time.Time, timeout time.Duration) (PriorityVisibilityQueueItem[T], bool) {
	var item PriorityVisibilityQueueItem[T]
	if queue == nil {
		return item, false
	}
	queue.RequeueExpired(now)
	queue.promoteReady(now)
	entry, ok := queue.readyPop()
	if !ok {
		return item, false
	}
	if entry.attempts != ^uint32(0) {
		entry.attempts++
	}
	if timeout <= 0 {
		timeout = queue.timeout()
	}
	until := now.Add(timeout)
	lease := priorityVisibilityQueueLease[T]{
		entry:       entry,
		until:       until,
		expiryIndex: len(queue.expirations.items),
	}
	queue.leaseMap()[entry.id] = lease
	queue.expiryPush(priorityVisibilityQueueExpiry{id: entry.id, until: until})
	return PriorityVisibilityQueueItem[T]{
		ID:         entry.id,
		Priority:   entry.priority,
		Value:      entry.value,
		Attempts:   entry.attempts,
		LeaseUntil: until,
	}, true
}

// Ack permanently removes an active lease.
func (queue *PriorityVisibilityQueue[T]) Ack(id uint64) bool {
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

// AckToken permanently removes an active lease when its epoch and ID match.
func (queue *PriorityVisibilityQueue[T]) AckToken(token PriorityVisibilityQueueLeaseToken) bool {
	if queue == nil || token.ID == 0 || token.Epoch == 0 || token.Epoch != queue.Epoch() {
		return false
	}
	return queue.Ack(token.ID)
}

// Nack makes an active lease available again at readyAt. Priority, ID, and
// retry attempts are retained.
func (queue *PriorityVisibilityQueue[T]) Nack(id uint64, readyAt time.Time) bool {
	if queue == nil || id == 0 {
		return false
	}
	lease, ok := queue.leases[id]
	if !ok {
		return false
	}
	queue.expiryRemove(lease.expiryIndex)
	delete(queue.leases, id)
	queue.requeueEntry(readyAt, lease.entry)
	return true
}

// NackToken makes an active lease available again when its epoch and ID match.
func (queue *PriorityVisibilityQueue[T]) NackToken(token PriorityVisibilityQueueLeaseToken, readyAt time.Time) bool {
	if queue == nil || token.ID == 0 || token.Epoch == 0 || token.Epoch != queue.Epoch() {
		return false
	}
	return queue.Nack(token.ID, readyAt)
}

// RequeueExpired makes every lease whose deadline is at or before now ready
// immediately and returns the number of recovered leases.
func (queue *PriorityVisibilityQueue[T]) RequeueExpired(now time.Time) int {
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
		queue.readyPush(lease.entry)
		recovered++
	}
	return recovered
}

func (queue *PriorityVisibilityQueue[T]) requeueEntry(readyAt time.Time, entry priorityVisibilityQueueEntry[T]) {
	if readyAt.IsZero() {
		queue.readyPush(entry)
		return
	}
	queue.delayedQueue().Push(readyAt, entry)
}

func (queue *PriorityVisibilityQueue[T]) promoteReady(now time.Time) {
	for queue.delayed != nil {
		item, ok := queue.delayed.Peek()
		if !ok || item.ReadyAt.After(now) {
			return
		}
		item, _ = queue.delayed.Pop()
		queue.readyPush(item.Value)
	}
}

// Clear removes pending items and active leases without reusing IDs or FIFO
// sequence numbers.
func (queue *PriorityVisibilityQueue[T]) Clear() {
	if queue == nil {
		return
	}
	if queue.delayed != nil {
		queue.delayed.Clear()
	}
	var zero priorityVisibilityQueueEntry[T]
	for index := range queue.ready {
		queue.ready[index] = zero
	}
	queue.ready = queue.ready[:0]
	queue.expirations.items = nil
	queue.leases = nil
}

func (queue *PriorityVisibilityQueue[T]) readyPush(entry priorityVisibilityQueueEntry[T]) {
	queue.ready = append(queue.ready, entry)
	queue.readySiftUp(len(queue.ready) - 1)
}

func (queue *PriorityVisibilityQueue[T]) readyPop() (priorityVisibilityQueueEntry[T], bool) {
	if len(queue.ready) == 0 {
		return priorityVisibilityQueueEntry[T]{}, false
	}
	root := queue.ready[0]
	last := len(queue.ready) - 1
	lastEntry := queue.ready[last]
	var zero priorityVisibilityQueueEntry[T]
	queue.ready[last] = zero
	queue.ready = queue.ready[:last]
	if len(queue.ready) > 0 {
		queue.ready[0] = lastEntry
		queue.readySiftDown(0)
	}
	return root, true
}

func (queue *PriorityVisibilityQueue[T]) readySiftUp(index int) {
	for index > 0 {
		parent := (index - 1) / 4
		if !priorityVisibilityQueueEntryBeforeAny(queue.ready[index], queue.ready[parent]) {
			return
		}
		queue.ready[index], queue.ready[parent] = queue.ready[parent], queue.ready[index]
		index = parent
	}
}

func (queue *PriorityVisibilityQueue[T]) readySiftDown(index int) {
	for {
		left := index*4 + 1
		if left >= len(queue.ready) {
			return
		}
		best := left
		last := left + 4
		if last > len(queue.ready) {
			last = len(queue.ready)
		}
		for child := left + 1; child < last; child++ {
			if priorityVisibilityQueueEntryBeforeAny(queue.ready[child], queue.ready[best]) {
				best = child
			}
		}
		if !priorityVisibilityQueueEntryBeforeAny(queue.ready[best], queue.ready[index]) {
			return
		}
		queue.ready[index], queue.ready[best] = queue.ready[best], queue.ready[index]
		index = best
	}
}

func priorityVisibilityQueueEntryBeforeAny[T any](left, right priorityVisibilityQueueEntry[T]) bool {
	if left.priority != right.priority {
		return left.priority < right.priority
	}
	return left.sequence < right.sequence
}

func (queue *PriorityVisibilityQueue[T]) expiryPush(expiry priorityVisibilityQueueExpiry) {
	queue.expirations.items = append(queue.expirations.items, expiry)
	queue.expiryUpdateIndex(len(queue.expirations.items) - 1)
	queue.expirySiftUp(len(queue.expirations.items) - 1)
}

func (queue *PriorityVisibilityQueue[T]) expirySiftUp(index int) {
	for index > 0 {
		parent := (index - 1) / 4
		if !priorityVisibilityQueueExpiryBefore(queue.expirations.items[index], queue.expirations.items[parent]) {
			return
		}
		queue.expirations.items[index], queue.expirations.items[parent] = queue.expirations.items[parent], queue.expirations.items[index]
		queue.expiryUpdateIndex(index)
		queue.expiryUpdateIndex(parent)
		index = parent
	}
}

func (queue *PriorityVisibilityQueue[T]) expirySiftDown(index int) {
	for {
		left := index*4 + 1
		if left >= len(queue.expirations.items) {
			return
		}
		best := left
		last := left + 4
		if last > len(queue.expirations.items) {
			last = len(queue.expirations.items)
		}
		for child := left + 1; child < last; child++ {
			if priorityVisibilityQueueExpiryBefore(queue.expirations.items[child], queue.expirations.items[best]) {
				best = child
			}
		}
		if !priorityVisibilityQueueExpiryBefore(queue.expirations.items[best], queue.expirations.items[index]) {
			return
		}
		queue.expirations.items[index], queue.expirations.items[best] = queue.expirations.items[best], queue.expirations.items[index]
		queue.expiryUpdateIndex(index)
		queue.expiryUpdateIndex(best)
		index = best
	}
}

func priorityVisibilityQueueExpiryBefore(left, right priorityVisibilityQueueExpiry) bool {
	if left.until.Before(right.until) {
		return true
	}
	if right.until.Before(left.until) {
		return false
	}
	return left.id < right.id
}

func (queue *PriorityVisibilityQueue[T]) expiryUpdateIndex(index int) {
	if index < len(queue.expirations.items) {
		expiry := queue.expirations.items[index]
		if lease, ok := queue.leases[expiry.id]; ok {
			lease.expiryIndex = index
			queue.leases[expiry.id] = lease
		}
	}
}

func (queue *PriorityVisibilityQueue[T]) expiryPop() priorityVisibilityQueueExpiry {
	root := queue.expirations.items[0]
	last := len(queue.expirations.items) - 1
	lastExpiry := queue.expirations.items[last]
	queue.expirations.items = queue.expirations.items[:last]
	if len(queue.expirations.items) > 0 {
		queue.expirations.items[0] = lastExpiry
		queue.expiryUpdateIndex(0)
		queue.expirySiftDown(0)
	}
	return root
}

func (queue *PriorityVisibilityQueue[T]) expiryRemove(index int) {
	if index < 0 || index >= len(queue.expirations.items) {
		return
	}
	last := len(queue.expirations.items) - 1
	if index == last {
		queue.expirations.items = queue.expirations.items[:last]
		return
	}
	queue.expirations.items[index] = queue.expirations.items[last]
	queue.expirations.items = queue.expirations.items[:last]
	queue.expiryUpdateIndex(index)
	if index > 0 && priorityVisibilityQueueExpiryBefore(queue.expirations.items[index], queue.expirations.items[(index-1)/4]) {
		queue.expirySiftUp(index)
		return
	}
	queue.expirySiftDown(index)
}

// Snapshot returns a checkpoint that can be validated and restored without
// modifying the queue. Values are copied according to Go's normal assignment
// rules; callers needing deep copies should make them in their codec.
func (queue *PriorityVisibilityQueue[T]) Snapshot() PriorityVisibilityQueueSnapshot[T] {
	if queue == nil {
		return PriorityVisibilityQueueSnapshot[T]{}
	}
	snapshot := PriorityVisibilityQueueSnapshot[T]{
		Capacity:          queue.capacity,
		VisibilityTimeout: queue.timeout(),
		Epoch:             queue.Epoch(),
		NextID:            queue.nextID,
		NextSequence:      queue.nextSequence,
		Pending:           make([]PriorityVisibilityQueueSnapshotItem[T], 0, queue.PendingLen()),
		Leases:            make([]PriorityVisibilityQueueLeaseSnapshot[T], 0, len(queue.leases)),
	}
	for _, entry := range queue.ready {
		snapshot.Pending = append(snapshot.Pending, PriorityVisibilityQueueSnapshotItem[T]{
			ID:       entry.id,
			Priority: entry.priority,
			Sequence: entry.sequence,
			Value:    entry.value,
			Attempts: entry.attempts,
		})
	}
	if queue.delayed != nil {
		for _, item := range queue.delayed.items {
			snapshot.Pending = append(snapshot.Pending, PriorityVisibilityQueueSnapshotItem[T]{
				ID:       item.Value.id,
				Priority: item.Value.priority,
				ReadyAt:  item.ReadyAt,
				Sequence: item.Value.sequence,
				Value:    item.Value.value,
				Attempts: item.Value.attempts,
			})
		}
	}
	for _, lease := range queue.leases {
		snapshot.Leases = append(snapshot.Leases, PriorityVisibilityQueueLeaseSnapshot[T]{
			ID:         lease.entry.id,
			Priority:   lease.entry.priority,
			Sequence:   lease.entry.sequence,
			Value:      lease.entry.value,
			Attempts:   lease.entry.attempts,
			LeaseUntil: lease.until,
		})
	}
	sort.Slice(snapshot.Pending, func(left, right int) bool {
		return snapshot.Pending[left].ID < snapshot.Pending[right].ID
	})
	sort.Slice(snapshot.Leases, func(left, right int) bool {
		return snapshot.Leases[left].ID < snapshot.Leases[right].ID
	})
	return snapshot
}

// ValidatePriorityVisibilityQueueSnapshot checks bounds, identity, and
// ordering metadata before a snapshot is restored.
func ValidatePriorityVisibilityQueueSnapshot[T any](snapshot PriorityVisibilityQueueSnapshot[T]) error {
	if snapshot.Capacity < 0 {
		return errors.New("hatriecache: priority visibility queue capacity is negative")
	}
	if snapshot.VisibilityTimeout <= 0 {
		return errors.New("hatriecache: priority visibility queue timeout is not positive")
	}
	if snapshot.Epoch == 0 {
		return errors.New("hatriecache: priority visibility queue epoch is zero")
	}
	if len(snapshot.Pending) > priorityVisibilityQueueMaxEntries || len(snapshot.Leases) > priorityVisibilityQueueMaxEntries {
		return errors.New("hatriecache: priority visibility queue has too many entries")
	}
	if snapshot.Capacity > 0 && len(snapshot.Pending)+len(snapshot.Leases) > snapshot.Capacity {
		return errors.New("hatriecache: priority visibility queue exceeds capacity")
	}
	ids := make(map[uint64]struct{}, len(snapshot.Pending)+len(snapshot.Leases))
	sequences := make(map[uint64]struct{}, len(snapshot.Pending)+len(snapshot.Leases))
	var maxID, maxSequence uint64
	for _, item := range snapshot.Pending {
		if item.ID == 0 || item.Sequence == 0 {
			return errors.New("hatriecache: priority visibility queue pending identity is zero")
		}
		if _, exists := ids[item.ID]; exists {
			return errors.New("hatriecache: priority visibility queue has duplicate IDs")
		}
		if _, exists := sequences[item.Sequence]; exists {
			return errors.New("hatriecache: priority visibility queue has duplicate sequences")
		}
		ids[item.ID] = struct{}{}
		sequences[item.Sequence] = struct{}{}
		if item.ID > maxID {
			maxID = item.ID
		}
		if item.Sequence > maxSequence {
			maxSequence = item.Sequence
		}
	}
	for _, item := range snapshot.Leases {
		if item.ID == 0 || item.Sequence == 0 || item.LeaseUntil.IsZero() {
			return errors.New("hatriecache: priority visibility queue lease identity or deadline is invalid")
		}
		if _, exists := ids[item.ID]; exists {
			return errors.New("hatriecache: priority visibility queue has duplicate IDs")
		}
		if _, exists := sequences[item.Sequence]; exists {
			return errors.New("hatriecache: priority visibility queue has duplicate sequences")
		}
		ids[item.ID] = struct{}{}
		sequences[item.Sequence] = struct{}{}
		if item.ID > maxID {
			maxID = item.ID
		}
		if item.Sequence > maxSequence {
			maxSequence = item.Sequence
		}
	}
	if maxID > snapshot.NextID || maxSequence > snapshot.NextSequence {
		return errors.New("hatriecache: priority visibility queue next counters are behind entries")
	}
	return nil
}

// RestorePriorityVisibilityQueue restores an in-memory snapshot. A non-zero
// epoch overrides the snapshot epoch, which is the recommended way to fence
// leases after a restart.
func RestorePriorityVisibilityQueue[T any](snapshot PriorityVisibilityQueueSnapshot[T], epoch uint64) (*PriorityVisibilityQueue[T], error) {
	if err := ValidatePriorityVisibilityQueueSnapshot(snapshot); err != nil {
		return nil, err
	}
	if epoch == 0 {
		epoch = snapshot.Epoch
	}
	queue := NewPriorityVisibilityQueueWithEpoch[T](snapshot.Capacity, snapshot.VisibilityTimeout, epoch)
	queue.nextID = snapshot.NextID
	queue.nextSequence = snapshot.NextSequence
	pending := append([]PriorityVisibilityQueueSnapshotItem[T](nil), snapshot.Pending...)
	sort.Slice(pending, func(left, right int) bool {
		if pending[left].ReadyAt.IsZero() != pending[right].ReadyAt.IsZero() {
			return pending[left].ReadyAt.IsZero()
		}
		if pending[left].ReadyAt.Equal(pending[right].ReadyAt) {
			return pending[left].Sequence < pending[right].Sequence
		}
		return pending[left].ReadyAt.Before(pending[right].ReadyAt)
	})
	for _, item := range pending {
		entry := priorityVisibilityQueueEntry[T]{
			id:       item.ID,
			priority: item.Priority,
			sequence: item.Sequence,
			value:    item.Value,
			attempts: item.Attempts,
		}
		if item.ReadyAt.IsZero() {
			queue.readyPush(entry)
		} else {
			queue.delayedQueue().Push(item.ReadyAt, entry)
		}
	}
	for _, item := range snapshot.Leases {
		entry := priorityVisibilityQueueEntry[T]{
			id:       item.ID,
			priority: item.Priority,
			sequence: item.Sequence,
			value:    item.Value,
			attempts: item.Attempts,
		}
		queue.leases[item.ID] = priorityVisibilityQueueLease[T]{
			entry:       entry,
			until:       item.LeaseUntil,
			expiryIndex: len(queue.expirations.items),
		}
		queue.expiryPush(priorityVisibilityQueueExpiry{id: item.ID, until: item.LeaseUntil})
	}
	return queue, nil
}

// MarshalSnapshot encodes the queue into a bounded CRC-protected binary
// snapshot using the caller-provided value codec.
func (queue *PriorityVisibilityQueue[T]) MarshalSnapshot(codec PriorityVisibilityQueueCodec[T]) ([]byte, error) {
	if queue == nil || codec.Encode == nil {
		return nil, errPriorityVisibilityQueueNilCodec
	}
	snapshot := queue.Snapshot()
	if err := ValidatePriorityVisibilityQueueSnapshot(snapshot); err != nil {
		return nil, err
	}
	if snapshot.Capacity > int(^uint32(0)) {
		return nil, errors.New("hatriecache: priority visibility queue capacity does not fit snapshot")
	}
	if len(snapshot.Pending) > int(^uint32(0)) || len(snapshot.Leases) > int(^uint32(0)) {
		return nil, errPriorityVisibilityQueueSnapshotTooBig
	}
	encoded := make([]byte, 0, priorityVisibilityQueueHeaderSize+priorityVisibilityQueueChecksumSize+(len(snapshot.Pending)+len(snapshot.Leases))*64)
	header := make([]byte, priorityVisibilityQueueHeaderSize)
	copy(header[:4], priorityVisibilityQueueMagic[:])
	header[4] = priorityVisibilityQueueFormatVersion
	binary.LittleEndian.PutUint32(header[8:12], uint32(snapshot.Capacity))
	binary.LittleEndian.PutUint64(header[12:20], uint64(int64(snapshot.VisibilityTimeout)))
	binary.LittleEndian.PutUint64(header[20:28], snapshot.Epoch)
	binary.LittleEndian.PutUint64(header[28:36], snapshot.NextID)
	binary.LittleEndian.PutUint64(header[36:44], snapshot.NextSequence)
	binary.LittleEndian.PutUint32(header[44:48], uint32(len(snapshot.Pending)))
	binary.LittleEndian.PutUint32(header[48:52], uint32(len(snapshot.Leases)))
	encoded = append(encoded, header...)
	for _, item := range snapshot.Pending {
		value, err := codec.Encode(item.Value)
		if err != nil {
			return nil, fmt.Errorf("hatriecache: encode pending priority visibility queue value: %w", err)
		}
		encoded, err = appendPriorityVisibilityQueueRecord(encoded, item.ID, item.Priority, item.Sequence, item.Attempts, item.ReadyAt, value)
		if err != nil {
			return nil, err
		}
	}
	for _, item := range snapshot.Leases {
		value, err := codec.Encode(item.Value)
		if err != nil {
			return nil, fmt.Errorf("hatriecache: encode leased priority visibility queue value: %w", err)
		}
		encoded, err = appendPriorityVisibilityQueueRecord(encoded, item.ID, item.Priority, item.Sequence, item.Attempts, item.LeaseUntil, value)
		if err != nil {
			return nil, err
		}
	}
	if len(encoded) > priorityVisibilityQueueMaxSnapshotBytes-priorityVisibilityQueueChecksumSize {
		return nil, errPriorityVisibilityQueueSnapshotTooBig
	}
	checksum := crc32.Checksum(encoded, crc32.IEEETable)
	var checksumBytes [priorityVisibilityQueueChecksumSize]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	encoded = append(encoded, checksumBytes[:]...)
	return encoded, nil
}

func appendPriorityVisibilityQueueRecord(destination []byte, id uint64, priority int64, sequence uint64, attempts uint32, timestamp time.Time, value []byte) ([]byte, error) {
	if len(value) > priorityVisibilityQueueMaxValueBytes {
		return nil, errors.New("hatriecache: priority visibility queue value is too large")
	}
	if len(destination) > priorityVisibilityQueueMaxSnapshotBytes-priorityVisibilityQueueChecksumSize-37-len(value) {
		return nil, errPriorityVisibilityQueueSnapshotTooBig
	}
	var fixed [37]byte
	binary.LittleEndian.PutUint64(fixed[0:8], id)
	binary.LittleEndian.PutUint64(fixed[8:16], uint64(priority))
	binary.LittleEndian.PutUint64(fixed[16:24], sequence)
	binary.LittleEndian.PutUint32(fixed[24:28], attempts)
	if timestamp.IsZero() {
		fixed[28] = 1
	} else {
		binary.LittleEndian.PutUint64(fixed[29:37], uint64(timestamp.UnixNano()))
	}
	destination = append(destination, fixed[:]...)
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], uint32(len(value)))
	destination = append(destination, length[:]...)
	destination = append(destination, value...)
	return destination, nil
}

// UnmarshalPriorityVisibilityQueue decodes a binary snapshot and preserves
// the epoch stored in that snapshot. Use LoadPriorityVisibilityQueue when a
// file restart should automatically fence old tokens.
func UnmarshalPriorityVisibilityQueue[T any](data []byte, codec PriorityVisibilityQueueCodec[T]) (*PriorityVisibilityQueue[T], error) {
	snapshot, err := unmarshalPriorityVisibilityQueueSnapshot(data, codec)
	if err != nil {
		return nil, err
	}
	return RestorePriorityVisibilityQueue(snapshot, snapshot.Epoch)
}

func unmarshalPriorityVisibilityQueueSnapshot[T any](data []byte, codec PriorityVisibilityQueueCodec[T]) (PriorityVisibilityQueueSnapshot[T], error) {
	if codec.Decode == nil {
		return PriorityVisibilityQueueSnapshot[T]{}, errPriorityVisibilityQueueNilCodec
	}
	if len(data) < priorityVisibilityQueueHeaderSize+priorityVisibilityQueueChecksumSize || len(data) > priorityVisibilityQueueMaxSnapshotBytes {
		return PriorityVisibilityQueueSnapshot[T]{}, errPriorityVisibilityQueueInvalidFormat
	}
	payload := data[:len(data)-priorityVisibilityQueueChecksumSize]
	wantChecksum := binary.LittleEndian.Uint32(data[len(data)-priorityVisibilityQueueChecksumSize:])
	if gotChecksum := crc32.Checksum(payload, crc32.IEEETable); gotChecksum != wantChecksum {
		return PriorityVisibilityQueueSnapshot[T]{}, errors.New("hatriecache: priority visibility queue checksum mismatch")
	}
	if string(payload[:4]) != string(priorityVisibilityQueueMagic[:]) || payload[4] != priorityVisibilityQueueFormatVersion || payload[5] != 0 || payload[6] != 0 || payload[7] != 0 {
		return PriorityVisibilityQueueSnapshot[T]{}, errPriorityVisibilityQueueInvalidFormat
	}
	capacity := binary.LittleEndian.Uint32(payload[8:12])
	if uint64(capacity) > uint64(int(^uint(0)>>1)) {
		return PriorityVisibilityQueueSnapshot[T]{}, errPriorityVisibilityQueueInvalidFormat
	}
	timeout := time.Duration(int64(binary.LittleEndian.Uint64(payload[12:20])))
	pendingCount := binary.LittleEndian.Uint32(payload[44:48])
	leaseCount := binary.LittleEndian.Uint32(payload[48:52])
	if pendingCount > priorityVisibilityQueueMaxEntries || leaseCount > priorityVisibilityQueueMaxEntries {
		return PriorityVisibilityQueueSnapshot[T]{}, errors.New("hatriecache: priority visibility queue has too many entries")
	}
	if capacity > 0 && uint64(pendingCount)+uint64(leaseCount) > uint64(capacity) {
		return PriorityVisibilityQueueSnapshot[T]{}, errors.New("hatriecache: priority visibility queue exceeds capacity")
	}
	snapshot := PriorityVisibilityQueueSnapshot[T]{
		Capacity:          int(capacity),
		VisibilityTimeout: timeout,
		Epoch:             binary.LittleEndian.Uint64(payload[20:28]),
		NextID:            binary.LittleEndian.Uint64(payload[28:36]),
		NextSequence:      binary.LittleEndian.Uint64(payload[36:44]),
		Pending:           make([]PriorityVisibilityQueueSnapshotItem[T], 0, pendingCount),
		Leases:            make([]PriorityVisibilityQueueLeaseSnapshot[T], 0, leaseCount),
	}
	if snapshot.Epoch == 0 || timeout <= 0 {
		return PriorityVisibilityQueueSnapshot[T]{}, errPriorityVisibilityQueueInvalidFormat
	}
	position := priorityVisibilityQueueHeaderSize
	for index := uint32(0); index < pendingCount; index++ {
		id, priority, sequence, attempts, timestamp, value, next, err := readPriorityVisibilityQueueRecord(payload, position, codec)
		if err != nil {
			return PriorityVisibilityQueueSnapshot[T]{}, fmt.Errorf("hatriecache: decode pending priority visibility queue record %d: %w", index, err)
		}
		snapshot.Pending = append(snapshot.Pending, PriorityVisibilityQueueSnapshotItem[T]{
			ID:       id,
			Priority: priority,
			ReadyAt:  timestamp,
			Sequence: sequence,
			Value:    value,
			Attempts: attempts,
		})
		position = next
	}
	for index := uint32(0); index < leaseCount; index++ {
		id, priority, sequence, attempts, timestamp, value, next, err := readPriorityVisibilityQueueRecord(payload, position, codec)
		if err != nil {
			return PriorityVisibilityQueueSnapshot[T]{}, fmt.Errorf("hatriecache: decode leased priority visibility queue record %d: %w", index, err)
		}
		snapshot.Leases = append(snapshot.Leases, PriorityVisibilityQueueLeaseSnapshot[T]{
			ID:         id,
			Priority:   priority,
			Sequence:   sequence,
			Value:      value,
			Attempts:   attempts,
			LeaseUntil: timestamp,
		})
		position = next
	}
	if position != len(payload) {
		return PriorityVisibilityQueueSnapshot[T]{}, errPriorityVisibilityQueueInvalidFormat
	}
	if err := ValidatePriorityVisibilityQueueSnapshot(snapshot); err != nil {
		return PriorityVisibilityQueueSnapshot[T]{}, err
	}
	return snapshot, nil
}

func readPriorityVisibilityQueueRecord[T any](payload []byte, position int, codec PriorityVisibilityQueueCodec[T]) (uint64, int64, uint64, uint32, time.Time, T, int, error) {
	var zero T
	if position < 0 || len(payload)-position < 41 {
		return 0, 0, 0, 0, time.Time{}, zero, 0, errPriorityVisibilityQueueInvalidFormat
	}
	id := binary.LittleEndian.Uint64(payload[position : position+8])
	priority := int64(binary.LittleEndian.Uint64(payload[position+8 : position+16]))
	sequence := binary.LittleEndian.Uint64(payload[position+16 : position+24])
	attempts := binary.LittleEndian.Uint32(payload[position+24 : position+28])
	timestampFlag := payload[position+28]
	if timestampFlag > 1 {
		return 0, 0, 0, 0, time.Time{}, zero, 0, errPriorityVisibilityQueueInvalidFormat
	}
	var timestamp time.Time
	if timestampFlag == 0 {
		nanos := int64(binary.LittleEndian.Uint64(payload[position+29 : position+37]))
		timestamp = time.Unix(0, nanos).UTC()
	}
	valueLength := binary.LittleEndian.Uint32(payload[position+37 : position+41])
	if valueLength > priorityVisibilityQueueMaxValueBytes || uint64(valueLength) > uint64(len(payload)-position-41) {
		return 0, 0, 0, 0, time.Time{}, zero, 0, errPriorityVisibilityQueueInvalidFormat
	}
	valueStart := position + 41
	valueEnd := valueStart + int(valueLength)
	value, err := codec.Decode(payload[valueStart:valueEnd])
	if err != nil {
		return 0, 0, 0, 0, time.Time{}, zero, 0, fmt.Errorf("decode value: %w", err)
	}
	return id, priority, sequence, attempts, timestamp, value, valueEnd, nil
}

// SavePriorityVisibilityQueue atomically writes a CRC-protected binary
// snapshot with mode 0600. It does not make individual mutations durable;
// callers choose when to checkpoint.
func SavePriorityVisibilityQueue[T any](path string, queue *PriorityVisibilityQueue[T], codec PriorityVisibilityQueueCodec[T]) error {
	if path == "" {
		return errors.New("hatriecache: priority visibility queue snapshot path is empty")
	}
	payload, err := queue.MarshalSnapshot(codec)
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	base := filepath.Base(path)
	temporary, err := os.CreateTemp(directory, "."+base+".tmp-*")
	if err != nil {
		return fmt.Errorf("hatriecache: create priority visibility queue snapshot temp file: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("hatriecache: chmod priority visibility queue snapshot: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		temporary.Close()
		return fmt.Errorf("hatriecache: write priority visibility queue snapshot: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("hatriecache: sync priority visibility queue snapshot: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("hatriecache: close priority visibility queue snapshot: %w", err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return fmt.Errorf("hatriecache: publish priority visibility queue snapshot: %w", err)
	}
	if err := syncPriorityVisibilityQueueDirectory(directory); err != nil {
		return fmt.Errorf("hatriecache: sync priority visibility queue snapshot directory: %w", err)
	}
	return nil
}

func syncPriorityVisibilityQueueDirectory(directory string) error {
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	if err := directoryFile.Sync(); err != nil {
		directoryFile.Close()
		return err
	}
	return directoryFile.Close()
}

// LoadPriorityVisibilityQueue loads a snapshot and advances its epoch so
// tokens from the previous process cannot acknowledge restored leases.
func LoadPriorityVisibilityQueue[T any](path string, codec PriorityVisibilityQueueCodec[T]) (*PriorityVisibilityQueue[T], error) {
	if path == "" {
		return nil, errors.New("hatriecache: priority visibility queue snapshot path is empty")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("hatriecache: open priority visibility queue snapshot: %w", err)
	}
	info, statErr := file.Stat()
	if statErr != nil {
		file.Close()
		return nil, fmt.Errorf("hatriecache: stat priority visibility queue snapshot: %w", statErr)
	}
	if info.Size() < 0 || info.Size() > priorityVisibilityQueueMaxSnapshotBytes {
		file.Close()
		return nil, errPriorityVisibilityQueueSnapshotTooBig
	}
	payload, readErr := io.ReadAll(io.LimitReader(file, priorityVisibilityQueueMaxSnapshotBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, fmt.Errorf("hatriecache: read priority visibility queue snapshot: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("hatriecache: close priority visibility queue snapshot: %w", closeErr)
	}
	if len(payload) > priorityVisibilityQueueMaxSnapshotBytes {
		return nil, errPriorityVisibilityQueueSnapshotTooBig
	}
	snapshot, err := unmarshalPriorityVisibilityQueueSnapshot(payload, codec)
	if err != nil {
		return nil, err
	}
	if snapshot.Epoch == ^uint64(0) {
		return nil, errors.New("hatriecache: priority visibility queue epoch exhausted")
	}
	return RestorePriorityVisibilityQueue(snapshot, snapshot.Epoch+1)
}
