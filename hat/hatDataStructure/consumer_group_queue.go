package hatDataStructure

import "time"

// DefaultConsumerGroupQueueEpoch is used by the zero-value queue and by
// constructors that receive a zero epoch.
const DefaultConsumerGroupQueueEpoch uint64 = DefaultVisibilityQueueEpoch

// ConsumerGroupLeaseToken identifies one delivery attempt and its owner.
// Attempts changes when a lease expires or is negatively acknowledged, so an
// old worker cannot acknowledge a later retry of the same item.
type ConsumerGroupLeaseToken struct {
	Epoch    uint64 `json:"epoch"`
	Group    string `json:"group"`
	Consumer string `json:"consumer"`
	ID       uint64 `json:"id"`
	Attempts uint32 `json:"attempts"`
}

// ConsumerGroupLease is one owned delivery from a ConsumerGroupQueue.
type ConsumerGroupLease[T any] struct {
	Token      ConsumerGroupLeaseToken
	Value      T
	Attempts   uint32
	LeaseUntil time.Time
}

type consumerGroupQueueGroup[T any] struct {
	queue     *VisibilityQueue[T]
	consumers map[string]struct{}
	owners    map[uint64]string
}

// ConsumerGroupQueue is a non-thread-safe collection of named work streams.
// Each group has its own pending queue; consumers registered in the same
// group compete for each item, while items in one group are invisible to
// consumers in another group. Capacity applies independently to each group.
// The zero value is ready for use with unbounded capacity and the default
// visibility timeout.
//
// Only active leases retain owner metadata. Pending values reuse the existing
// VisibilityQueue storage and do not carry a group or consumer string per
// item.
type ConsumerGroupQueue[T any] struct {
	groups            map[string]*consumerGroupQueueGroup[T]
	capacity          int
	visibilityTimeout time.Duration
	epoch             uint64
}

// NewConsumerGroupQueue creates a consumer-group queue. A non-positive
// capacity means unbounded and a non-positive timeout selects the default.
func NewConsumerGroupQueue[T any](capacity int, visibilityTimeout time.Duration) *ConsumerGroupQueue[T] {
	return NewConsumerGroupQueueWithEpoch[T](capacity, visibilityTimeout, DefaultConsumerGroupQueueEpoch)
}

// NewConsumerGroupQueueWithEpoch creates a queue with an explicit restart
// epoch. Persist the epoch with the queue owner and advance it before
// restoring a queue in a new process so old lease tokens cannot acknowledge
// restored work.
func NewConsumerGroupQueueWithEpoch[T any](capacity int, visibilityTimeout time.Duration, epoch uint64) *ConsumerGroupQueue[T] {
	if capacity < 0 {
		capacity = 0
	}
	if visibilityTimeout <= 0 {
		visibilityTimeout = DefaultVisibilityQueueTimeout
	}
	if epoch == 0 {
		epoch = DefaultConsumerGroupQueueEpoch
	}
	return &ConsumerGroupQueue[T]{
		groups:            make(map[string]*consumerGroupQueueGroup[T]),
		capacity:          capacity,
		visibilityTimeout: visibilityTimeout,
		epoch:             epoch,
	}
}

// Epoch returns the queue's lease epoch.
func (queue *ConsumerGroupQueue[T]) Epoch() uint64 {
	if queue == nil {
		return 0
	}
	if queue.epoch == 0 {
		queue.epoch = DefaultConsumerGroupQueueEpoch
	}
	return queue.epoch
}

func (queue *ConsumerGroupQueue[T]) group(name string, create bool) *consumerGroupQueueGroup[T] {
	if queue == nil || name == "" {
		return nil
	}
	if queue.groups == nil {
		if !create {
			return nil
		}
		queue.groups = make(map[string]*consumerGroupQueueGroup[T])
	}
	if group := queue.groups[name]; group != nil || !create {
		return group
	}
	group := &consumerGroupQueueGroup[T]{
		queue:     NewVisibilityQueueWithEpoch[T](queue.capacity, queue.visibilityTimeout, queue.Epoch()),
		consumers: make(map[string]struct{}),
		owners:    make(map[uint64]string),
	}
	queue.groups[name] = group
	return group
}

// Register adds a consumer to a group. Re-registering the same consumer is
// idempotent.
func (queue *ConsumerGroupQueue[T]) Register(groupName, consumer string) bool {
	if consumer == "" {
		return false
	}
	group := queue.group(groupName, true)
	if group == nil {
		return false
	}
	group.consumers[consumer] = struct{}{}
	return true
}

// Unregister removes a consumer and immediately returns its active leases to
// the group's pending queue at readyAt. Pending work remains available to the
// other consumers in the group.
func (queue *ConsumerGroupQueue[T]) Unregister(groupName, consumer string, readyAt time.Time) bool {
	group := queue.group(groupName, false)
	if group == nil {
		return false
	}
	if _, ok := group.consumers[consumer]; !ok {
		return false
	}
	queue.release(group, consumer, readyAt)
	delete(group.consumers, consumer)
	return true
}

// Enqueue adds value to one named group's work stream.
func (queue *ConsumerGroupQueue[T]) Enqueue(groupName string, value T) bool {
	return queue.EnqueueAt(groupName, time.Time{}, value)
}

// EnqueueAt adds value to one group and makes it available at readyAt.
func (queue *ConsumerGroupQueue[T]) EnqueueAt(groupName string, readyAt time.Time, value T) bool {
	group := queue.group(groupName, true)
	return group != nil && group.queue.EnqueueAt(readyAt, value)
}

// Lease claims the next ready item for a registered consumer.
func (queue *ConsumerGroupQueue[T]) Lease(groupName, consumer string, now time.Time) (ConsumerGroupLease[T], bool) {
	var result ConsumerGroupLease[T]
	group := queue.group(groupName, false)
	if group == nil {
		return result, false
	}
	if _, ok := group.consumers[consumer]; !ok {
		return result, false
	}
	lease, ok := group.queue.LeaseWithToken(now)
	if !ok {
		return result, false
	}
	group.owners[lease.Token.ID] = consumer
	group.maybeSweepOwners()
	result = ConsumerGroupLease[T]{
		Token: ConsumerGroupLeaseToken{
			Epoch:    lease.Token.Epoch,
			Group:    groupName,
			Consumer: consumer,
			ID:       lease.Token.ID,
			Attempts: lease.Attempts,
		},
		Value:      lease.Value,
		Attempts:   lease.Attempts,
		LeaseUntil: lease.LeaseUntil,
	}
	return result, true
}

// Ack permanently completes an owned delivery. It rejects stale retry tokens
// and tokens presented by a different consumer.
func (queue *ConsumerGroupQueue[T]) Ack(token ConsumerGroupLeaseToken) bool {
	group := queue.validLeaseGroup(token)
	if group == nil || group.owners[token.ID] != token.Consumer {
		return false
	}
	lease, ok := group.queue.leases[token.ID]
	if !ok || lease.entry.attempts != token.Attempts {
		return false
	}
	if !group.queue.AckToken(VisibilityQueueLeaseToken{Epoch: token.Epoch, ID: token.ID}) {
		return false
	}
	delete(group.owners, token.ID)
	return true
}

// Nack returns an owned delivery to its group at readyAt without losing its
// stable item ID or retry attempt count.
func (queue *ConsumerGroupQueue[T]) Nack(token ConsumerGroupLeaseToken, readyAt time.Time) bool {
	group := queue.validLeaseGroup(token)
	if group == nil || group.owners[token.ID] != token.Consumer {
		return false
	}
	lease, ok := group.queue.leases[token.ID]
	if !ok || lease.entry.attempts != token.Attempts {
		return false
	}
	if !group.queue.NackToken(VisibilityQueueLeaseToken{Epoch: token.Epoch, ID: token.ID}, readyAt) {
		return false
	}
	delete(group.owners, token.ID)
	return true
}

// Release returns all active leases owned by one consumer to its group.
// It is useful when a worker is drained or declared unavailable.
func (queue *ConsumerGroupQueue[T]) Release(groupName, consumer string, readyAt time.Time) int {
	group := queue.group(groupName, false)
	if group == nil {
		return 0
	}
	return queue.release(group, consumer, readyAt)
}

func (queue *ConsumerGroupQueue[T]) release(group *consumerGroupQueueGroup[T], consumer string, readyAt time.Time) int {
	group.sweepExpired(readyAt)
	released := 0
	for id, owner := range group.owners {
		if owner != consumer {
			continue
		}
		_, ok := group.queue.leases[id]
		if !ok {
			delete(group.owners, id)
			continue
		}
		if group.queue.NackToken(VisibilityQueueLeaseToken{Epoch: group.queue.Epoch(), ID: id}, readyAt) {
			delete(group.owners, id)
			released++
		}
	}
	return released
}

// RequeueExpired makes every expired delivery available again and returns the
// number of deliveries requeued.
func (queue *ConsumerGroupQueue[T]) RequeueExpired(now time.Time) int {
	if queue == nil {
		return 0
	}
	requeued := 0
	for _, group := range queue.groups {
		requeued += group.sweepExpired(now)
	}
	return requeued
}

// Len returns pending plus leased items across all groups.
func (queue *ConsumerGroupQueue[T]) Len() int {
	if queue == nil {
		return 0
	}
	total := 0
	for _, group := range queue.groups {
		total += group.queue.Len()
	}
	return total
}

// GroupLen returns pending plus leased items in one group.
func (queue *ConsumerGroupQueue[T]) GroupLen(groupName string) int {
	group := queue.group(groupName, false)
	if group == nil {
		return 0
	}
	return group.queue.Len()
}

func (queue *ConsumerGroupQueue[T]) validLeaseGroup(token ConsumerGroupLeaseToken) *consumerGroupQueueGroup[T] {
	if queue == nil || token.ID == 0 || token.Attempts == 0 || token.Group == "" || token.Consumer == "" || token.Epoch != queue.Epoch() {
		return nil
	}
	group := queue.group(token.Group, false)
	if group == nil {
		return nil
	}
	return group
}

func (group *consumerGroupQueueGroup[T]) sweepExpired(now time.Time) int {
	requeued := group.queue.RequeueExpired(now)
	group.sweepOwners()
	return requeued
}

func (group *consumerGroupQueueGroup[T]) sweepOwners() {
	for id := range group.owners {
		if _, ok := group.queue.leases[id]; !ok {
			delete(group.owners, id)
		}
	}
}

func (group *consumerGroupQueueGroup[T]) maybeSweepOwners() {
	// Expiry already uses the underlying heap. Avoid an O(active leases) scan
	// on every delivery, but bound stale owner metadata after mass expiry.
	if len(group.owners) > group.queue.LeaseLen()*2+64 {
		group.sweepOwners()
	}
}
