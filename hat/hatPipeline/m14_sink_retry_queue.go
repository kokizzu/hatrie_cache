package hatPipeline

import (
	"errors"
	"strings"
	"sync"
	"time"

	"hatrie_cache/hat/hatDataStructure"
)

const (
	// DefaultSinkRetryCapacity bounds pending and leased deliveries for each
	// sink when no explicit capacity is supplied.
	DefaultSinkRetryCapacity = 1024
	// DefaultSinkRetryVisibilityTimeout is the lease duration used by a zero
	// visibility-timeout option.
	DefaultSinkRetryVisibilityTimeout = time.Minute
	// DefaultSinkRetryMaxAttempts limits delivery attempts before a poison
	// message is moved to the dead-letter list.
	DefaultSinkRetryMaxAttempts uint32 = 8
	// DefaultSinkRetryDeadLetterLimit bounds retained poison messages.
	DefaultSinkRetryDeadLetterLimit = 256
)

var (
	// ErrSinkRetryQueueInvalidCapacity reports a negative per-sink capacity.
	ErrSinkRetryQueueInvalidCapacity = errors.New("hatPipeline: sink retry queue capacity is invalid")
	// ErrSinkRetryQueueInvalidVisibilityTimeout reports a negative lease
	// duration.
	ErrSinkRetryQueueInvalidVisibilityTimeout = errors.New("hatPipeline: sink retry queue visibility timeout is invalid")
	// ErrSinkRetryQueueInvalidDeadLetterLimit reports a negative retention
	// limit.
	ErrSinkRetryQueueInvalidDeadLetterLimit = errors.New("hatPipeline: sink retry queue dead-letter limit is invalid")
)

// SinkRetryQueueOptions controls one SinkRetryQueue. Capacity and the
// dead-letter limit are per queue state as documented below; capacity is per
// sink, while dead letters are retained in one bounded list shared by sinks.
type SinkRetryQueueOptions struct {
	// Capacity is the maximum number of pending and leased deliveries per
	// sink. Zero uses DefaultSinkRetryCapacity.
	Capacity int
	// VisibilityTimeout is the default lease duration. Zero uses
	// DefaultSinkRetryVisibilityTimeout.
	VisibilityTimeout time.Duration
	// MaxAttempts is the number of actual delivery leases allowed. Zero uses
	// DefaultSinkRetryMaxAttempts.
	MaxAttempts uint32
	// DeadLetterLimit is the maximum number of retained poison deliveries.
	// Zero uses DefaultSinkRetryDeadLetterLimit.
	DeadLetterLimit int
	// Epoch fences leases across restarts. Zero uses the visibility queue's
	// default epoch; callers restoring ownership should advance it.
	Epoch uint64
}

// SinkRetryLeaseToken fences acknowledgement and retry operations to one
// sink queue, restart epoch, and lease attempt. Treat it as opaque.
type SinkRetryLeaseToken struct {
	Sink     string
	QueueID  uint64
	Epoch    uint64
	ID       uint64
	Attempts uint32
}

// SinkRetryLease is a leased delivery. Pass the complete value to Ack or
// Retry; a lease is hidden from other consumers until it is acknowledged,
// retried, or its visibility timeout expires.
type SinkRetryLease[T any] struct {
	Sink       string
	Token      SinkRetryLeaseToken
	Value      T
	Attempts   uint32
	LeaseUntil time.Time
}

// SinkRetryDeadLetter is a poison delivery retained for inspection or
// operator replay.
type SinkRetryDeadLetter[T any] struct {
	ID       uint64
	Sink     string
	Value    T
	FailedAt time.Time
	Attempts uint32
	Reason   string
}

// SinkRetryQueueOutcome describes what Retry did with a valid lease.
type SinkRetryQueueOutcome uint8

const (
	SinkRetryOutcomeInvalid SinkRetryQueueOutcome = iota
	SinkRetryOutcomeRequeued
	SinkRetryOutcomeDeadLettered
)

// SinkRetryQueueStats is a point-in-time queue summary.
type SinkRetryQueueStats struct {
	SinkCount   int
	Pending     int
	Leased      int
	DeadLetters int
}

type sinkRetryState[T any] struct {
	queue          *hatDataStructure.VisibilityQueue[T]
	queueID        uint64
	activeAttempts map[uint64]uint32
	activeValues   map[uint64]T
}

// SinkRetryQueue provides a synchronized, bounded retry queue partitioned by
// sink name. Each sink has independent capacity and lease ordering. Delivery
// workers normally call Lease and then Ack on success or Retry on failure;
// the queue does not perform network or storage side effects itself.
//
// The queue is in-memory. Persist sink progress or the delivery payloads with
// the existing checkpoint/journal APIs when recovery across process loss is
// required. Advance Options.Epoch before restoring ownership so old leases
// cannot acknowledge new work.
type SinkRetryQueue[T any] struct {
	mu sync.Mutex

	capacity          int
	visibilityTimeout time.Duration
	maxAttempts       uint32
	deadLetterLimit   int
	epoch             uint64

	nextQueueID      uint64
	sinks            map[string]*sinkRetryState[T]
	deadLetters      []SinkRetryDeadLetter[T]
	nextDeadLetterID uint64
}

// NewSinkRetryQueue creates a bounded per-sink retry queue.
func NewSinkRetryQueue[T any](options SinkRetryQueueOptions) (*SinkRetryQueue[T], error) {
	if options.Capacity < 0 {
		return nil, ErrSinkRetryQueueInvalidCapacity
	}
	if options.VisibilityTimeout < 0 {
		return nil, ErrSinkRetryQueueInvalidVisibilityTimeout
	}
	if options.DeadLetterLimit < 0 {
		return nil, ErrSinkRetryQueueInvalidDeadLetterLimit
	}
	if options.Capacity == 0 {
		options.Capacity = DefaultSinkRetryCapacity
	}
	if options.VisibilityTimeout == 0 {
		options.VisibilityTimeout = DefaultSinkRetryVisibilityTimeout
	}
	if options.MaxAttempts == 0 {
		options.MaxAttempts = DefaultSinkRetryMaxAttempts
	}
	if options.DeadLetterLimit == 0 {
		options.DeadLetterLimit = DefaultSinkRetryDeadLetterLimit
	}
	if options.Epoch == 0 {
		options.Epoch = hatDataStructure.DefaultVisibilityQueueEpoch
	}
	return &SinkRetryQueue[T]{
		capacity:          options.Capacity,
		visibilityTimeout: options.VisibilityTimeout,
		maxAttempts:       options.MaxAttempts,
		deadLetterLimit:   options.DeadLetterLimit,
		epoch:             options.Epoch,
		sinks:             make(map[string]*sinkRetryState[T]),
	}, nil
}

// Enqueue adds an immediately ready delivery for sink. It returns false for
// an invalid sink or when that sink has reached its capacity.
func (queue *SinkRetryQueue[T]) Enqueue(sink string, value T) bool {
	return queue.EnqueueAt(sink, time.Time{}, value)
}

// EnqueueAt adds a delivery that becomes ready at readyAt.
func (queue *SinkRetryQueue[T]) EnqueueAt(sink string, readyAt time.Time, value T) bool {
	if queue == nil || !validSinkRetryName(sink) {
		return false
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	state := queue.sinkStateLocked(sink)
	return state.queue.EnqueueAt(readyAt, value)
}

// Lease returns the next ready delivery for sink. If an expired poison item
// crosses MaxAttempts, it is retained as a dead letter and the next item is
// considered in the same call.
func (queue *SinkRetryQueue[T]) Lease(sink string, now time.Time) (SinkRetryLease[T], bool) {
	var zero SinkRetryLease[T]
	if queue == nil || !validSinkRetryName(sink) {
		return zero, false
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	state, ok := queue.sinks[sink]
	if !ok {
		return zero, false
	}
	for {
		item, ok := state.queue.LeaseWithToken(now)
		if !ok {
			return zero, false
		}
		state.activeAttempts[item.Token.ID] = item.Attempts
		state.activeValues[item.Token.ID] = item.Value
		if item.Attempts <= queue.maxAttempts {
			return SinkRetryLease[T]{
				Sink:  sink,
				Token: SinkRetryLeaseToken{Sink: sink, QueueID: state.queueID, Epoch: item.Token.Epoch, ID: item.Token.ID, Attempts: item.Attempts},
				Value: item.Value, Attempts: item.Attempts, LeaseUntil: item.LeaseUntil,
			}, true
		}
		if !state.queue.AckToken(item.Token) {
			delete(state.activeAttempts, item.Token.ID)
			delete(state.activeValues, item.Token.ID)
			continue
		}
		delete(state.activeAttempts, item.Token.ID)
		delete(state.activeValues, item.Token.ID)
		queue.recordDeadLetterLocked(sink, item.Value, item.Attempts, now, "maximum delivery attempts exceeded")
	}
}

// Ack permanently removes a valid active lease.
func (queue *SinkRetryQueue[T]) Ack(lease SinkRetryLease[T]) bool {
	if queue == nil {
		return false
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	state, token, ok := queue.activeLeaseLocked(lease)
	if !ok || !state.queue.AckToken(token) {
		return false
	}
	delete(state.activeAttempts, token.ID)
	delete(state.activeValues, token.ID)
	return true
}

// Retry makes a valid lease available at readyAt, or moves it to the bounded
// dead-letter list when its attempt budget is exhausted. The returned bool is
// false for a stale, cross-sink, or already completed lease.
func (queue *SinkRetryQueue[T]) Retry(lease SinkRetryLease[T], readyAt time.Time, reason string) (SinkRetryQueueOutcome, uint64, bool) {
	if queue == nil {
		return SinkRetryOutcomeInvalid, 0, false
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	state, token, ok := queue.activeLeaseLocked(lease)
	if !ok {
		return SinkRetryOutcomeInvalid, 0, false
	}
	attempts := state.activeAttempts[token.ID]
	value := state.activeValues[token.ID]
	if attempts < queue.maxAttempts {
		if !state.queue.NackToken(token, readyAt) {
			return SinkRetryOutcomeInvalid, 0, false
		}
		delete(state.activeAttempts, token.ID)
		delete(state.activeValues, token.ID)
		return SinkRetryOutcomeRequeued, 0, true
	}
	if !state.queue.AckToken(token) {
		return SinkRetryOutcomeInvalid, 0, false
	}
	delete(state.activeAttempts, token.ID)
	delete(state.activeValues, token.ID)
	deadLetterID := queue.recordDeadLetterLocked(lease.Sink, value, attempts, time.Now().UTC(), reason)
	return SinkRetryOutcomeDeadLettered, deadLetterID, true
}

// RequeueExpired makes expired leases ready again and returns the number
// recovered. Attempt limits are enforced when those items are leased next.
func (queue *SinkRetryQueue[T]) RequeueExpired(now time.Time) int {
	if queue == nil {
		return 0
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	recovered := 0
	for _, state := range queue.sinks {
		recovered += state.queue.RequeueExpired(now)
	}
	return recovered
}

// DeadLetter returns a retained poison delivery by ID.
func (queue *SinkRetryQueue[T]) DeadLetter(id uint64) (SinkRetryDeadLetter[T], bool) {
	if queue == nil || id == 0 {
		return SinkRetryDeadLetter[T]{}, false
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for _, item := range queue.deadLetters {
		if item.ID == id {
			return item, true
		}
	}
	return SinkRetryDeadLetter[T]{}, false
}

// DeadLetters returns an independent snapshot in failure order.
func (queue *SinkRetryQueue[T]) DeadLetters() []SinkRetryDeadLetter[T] {
	if queue == nil {
		return nil
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.deadLetters) == 0 {
		return nil
	}
	deadLetters := make([]SinkRetryDeadLetter[T], len(queue.deadLetters))
	copy(deadLetters, queue.deadLetters)
	return deadLetters
}

// ReplayDeadLetter removes a retained poison delivery and resets its attempt
// budget by enqueuing it as new work. It leaves the failure retained when the
// sink capacity is full.
func (queue *SinkRetryQueue[T]) ReplayDeadLetter(id uint64, readyAt time.Time) bool {
	if queue == nil || id == 0 {
		return false
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for index, item := range queue.deadLetters {
		if item.ID != id {
			continue
		}
		state := queue.sinkStateLocked(item.Sink)
		if !state.queue.EnqueueAt(readyAt, item.Value) {
			return false
		}
		queue.removeDeadLetterLocked(index)
		return true
	}
	return false
}

// DiscardDeadLetter permanently removes a retained poison delivery.
func (queue *SinkRetryQueue[T]) DiscardDeadLetter(id uint64) bool {
	if queue == nil || id == 0 {
		return false
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for index, item := range queue.deadLetters {
		if item.ID == id {
			queue.removeDeadLetterLocked(index)
			return true
		}
	}
	return false
}

// Stats returns a point-in-time summary. SinkCount includes every sink that
// has received work so lease IDs remain unique for the life of the queue.
func (queue *SinkRetryQueue[T]) Stats() SinkRetryQueueStats {
	if queue == nil {
		return SinkRetryQueueStats{}
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	stats := SinkRetryQueueStats{SinkCount: len(queue.sinks), DeadLetters: len(queue.deadLetters)}
	for _, state := range queue.sinks {
		stats.Pending += state.queue.PendingLen()
		stats.Leased += state.queue.LeaseLen()
	}
	return stats
}

// Clear removes pending, leased, and retained deliveries while preserving
// queue and lease epochs so old tokens cannot become valid again.
func (queue *SinkRetryQueue[T]) Clear() {
	if queue == nil {
		return
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for _, state := range queue.sinks {
		state.queue.Clear()
		clear(state.activeAttempts)
		clear(state.activeValues)
	}
	for index := range queue.deadLetters {
		var zero T
		queue.deadLetters[index].Value = zero
	}
	queue.deadLetters = queue.deadLetters[:0]
}

func validSinkRetryName(sink string) bool {
	return strings.TrimSpace(sink) != ""
}

func (queue *SinkRetryQueue[T]) sinkStateLocked(sink string) *sinkRetryState[T] {
	if state, ok := queue.sinks[sink]; ok {
		return state
	}
	queue.nextQueueID++
	if queue.nextQueueID == 0 {
		queue.nextQueueID++
	}
	state := &sinkRetryState[T]{
		queue:          hatDataStructure.NewVisibilityQueueWithEpoch[T](queue.capacity, queue.visibilityTimeout, queue.epoch),
		queueID:        queue.nextQueueID,
		activeAttempts: make(map[uint64]uint32),
		activeValues:   make(map[uint64]T),
	}
	queue.sinks[sink] = state
	return state
}

func (queue *SinkRetryQueue[T]) activeLeaseLocked(lease SinkRetryLease[T]) (*sinkRetryState[T], hatDataStructure.VisibilityQueueLeaseToken, bool) {
	token := lease.Token
	if !validSinkRetryName(lease.Sink) || lease.Sink != token.Sink || token.QueueID == 0 || token.Epoch == 0 || token.ID == 0 || token.Attempts == 0 {
		return nil, hatDataStructure.VisibilityQueueLeaseToken{}, false
	}
	state, ok := queue.sinks[token.Sink]
	if !ok || state.queueID != token.QueueID || state.activeAttempts[token.ID] != token.Attempts {
		return nil, hatDataStructure.VisibilityQueueLeaseToken{}, false
	}
	return state, hatDataStructure.VisibilityQueueLeaseToken{Epoch: token.Epoch, ID: token.ID}, true
}

func (queue *SinkRetryQueue[T]) recordDeadLetterLocked(sink string, value T, attempts uint32, failedAt time.Time, reason string) uint64 {
	if queue.deadLetterLimit <= 0 {
		return 0
	}
	queue.nextDeadLetterID++
	if queue.nextDeadLetterID == 0 {
		queue.nextDeadLetterID++
	}
	id := queue.nextDeadLetterID
	queue.deadLetters = append(queue.deadLetters, SinkRetryDeadLetter[T]{ID: id, Sink: sink, Value: value, FailedAt: failedAt, Attempts: attempts, Reason: reason})
	if len(queue.deadLetters) > queue.deadLetterLimit {
		drop := len(queue.deadLetters) - queue.deadLetterLimit
		copy(queue.deadLetters, queue.deadLetters[drop:])
		for index := queue.deadLetterLimit; index < len(queue.deadLetters); index++ {
			var zero T
			queue.deadLetters[index].Value = zero
		}
		queue.deadLetters = queue.deadLetters[:queue.deadLetterLimit]
	}
	return id
}

func (queue *SinkRetryQueue[T]) removeDeadLetterLocked(index int) {
	last := len(queue.deadLetters) - 1
	var zero T
	queue.deadLetters[index].Value = zero
	copy(queue.deadLetters[index:], queue.deadLetters[index+1:])
	queue.deadLetters[last] = SinkRetryDeadLetter[T]{}
	queue.deadLetters = queue.deadLetters[:last]
}
