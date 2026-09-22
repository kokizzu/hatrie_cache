package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const deduplicatingPriorityVisibilityQueueMaxKeyBytes = 4096

var (
	errDeduplicatingPriorityVisibilityQueueNilCodec   = errors.New("hatriecache: deduplicating priority visibility queue codec is incomplete")
	errDeduplicatingPriorityVisibilityQueueInvalidKey = errors.New("hatriecache: deduplicating priority visibility queue key is invalid")
)

// DeduplicatingPriorityVisibilityQueueCodec encodes values for binary queue
// snapshots. The client key is encoded by the queue and is not passed to the
// caller-owned value codec.
type DeduplicatingPriorityVisibilityQueueCodec[T any] struct {
	Encode func(T) ([]byte, error)
	Decode func([]byte) (T, error)
}

// DeduplicatingPriorityVisibilityQueueItem is a leased item and its client
// supplied identity. A key remains reserved until Ack succeeds.
type DeduplicatingPriorityVisibilityQueueItem[T any] struct {
	ID         uint64
	Key        string
	Priority   int64
	Value      T
	Attempts   uint32
	LeaseUntil time.Time
}

// DeduplicatingPriorityVisibilityQueueLease is the token-bearing form of a
// leased item for callers that persist or transfer acknowledgements.
type DeduplicatingPriorityVisibilityQueueLease[T any] struct {
	Token      PriorityVisibilityQueueLeaseToken
	Key        string
	Priority   int64
	Value      T
	Attempts   uint32
	LeaseUntil time.Time
}

// DeduplicatingPriorityVisibilityQueueSnapshotItem is one pending item in a
// deduplicating queue snapshot.
type DeduplicatingPriorityVisibilityQueueSnapshotItem[T any] struct {
	ID              uint64
	Key             string
	Priority        int64
	ReadyAt         time.Time
	Sequence        uint64
	Value           T
	Attempts        uint32
	StarvationSkips uint32
}

// DeduplicatingPriorityVisibilityQueueLeaseSnapshot is one active lease in a
// deduplicating queue snapshot.
type DeduplicatingPriorityVisibilityQueueLeaseSnapshot[T any] struct {
	ID              uint64
	Key             string
	Priority        int64
	Sequence        uint64
	Value           T
	Attempts        uint32
	LeaseUntil      time.Time
	StarvationSkips uint32
}

// DeduplicatingPriorityVisibilityQueueSnapshot is a checkpoint that preserves
// both queue state and client identities.
type DeduplicatingPriorityVisibilityQueueSnapshot[T any] struct {
	Capacity          int
	VisibilityTimeout time.Duration
	Epoch             uint64
	NextID            uint64
	NextSequence      uint64
	StarvationAfter   uint32
	EnableMetrics     bool
	Pending           []DeduplicatingPriorityVisibilityQueueSnapshotItem[T]
	Leases            []DeduplicatingPriorityVisibilityQueueLeaseSnapshot[T]
}

type deduplicatingPriorityVisibilityQueueValue[T any] struct {
	key   string
	value T
}

// DeduplicatingPriorityVisibilityQueue adds client-key uniqueness to a
// PriorityVisibilityQueue. It is non-thread-safe like the wrapped queue.
// Pending and leased items reserve their key until acknowledgement or Clear.
type DeduplicatingPriorityVisibilityQueue[T any] struct {
	queue *PriorityVisibilityQueue[deduplicatingPriorityVisibilityQueueValue[T]]
	keys  map[string]uint64
	ids   map[uint64]string
}

// NewDeduplicatingPriorityVisibilityQueue creates a deduplicating priority
// queue. A non-positive capacity is unbounded and a non-positive timeout uses
// the package default.
func NewDeduplicatingPriorityVisibilityQueue[T any](capacity int, visibilityTimeout time.Duration) *DeduplicatingPriorityVisibilityQueue[T] {
	return NewDeduplicatingPriorityVisibilityQueueWithOptions[T](PriorityVisibilityQueueOptions{
		Capacity:          capacity,
		VisibilityTimeout: visibilityTimeout,
		Epoch:             DefaultVisibilityQueueEpoch,
	})
}

// NewDeduplicatingPriorityVisibilityQueueWithOptions creates a deduplicating
// priority queue with the same scheduling options as PriorityVisibilityQueue.
func NewDeduplicatingPriorityVisibilityQueueWithOptions[T any](options PriorityVisibilityQueueOptions) *DeduplicatingPriorityVisibilityQueue[T] {
	return &DeduplicatingPriorityVisibilityQueue[T]{
		queue: NewPriorityVisibilityQueueWithOptions[deduplicatingPriorityVisibilityQueueValue[T]](options),
		keys:  make(map[string]uint64),
		ids:   make(map[uint64]string),
	}
}

// Len returns pending plus leased items.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) Len() int {
	if queue == nil {
		return 0
	}
	return queue.queue.Len()
}

// PendingLen returns pending items, including delayed items.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) PendingLen() int {
	if queue == nil {
		return 0
	}
	return queue.queue.PendingLen()
}

// LeaseLen returns active leases.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) LeaseLen() int {
	if queue == nil {
		return 0
	}
	return queue.queue.LeaseLen()
}

// Metrics returns the underlying priority queue metrics. Client-key
// deduplication does not change capacity, age, or consumer-lag semantics.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) Metrics(now time.Time) PriorityVisibilityQueueMetrics {
	if queue == nil {
		return PriorityVisibilityQueueMetrics{}
	}
	return queue.queue.Metrics(now)
}

// Enqueue adds a ready item. It returns false for an invalid or already
// reserved key, a full queue, or an exhausted queue identity counter.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) Enqueue(key string, priority int64, value T) bool {
	return queue.EnqueueAt(key, priority, time.Time{}, value)
}

// EnqueueAt adds an item that becomes ready at readyAt.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) EnqueueAt(key string, priority int64, readyAt time.Time, value T) bool {
	if queue == nil || !validDeduplicatingPriorityVisibilityQueueKey(key) {
		return false
	}
	if queue.keys == nil {
		queue.keys = make(map[string]uint64)
	}
	if _, exists := queue.keys[key]; exists {
		return false
	}
	if !queue.queue.EnqueueAt(priority, readyAt, deduplicatingPriorityVisibilityQueueValue[T]{key: key, value: value}) {
		return false
	}
	queue.keys[key] = 0
	return true
}

// EnqueueAfter adds an item after delay has elapsed from now.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) EnqueueAfter(key string, priority int64, now time.Time, delay time.Duration, value T) bool {
	return queue.EnqueueAt(key, priority, now.Add(delay), value)
}

// Lease returns the next ready item and reserves its key until Ack, Nack, or
// visibility expiry.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) Lease(now time.Time) (DeduplicatingPriorityVisibilityQueueItem[T], bool) {
	if queue == nil {
		return DeduplicatingPriorityVisibilityQueueItem[T]{}, false
	}
	item, ok := queue.queue.Lease(now)
	if !ok {
		return DeduplicatingPriorityVisibilityQueueItem[T]{}, false
	}
	queue.recordLease(item.ID, item.Value.key)
	return DeduplicatingPriorityVisibilityQueueItem[T]{
		ID:         item.ID,
		Key:        item.Value.key,
		Priority:   item.Priority,
		Value:      item.Value.value,
		Attempts:   item.Attempts,
		LeaseUntil: item.LeaseUntil,
	}, true
}

// LeaseWithToken returns the next ready item with an epoch-fenced token.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) LeaseWithToken(now time.Time) (DeduplicatingPriorityVisibilityQueueLease[T], bool) {
	if queue == nil {
		return DeduplicatingPriorityVisibilityQueueLease[T]{}, false
	}
	item, ok := queue.queue.LeaseWithToken(now)
	if !ok {
		return DeduplicatingPriorityVisibilityQueueLease[T]{}, false
	}
	queue.recordLease(item.Token.ID, item.Value.key)
	return DeduplicatingPriorityVisibilityQueueLease[T]{
		Token:      item.Token,
		Key:        item.Value.key,
		Priority:   item.Priority,
		Value:      item.Value.value,
		Attempts:   item.Attempts,
		LeaseUntil: item.LeaseUntil,
	}, true
}

// Ack permanently removes an active lease and releases its key.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) Ack(id uint64) bool {
	if queue == nil || !queue.queue.Ack(id) {
		return false
	}
	queue.releaseLease(id)
	return true
}

// AckToken acknowledges an active lease with an epoch-fenced token.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) AckToken(token PriorityVisibilityQueueLeaseToken) bool {
	if queue == nil || !queue.queue.AckToken(token) {
		return false
	}
	queue.releaseLease(token.ID)
	return true
}

// Nack makes an active lease available again at readyAt while retaining its
// key reservation.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) Nack(id uint64, readyAt time.Time) bool {
	if queue == nil || !queue.queue.Nack(id, readyAt) {
		return false
	}
	queue.requeueLease(id)
	return true
}

// NackToken makes an active lease available again when its epoch and ID match.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) NackToken(token PriorityVisibilityQueueLeaseToken, readyAt time.Time) bool {
	if queue == nil || !queue.queue.NackToken(token, readyAt) {
		return false
	}
	queue.requeueLease(token.ID)
	return true
}

// RequeueExpired makes expired leases ready and retains their key
// reservations.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) RequeueExpired(now time.Time) int {
	if queue == nil {
		return 0
	}
	recovered := queue.queue.RequeueExpired(now)
	if recovered > 0 {
		_ = queue.rebuildIndexes()
	}
	return recovered
}

// Clear removes all items and releases every client key.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) Clear() {
	if queue == nil {
		return
	}
	queue.queue.Clear()
	queue.keys = nil
	queue.ids = nil
}

// Snapshot returns a checkpoint that preserves client-key reservations.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) Snapshot() DeduplicatingPriorityVisibilityQueueSnapshot[T] {
	if queue == nil {
		return DeduplicatingPriorityVisibilityQueueSnapshot[T]{}
	}
	inner := queue.queue.Snapshot()
	snapshot := DeduplicatingPriorityVisibilityQueueSnapshot[T]{
		Capacity:          inner.Capacity,
		VisibilityTimeout: inner.VisibilityTimeout,
		Epoch:             inner.Epoch,
		NextID:            inner.NextID,
		NextSequence:      inner.NextSequence,
		StarvationAfter:   inner.StarvationAfter,
		EnableMetrics:     inner.EnableMetrics,
		Pending:           make([]DeduplicatingPriorityVisibilityQueueSnapshotItem[T], 0, len(inner.Pending)),
		Leases:            make([]DeduplicatingPriorityVisibilityQueueLeaseSnapshot[T], 0, len(inner.Leases)),
	}
	for _, item := range inner.Pending {
		snapshot.Pending = append(snapshot.Pending, DeduplicatingPriorityVisibilityQueueSnapshotItem[T]{
			ID:              item.ID,
			Key:             item.Value.key,
			Priority:        item.Priority,
			ReadyAt:         item.ReadyAt,
			Sequence:        item.Sequence,
			Value:           item.Value.value,
			Attempts:        item.Attempts,
			StarvationSkips: item.StarvationSkips,
		})
	}
	for _, item := range inner.Leases {
		snapshot.Leases = append(snapshot.Leases, DeduplicatingPriorityVisibilityQueueLeaseSnapshot[T]{
			ID:              item.ID,
			Key:             item.Value.key,
			Priority:        item.Priority,
			Sequence:        item.Sequence,
			Value:           item.Value.value,
			Attempts:        item.Attempts,
			LeaseUntil:      item.LeaseUntil,
			StarvationSkips: item.StarvationSkips,
		})
	}
	return snapshot
}

// RestoreDeduplicatingPriorityVisibilityQueue restores a snapshot and rejects
// duplicate or invalid client identities before making it visible.
func RestoreDeduplicatingPriorityVisibilityQueue[T any](snapshot DeduplicatingPriorityVisibilityQueueSnapshot[T], epoch uint64) (*DeduplicatingPriorityVisibilityQueue[T], error) {
	inner := PriorityVisibilityQueueSnapshot[deduplicatingPriorityVisibilityQueueValue[T]]{
		Capacity:          snapshot.Capacity,
		VisibilityTimeout: snapshot.VisibilityTimeout,
		Epoch:             snapshot.Epoch,
		NextID:            snapshot.NextID,
		NextSequence:      snapshot.NextSequence,
		StarvationAfter:   snapshot.StarvationAfter,
		EnableMetrics:     snapshot.EnableMetrics,
		Pending:           make([]PriorityVisibilityQueueSnapshotItem[deduplicatingPriorityVisibilityQueueValue[T]], 0, len(snapshot.Pending)),
		Leases:            make([]PriorityVisibilityQueueLeaseSnapshot[deduplicatingPriorityVisibilityQueueValue[T]], 0, len(snapshot.Leases)),
	}
	keys := make(map[string]struct{}, len(snapshot.Pending)+len(snapshot.Leases))
	for _, item := range snapshot.Pending {
		if !validDeduplicatingPriorityVisibilityQueueKey(item.Key) {
			return nil, errDeduplicatingPriorityVisibilityQueueInvalidKey
		}
		if _, exists := keys[item.Key]; exists {
			return nil, fmt.Errorf("hatriecache: duplicate deduplicating priority visibility queue key %q", item.Key)
		}
		keys[item.Key] = struct{}{}
		inner.Pending = append(inner.Pending, PriorityVisibilityQueueSnapshotItem[deduplicatingPriorityVisibilityQueueValue[T]]{
			ID:              item.ID,
			Priority:        item.Priority,
			ReadyAt:         item.ReadyAt,
			Sequence:        item.Sequence,
			Value:           deduplicatingPriorityVisibilityQueueValue[T]{key: item.Key, value: item.Value},
			Attempts:        item.Attempts,
			StarvationSkips: item.StarvationSkips,
		})
	}
	for _, item := range snapshot.Leases {
		if !validDeduplicatingPriorityVisibilityQueueKey(item.Key) {
			return nil, errDeduplicatingPriorityVisibilityQueueInvalidKey
		}
		if _, exists := keys[item.Key]; exists {
			return nil, fmt.Errorf("hatriecache: duplicate deduplicating priority visibility queue key %q", item.Key)
		}
		keys[item.Key] = struct{}{}
		inner.Leases = append(inner.Leases, PriorityVisibilityQueueLeaseSnapshot[deduplicatingPriorityVisibilityQueueValue[T]]{
			ID:              item.ID,
			Priority:        item.Priority,
			Sequence:        item.Sequence,
			Value:           deduplicatingPriorityVisibilityQueueValue[T]{key: item.Key, value: item.Value},
			Attempts:        item.Attempts,
			LeaseUntil:      item.LeaseUntil,
			StarvationSkips: item.StarvationSkips,
		})
	}
	underlying, err := RestorePriorityVisibilityQueue(inner, epoch)
	if err != nil {
		return nil, err
	}
	queue := &DeduplicatingPriorityVisibilityQueue[T]{queue: underlying}
	if err := queue.rebuildIndexes(); err != nil {
		return nil, err
	}
	return queue, nil
}

// MarshalSnapshot encodes the queue using the wrapped queue's CRC-protected
// binary format.
func (queue *DeduplicatingPriorityVisibilityQueue[T]) MarshalSnapshot(codec DeduplicatingPriorityVisibilityQueueCodec[T]) ([]byte, error) {
	if queue == nil || codec.Encode == nil {
		return nil, errDeduplicatingPriorityVisibilityQueueNilCodec
	}
	return queue.queue.MarshalSnapshot(queue.snapshotCodec(codec))
}

// UnmarshalDeduplicatingPriorityVisibilityQueue decodes a binary snapshot.
func UnmarshalDeduplicatingPriorityVisibilityQueue[T any](data []byte, codec DeduplicatingPriorityVisibilityQueueCodec[T]) (*DeduplicatingPriorityVisibilityQueue[T], error) {
	if codec.Decode == nil {
		return nil, errDeduplicatingPriorityVisibilityQueueNilCodec
	}
	underlying, err := UnmarshalPriorityVisibilityQueue(data, deduplicatingPriorityVisibilityQueueSnapshotCodec(codec))
	if err != nil {
		return nil, err
	}
	queue := &DeduplicatingPriorityVisibilityQueue[T]{queue: underlying}
	if err := queue.rebuildIndexes(); err != nil {
		return nil, err
	}
	return queue, nil
}

// SaveDeduplicatingPriorityVisibilityQueue atomically writes a queue snapshot.
func SaveDeduplicatingPriorityVisibilityQueue[T any](path string, queue *DeduplicatingPriorityVisibilityQueue[T], codec DeduplicatingPriorityVisibilityQueueCodec[T]) error {
	if queue == nil || codec.Encode == nil {
		return errDeduplicatingPriorityVisibilityQueueNilCodec
	}
	return SavePriorityVisibilityQueue(path, queue.queue, queue.snapshotCodec(codec))
}

// LoadDeduplicatingPriorityVisibilityQueue atomically loads a queue snapshot.
func LoadDeduplicatingPriorityVisibilityQueue[T any](path string, codec DeduplicatingPriorityVisibilityQueueCodec[T]) (*DeduplicatingPriorityVisibilityQueue[T], error) {
	if codec.Decode == nil {
		return nil, errDeduplicatingPriorityVisibilityQueueNilCodec
	}
	underlying, err := LoadPriorityVisibilityQueue(path, deduplicatingPriorityVisibilityQueueSnapshotCodec(codec))
	if err != nil {
		return nil, err
	}
	queue := &DeduplicatingPriorityVisibilityQueue[T]{queue: underlying}
	if err := queue.rebuildIndexes(); err != nil {
		return nil, err
	}
	return queue, nil
}

func (queue *DeduplicatingPriorityVisibilityQueue[T]) recordLease(id uint64, key string) {
	if queue.ids == nil {
		queue.ids = make(map[uint64]string)
	}
	if queue.keys == nil {
		queue.keys = make(map[string]uint64)
	}
	queue.ids[id] = key
	queue.keys[key] = id
}

func (queue *DeduplicatingPriorityVisibilityQueue[T]) releaseLease(id uint64) {
	key, ok := queue.ids[id]
	if !ok {
		return
	}
	delete(queue.ids, id)
	delete(queue.keys, key)
}

func (queue *DeduplicatingPriorityVisibilityQueue[T]) requeueLease(id uint64) {
	key, ok := queue.ids[id]
	if !ok {
		return
	}
	delete(queue.ids, id)
	queue.keys[key] = 0
}

func (queue *DeduplicatingPriorityVisibilityQueue[T]) rebuildIndexes() error {
	if queue == nil || queue.queue == nil {
		return errDeduplicatingPriorityVisibilityQueueInvalidKey
	}
	snapshot := queue.queue.Snapshot()
	keys := make(map[string]uint64, len(snapshot.Pending)+len(snapshot.Leases))
	ids := make(map[uint64]string, len(snapshot.Leases))
	for _, item := range snapshot.Pending {
		key := item.Value.key
		if !validDeduplicatingPriorityVisibilityQueueKey(key) {
			return errDeduplicatingPriorityVisibilityQueueInvalidKey
		}
		if _, exists := keys[key]; exists {
			return fmt.Errorf("hatriecache: duplicate deduplicating priority visibility queue key %q", key)
		}
		keys[key] = 0
	}
	for _, item := range snapshot.Leases {
		key := item.Value.key
		if !validDeduplicatingPriorityVisibilityQueueKey(key) {
			return errDeduplicatingPriorityVisibilityQueueInvalidKey
		}
		if _, exists := keys[key]; exists {
			return fmt.Errorf("hatriecache: duplicate deduplicating priority visibility queue key %q", key)
		}
		keys[key] = item.ID
		ids[item.ID] = key
	}
	queue.keys = keys
	queue.ids = ids
	return nil
}

func (queue *DeduplicatingPriorityVisibilityQueue[T]) snapshotCodec(codec DeduplicatingPriorityVisibilityQueueCodec[T]) PriorityVisibilityQueueCodec[deduplicatingPriorityVisibilityQueueValue[T]] {
	return deduplicatingPriorityVisibilityQueueSnapshotCodec(codec)
}

func deduplicatingPriorityVisibilityQueueSnapshotCodec[T any](codec DeduplicatingPriorityVisibilityQueueCodec[T]) PriorityVisibilityQueueCodec[deduplicatingPriorityVisibilityQueueValue[T]] {
	return PriorityVisibilityQueueCodec[deduplicatingPriorityVisibilityQueueValue[T]]{
		Encode: func(value deduplicatingPriorityVisibilityQueueValue[T]) ([]byte, error) {
			if !validDeduplicatingPriorityVisibilityQueueKey(value.key) || codec.Encode == nil {
				return nil, errDeduplicatingPriorityVisibilityQueueInvalidKey
			}
			encodedValue, err := codec.Encode(value.value)
			if err != nil {
				return nil, err
			}
			if len(value.key) > int(^uint32(0)) || len(encodedValue) > priorityVisibilityQueueMaxValueBytes {
				return nil, errPriorityVisibilityQueueSnapshotTooBig
			}
			encoded := make([]byte, 4+len(value.key)+len(encodedValue))
			binary.LittleEndian.PutUint32(encoded[:4], uint32(len(value.key)))
			copy(encoded[4:], value.key)
			copy(encoded[4+len(value.key):], encodedValue)
			return encoded, nil
		},
		Decode: func(encoded []byte) (deduplicatingPriorityVisibilityQueueValue[T], error) {
			if len(encoded) < 4 {
				return deduplicatingPriorityVisibilityQueueValue[T]{}, errDeduplicatingPriorityVisibilityQueueInvalidKey
			}
			keyLength := binary.LittleEndian.Uint32(encoded[:4])
			if keyLength == 0 || keyLength > deduplicatingPriorityVisibilityQueueMaxKeyBytes || uint64(keyLength) > uint64(len(encoded)-4) || codec.Decode == nil {
				return deduplicatingPriorityVisibilityQueueValue[T]{}, errDeduplicatingPriorityVisibilityQueueInvalidKey
			}
			keyEnd := 4 + int(keyLength)
			value, err := codec.Decode(encoded[keyEnd:])
			if err != nil {
				return deduplicatingPriorityVisibilityQueueValue[T]{}, err
			}
			return deduplicatingPriorityVisibilityQueueValue[T]{key: string(encoded[4:keyEnd]), value: value}, nil
		},
	}
}

func validDeduplicatingPriorityVisibilityQueueKey(key string) bool {
	return len(key) > 0 && len(key) <= deduplicatingPriorityVisibilityQueueMaxKeyBytes
}
