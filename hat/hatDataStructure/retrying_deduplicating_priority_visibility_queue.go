package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	retryingDeduplicatingPriorityVisibilityQueueFormatVersion    byte = 1
	retryingDeduplicatingPriorityVisibilityQueueHeaderSize            = 24
	retryingDeduplicatingPriorityVisibilityQueueRecordHeaderSize      = 32
)

var retryingDeduplicatingPriorityVisibilityQueueMagic = [4]byte{'H', 'R', 'D', 'Q'}

var (
	errRetryingDeduplicatingPriorityVisibilityQueueNilCodec  = errors.New("hatriecache: retrying deduplicating priority visibility queue codec is incomplete")
	errRetryingDeduplicatingPriorityVisibilityQueueInvalid   = errors.New("hatriecache: retrying deduplicating priority visibility queue snapshot is invalid")
	errRetryingDeduplicatingPriorityVisibilityQueueTooBig    = errors.New("hatriecache: retrying deduplicating priority visibility queue snapshot is too large")
	errRetryingDeduplicatingPriorityVisibilityQueueEpoch     = errors.New("hatriecache: retrying deduplicating priority visibility queue epoch exhausted")
	errRetryingDeduplicatingPriorityVisibilityQueueEmptyPath = errors.New("hatriecache: retrying deduplicating priority visibility queue snapshot path is empty")
)

// RetryDeadLetterReason identifies why a task was routed to the dead-letter
// collection.
type RetryDeadLetterReason uint8

const (
	RetryDeadLetterMaxAttempts       RetryDeadLetterReason = 1
	RetryDeadLetterVisibilityExpired RetryDeadLetterReason = 2
)

func (reason RetryDeadLetterReason) String() string {
	switch reason {
	case RetryDeadLetterMaxAttempts:
		return "max-attempts"
	case RetryDeadLetterVisibilityExpired:
		return "visibility-expired"
	default:
		return "unknown"
	}
}

// RetryingDeduplicatingPriorityVisibilityQueueOptions configures the opt-in
// retry/dead-letter wrapper. MaxAttempts==0 means unlimited retries.
type RetryingDeduplicatingPriorityVisibilityQueueOptions struct {
	PriorityVisibilityQueueOptions
	MaxAttempts uint32
}

// RetryingDeduplicatingPriorityVisibilityQueueCodec encodes task values and
// is used for both the active queue and dead-letter records.
type RetryingDeduplicatingPriorityVisibilityQueueCodec[T any] struct {
	Encode func(T) ([]byte, error)
	Decode func([]byte) (T, error)
}

// RetryingDeduplicatingPriorityVisibilityQueueDeadLetter is a failed task
// removed from active work after reaching the configured retry policy.
type RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T any] struct {
	ID       uint64
	Key      string
	Priority int64
	Value    T
	Attempts uint32
	Reason   RetryDeadLetterReason
}

// RetryingDeduplicatingPriorityVisibilityQueueMetrics combines queue pressure
// and consumer progress with the current dead-letter count and retry policy.
type RetryingDeduplicatingPriorityVisibilityQueueMetrics struct {
	PriorityVisibilityQueueMetrics
	DeadLetters int
	MaxAttempts uint32
}

// RetryingDeduplicatingPriorityVisibilityQueueSnapshot is a checkpoint of
// active work, retry policy, and dead-letter records.
type RetryingDeduplicatingPriorityVisibilityQueueSnapshot[T any] struct {
	Queue       DeduplicatingPriorityVisibilityQueueSnapshot[T]
	MaxAttempts uint32
	DeadLetters []RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T]
}

// RetryingDeduplicatingPriorityVisibilityQueue adds retry limits and an
// in-memory dead-letter collection to the client-key deduplicating queue. It
// is non-thread-safe like the wrapped queue.
type RetryingDeduplicatingPriorityVisibilityQueue[T any] struct {
	queue        *DeduplicatingPriorityVisibilityQueue[T]
	maxAttempts  uint32
	active       map[uint64]DeduplicatingPriorityVisibilityQueueItem[T]
	deadLetters  []RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T]
	deadLetterAt int
	nextExpiry   time.Time
}

// NewRetryingDeduplicatingPriorityVisibilityQueue creates an opt-in retrying
// queue. A zero maxAttempts allows unlimited retries.
func NewRetryingDeduplicatingPriorityVisibilityQueue[T any](capacity int, visibilityTimeout time.Duration, maxAttempts uint32) *RetryingDeduplicatingPriorityVisibilityQueue[T] {
	return NewRetryingDeduplicatingPriorityVisibilityQueueWithOptions[T](RetryingDeduplicatingPriorityVisibilityQueueOptions{
		PriorityVisibilityQueueOptions: PriorityVisibilityQueueOptions{
			Capacity:          capacity,
			VisibilityTimeout: visibilityTimeout,
			Epoch:             DefaultVisibilityQueueEpoch,
		},
		MaxAttempts: maxAttempts,
	})
}

// NewRetryingDeduplicatingPriorityVisibilityQueueWithOptions creates a queue
// with the base priority/visibility options and a retry limit.
func NewRetryingDeduplicatingPriorityVisibilityQueueWithOptions[T any](options RetryingDeduplicatingPriorityVisibilityQueueOptions) *RetryingDeduplicatingPriorityVisibilityQueue[T] {
	return &RetryingDeduplicatingPriorityVisibilityQueue[T]{
		queue:       NewDeduplicatingPriorityVisibilityQueueWithOptions[T](options.PriorityVisibilityQueueOptions),
		maxAttempts: options.MaxAttempts,
		active:      make(map[uint64]DeduplicatingPriorityVisibilityQueueItem[T]),
	}
}

// Len returns active pending plus leased work, excluding dead letters.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) Len() int {
	if queue == nil {
		return 0
	}
	return queue.queue.Len()
}

// PendingLen returns pending work, including delayed retries.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) PendingLen() int {
	if queue == nil {
		return 0
	}
	return queue.queue.PendingLen()
}

// LeaseLen returns active leases.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) LeaseLen() int {
	if queue == nil {
		return 0
	}
	return queue.queue.LeaseLen()
}

// DeadLetterLen returns the number of failed tasks waiting for inspection or
// forwarding.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) DeadLetterLen() int {
	if queue == nil {
		return 0
	}
	return len(queue.deadLetters) - queue.deadLetterAt
}

// Metrics returns the underlying queue metrics plus retry/dead-letter policy
// state. Age fields are available when EnableMetrics was set in the options.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) Metrics(now time.Time) RetryingDeduplicatingPriorityVisibilityQueueMetrics {
	if queue == nil {
		return RetryingDeduplicatingPriorityVisibilityQueueMetrics{}
	}
	return RetryingDeduplicatingPriorityVisibilityQueueMetrics{
		PriorityVisibilityQueueMetrics: queue.queue.Metrics(now),
		DeadLetters:                    queue.DeadLetterLen(),
		MaxAttempts:                    queue.maxAttempts,
	}
}

// Enqueue adds ready work identified by key.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) Enqueue(key string, priority int64, value T) bool {
	if queue == nil {
		return false
	}
	return queue.queue.Enqueue(key, priority, value)
}

// EnqueueAt adds work that becomes ready at readyAt.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) EnqueueAt(key string, priority int64, readyAt time.Time, value T) bool {
	if queue == nil {
		return false
	}
	return queue.queue.EnqueueAt(key, priority, readyAt, value)
}

// EnqueueAfter adds work after delay has elapsed from now.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) EnqueueAfter(key string, priority int64, now time.Time, delay time.Duration, value T) bool {
	if queue == nil {
		return false
	}
	return queue.queue.EnqueueAfter(key, priority, now, delay, value)
}

// Lease returns the next ready task and records its attempt for retry policy.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) Lease(now time.Time) (DeduplicatingPriorityVisibilityQueueItem[T], bool) {
	if queue == nil {
		return DeduplicatingPriorityVisibilityQueueItem[T]{}, false
	}
	queue.requeueExpiredIfDue(now)
	item, ok := queue.queue.Lease(now)
	if !ok {
		return DeduplicatingPriorityVisibilityQueueItem[T]{}, false
	}
	queue.ensureActive()[item.ID] = item
	queue.trackExpiry(item.LeaseUntil)
	return item, true
}

// LeaseWithToken returns the next ready task with an epoch-fenced token.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) LeaseWithToken(now time.Time) (DeduplicatingPriorityVisibilityQueueLease[T], bool) {
	if queue == nil {
		return DeduplicatingPriorityVisibilityQueueLease[T]{}, false
	}
	queue.requeueExpiredIfDue(now)
	item, ok := queue.queue.LeaseWithToken(now)
	if !ok {
		return DeduplicatingPriorityVisibilityQueueLease[T]{}, false
	}
	queue.ensureActive()[item.Token.ID] = DeduplicatingPriorityVisibilityQueueItem[T]{
		ID:         item.Token.ID,
		Key:        item.Key,
		Priority:   item.Priority,
		Value:      item.Value,
		Attempts:   item.Attempts,
		LeaseUntil: item.LeaseUntil,
	}
	queue.trackExpiry(item.LeaseUntil)
	return item, true
}

// Ack permanently removes active work and releases its key.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) Ack(id uint64) bool {
	if queue == nil || !queue.queue.Ack(id) {
		return false
	}
	delete(queue.ensureActive(), id)
	return true
}

// AckToken acknowledges active work with an epoch-fenced token.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) AckToken(token PriorityVisibilityQueueLeaseToken) bool {
	if queue == nil || !queue.queue.AckToken(token) {
		return false
	}
	delete(queue.ensureActive(), token.ID)
	return true
}

// Nack retries a task unless its attempt count has reached MaxAttempts. At
// the limit the task is acknowledged from active work and routed to the dead
// letter collection.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) Nack(id uint64, readyAt time.Time) bool {
	if queue == nil {
		return false
	}
	item, ok := queue.ensureActive()[id]
	if !ok {
		return false
	}
	if queue.shouldDeadLetter(item) {
		if !queue.queue.Ack(id) {
			return false
		}
		delete(queue.active, id)
		queue.appendDeadLetter(item, RetryDeadLetterMaxAttempts)
		return true
	}
	if !queue.queue.Nack(id, readyAt) {
		return false
	}
	delete(queue.active, id)
	return true
}

// NackToken retries or dead-letters a task after validating its epoch-fenced
// token.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) NackToken(token PriorityVisibilityQueueLeaseToken, readyAt time.Time) bool {
	if queue == nil {
		return false
	}
	item, ok := queue.ensureActive()[token.ID]
	if !ok {
		return false
	}
	if queue.shouldDeadLetter(item) {
		if !queue.queue.AckToken(token) {
			return false
		}
		delete(queue.active, token.ID)
		queue.appendDeadLetter(item, RetryDeadLetterMaxAttempts)
		return true
	}
	if !queue.queue.NackToken(token, readyAt) {
		return false
	}
	delete(queue.active, token.ID)
	return true
}

// RequeueExpired retries expired leases and dead-letters expired tasks that
// have reached MaxAttempts. The return value counts every expired lease
// processed, whether requeued or dead-lettered.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) RequeueExpired(now time.Time) int {
	if queue == nil {
		return 0
	}
	return queue.requeueExpiredIfDue(now)
}

func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) requeueExpiredIfDue(now time.Time) int {
	if queue.nextExpiry.IsZero() || queue.nextExpiry.After(now) {
		return 0
	}
	active := queue.ensureActive()
	expired := make([]uint64, 0)
	processed := 0
	for id, item := range active {
		if item.LeaseUntil.After(now) {
			continue
		}
		if queue.shouldDeadLetter(item) {
			if queue.queue.Ack(id) {
				delete(active, id)
				queue.appendDeadLetter(item, RetryDeadLetterVisibilityExpired)
				processed++
			}
			continue
		}
		expired = append(expired, id)
	}
	processed += queue.queue.RequeueExpired(now)
	for _, id := range expired {
		delete(active, id)
	}
	queue.recomputeNextExpiry()
	return processed
}

// PopDeadLetter removes and returns the oldest dead-letter task.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) PopDeadLetter() (RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T], bool) {
	if queue == nil || queue.deadLetterAt >= len(queue.deadLetters) {
		return RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T]{}, false
	}
	item := queue.deadLetters[queue.deadLetterAt]
	queue.deadLetters[queue.deadLetterAt] = RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T]{}
	queue.deadLetterAt++
	if queue.deadLetterAt == len(queue.deadLetters) {
		if cap(queue.deadLetters) > 64 {
			queue.deadLetters = nil
		} else {
			queue.deadLetters = queue.deadLetters[:0]
		}
		queue.deadLetterAt = 0
	}
	return item, true
}

// DeadLetters appends a copy of all waiting dead letters to dst.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) DeadLetters(dst []RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T]) []RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T] {
	if queue == nil || queue.deadLetterAt >= len(queue.deadLetters) {
		return dst
	}
	return append(dst, queue.deadLetters[queue.deadLetterAt:]...)
}

// Clear removes active work and dead letters.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) Clear() {
	if queue == nil {
		return
	}
	queue.queue.Clear()
	queue.active = nil
	queue.deadLetters = nil
	queue.deadLetterAt = 0
	queue.nextExpiry = time.Time{}
}

// Snapshot returns active queue state and all unconsumed dead letters.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) Snapshot() RetryingDeduplicatingPriorityVisibilityQueueSnapshot[T] {
	if queue == nil {
		return RetryingDeduplicatingPriorityVisibilityQueueSnapshot[T]{}
	}
	deadLetters := append([]RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T](nil), queue.DeadLetters(nil)...)
	return RetryingDeduplicatingPriorityVisibilityQueueSnapshot[T]{
		Queue:       queue.queue.Snapshot(),
		MaxAttempts: queue.maxAttempts,
		DeadLetters: deadLetters,
	}
}

// RestoreRetryingDeduplicatingPriorityVisibilityQueue restores active and
// dead-letter state. A non-zero epoch overrides the snapshot epoch.
func RestoreRetryingDeduplicatingPriorityVisibilityQueue[T any](snapshot RetryingDeduplicatingPriorityVisibilityQueueSnapshot[T], epoch uint64) (*RetryingDeduplicatingPriorityVisibilityQueue[T], error) {
	for _, dead := range snapshot.DeadLetters {
		if !validDeduplicatingPriorityVisibilityQueueKey(dead.Key) || !validRetryDeadLetterReason(dead.Reason) {
			return nil, errRetryingDeduplicatingPriorityVisibilityQueueInvalid
		}
	}
	base, err := RestoreDeduplicatingPriorityVisibilityQueue(snapshot.Queue, epoch)
	if err != nil {
		return nil, err
	}
	queue := &RetryingDeduplicatingPriorityVisibilityQueue[T]{
		queue:       base,
		maxAttempts: snapshot.MaxAttempts,
		active:      make(map[uint64]DeduplicatingPriorityVisibilityQueueItem[T], len(snapshot.Queue.Leases)),
		deadLetters: append([]RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T](nil), snapshot.DeadLetters...),
	}
	for _, lease := range snapshot.Queue.Leases {
		queue.active[lease.ID] = DeduplicatingPriorityVisibilityQueueItem[T]{
			ID:         lease.ID,
			Key:        lease.Key,
			Priority:   lease.Priority,
			Value:      lease.Value,
			Attempts:   lease.Attempts,
			LeaseUntil: lease.LeaseUntil,
		}
	}
	queue.recomputeNextExpiry()
	return queue, nil
}

// MarshalSnapshot encodes active state and dead letters with a CRC-protected
// bounded binary envelope.
func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) MarshalSnapshot(codec RetryingDeduplicatingPriorityVisibilityQueueCodec[T]) ([]byte, error) {
	if queue == nil || codec.Encode == nil {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueNilCodec
	}
	base, err := queue.queue.MarshalSnapshot(DeduplicatingPriorityVisibilityQueueCodec[T]{
		Encode: codec.Encode,
		Decode: codec.Decode,
	})
	if err != nil {
		return nil, err
	}
	snapshot := queue.Snapshot()
	if uint64(len(base)) > uint64(^uint32(0)) || len(snapshot.DeadLetters) > int(^uint32(0)) {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueTooBig
	}
	if len(snapshot.DeadLetters) > priorityVisibilityQueueMaxEntries {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueTooBig
	}
	payload := make([]byte, 0, retryingDeduplicatingPriorityVisibilityQueueHeaderSize+len(base)+len(snapshot.DeadLetters)*64+priorityVisibilityQueueChecksumSize)
	header := make([]byte, retryingDeduplicatingPriorityVisibilityQueueHeaderSize)
	copy(header[:4], retryingDeduplicatingPriorityVisibilityQueueMagic[:])
	header[4] = retryingDeduplicatingPriorityVisibilityQueueFormatVersion
	binary.LittleEndian.PutUint32(header[8:12], snapshot.MaxAttempts)
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(base)))
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(snapshot.DeadLetters)))
	payload = append(payload, header...)
	payload = append(payload, base...)
	for _, dead := range snapshot.DeadLetters {
		encoded, err := encodeRetryDeadLetter(dead, codec.Encode)
		if err != nil {
			return nil, err
		}
		if uint64(len(payload))+uint64(len(encoded))+priorityVisibilityQueueChecksumSize > uint64(priorityVisibilityQueueMaxSnapshotBytes) {
			return nil, errRetryingDeduplicatingPriorityVisibilityQueueTooBig
		}
		payload = append(payload, encoded...)
	}
	checksum := crc32.Checksum(payload, crc32.IEEETable)
	var checksumBytes [priorityVisibilityQueueChecksumSize]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	payload = append(payload, checksumBytes[:]...)
	if len(payload) > priorityVisibilityQueueMaxSnapshotBytes {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueTooBig
	}
	return payload, nil
}

// UnmarshalRetryingDeduplicatingPriorityVisibilityQueue decodes a snapshot
// without advancing its epoch.
func UnmarshalRetryingDeduplicatingPriorityVisibilityQueue[T any](data []byte, codec RetryingDeduplicatingPriorityVisibilityQueueCodec[T]) (*RetryingDeduplicatingPriorityVisibilityQueue[T], error) {
	return unmarshalRetryingDeduplicatingPriorityVisibilityQueue(data, codec, false)
}

// SaveRetryingDeduplicatingPriorityVisibilityQueue atomically writes a
// snapshot with restrictive file permissions.
func SaveRetryingDeduplicatingPriorityVisibilityQueue[T any](path string, queue *RetryingDeduplicatingPriorityVisibilityQueue[T], codec RetryingDeduplicatingPriorityVisibilityQueueCodec[T]) error {
	if path == "" {
		return errRetryingDeduplicatingPriorityVisibilityQueueEmptyPath
	}
	payload, err := queue.MarshalSnapshot(codec)
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	base := filepath.Base(path)
	temporary, err := os.CreateTemp(directory, "."+base+".tmp-*")
	if err != nil {
		return fmt.Errorf("hatriecache: create retrying queue snapshot temp file: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("hatriecache: chmod retrying queue snapshot: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		temporary.Close()
		return fmt.Errorf("hatriecache: write retrying queue snapshot: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("hatriecache: sync retrying queue snapshot: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("hatriecache: close retrying queue snapshot: %w", err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return fmt.Errorf("hatriecache: publish retrying queue snapshot: %w", err)
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("hatriecache: open retrying queue snapshot directory: %w", err)
	}
	if err := directoryFile.Sync(); err != nil {
		directoryFile.Close()
		return fmt.Errorf("hatriecache: sync retrying queue snapshot directory: %w", err)
	}
	if err := directoryFile.Close(); err != nil {
		return fmt.Errorf("hatriecache: close retrying queue snapshot directory: %w", err)
	}
	return nil
}

// LoadRetryingDeduplicatingPriorityVisibilityQueue loads a snapshot and
// advances its epoch so pre-restart tokens cannot acknowledge restored work.
func LoadRetryingDeduplicatingPriorityVisibilityQueue[T any](path string, codec RetryingDeduplicatingPriorityVisibilityQueueCodec[T]) (*RetryingDeduplicatingPriorityVisibilityQueue[T], error) {
	if path == "" {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueEmptyPath
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("hatriecache: open retrying queue snapshot: %w", err)
	}
	info, statErr := file.Stat()
	if statErr != nil {
		file.Close()
		return nil, fmt.Errorf("hatriecache: stat retrying queue snapshot: %w", statErr)
	}
	if info.Size() < 0 || info.Size() > priorityVisibilityQueueMaxSnapshotBytes {
		file.Close()
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueTooBig
	}
	payload, readErr := io.ReadAll(io.LimitReader(file, priorityVisibilityQueueMaxSnapshotBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, fmt.Errorf("hatriecache: read retrying queue snapshot: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("hatriecache: close retrying queue snapshot: %w", closeErr)
	}
	if len(payload) > priorityVisibilityQueueMaxSnapshotBytes {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueTooBig
	}
	return unmarshalRetryingDeduplicatingPriorityVisibilityQueue(payload, codec, true)
}

func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) ensureActive() map[uint64]DeduplicatingPriorityVisibilityQueueItem[T] {
	if queue.active == nil {
		queue.active = make(map[uint64]DeduplicatingPriorityVisibilityQueueItem[T])
	}
	return queue.active
}

func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) trackExpiry(until time.Time) {
	if until.IsZero() || (!queue.nextExpiry.IsZero() && !until.Before(queue.nextExpiry)) {
		return
	}
	queue.nextExpiry = until
}

func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) recomputeNextExpiry() {
	queue.nextExpiry = time.Time{}
	for _, item := range queue.active {
		queue.trackExpiry(item.LeaseUntil)
	}
}

func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) shouldDeadLetter(item DeduplicatingPriorityVisibilityQueueItem[T]) bool {
	return queue.maxAttempts != 0 && item.Attempts >= queue.maxAttempts
}

func (queue *RetryingDeduplicatingPriorityVisibilityQueue[T]) appendDeadLetter(item DeduplicatingPriorityVisibilityQueueItem[T], reason RetryDeadLetterReason) {
	queue.deadLetters = append(queue.deadLetters, RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T]{
		ID:       item.ID,
		Key:      item.Key,
		Priority: item.Priority,
		Value:    item.Value,
		Attempts: item.Attempts,
		Reason:   reason,
	})
}

func validRetryDeadLetterReason(reason RetryDeadLetterReason) bool {
	return reason == RetryDeadLetterMaxAttempts || reason == RetryDeadLetterVisibilityExpired
}

func encodeRetryDeadLetter[T any](dead RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T], encode func(T) ([]byte, error)) ([]byte, error) {
	if !validDeduplicatingPriorityVisibilityQueueKey(dead.Key) || !validRetryDeadLetterReason(dead.Reason) {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueInvalid
	}
	value, err := encode(dead.Value)
	if err != nil {
		return nil, err
	}
	if len(value) > priorityVisibilityQueueMaxValueBytes {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueTooBig
	}
	encoded := make([]byte, retryingDeduplicatingPriorityVisibilityQueueRecordHeaderSize+len(dead.Key)+len(value))
	binary.LittleEndian.PutUint64(encoded[0:8], dead.ID)
	binary.LittleEndian.PutUint64(encoded[8:16], uint64(dead.Priority))
	binary.LittleEndian.PutUint32(encoded[16:20], dead.Attempts)
	encoded[20] = byte(dead.Reason)
	binary.LittleEndian.PutUint32(encoded[24:28], uint32(len(dead.Key)))
	binary.LittleEndian.PutUint32(encoded[28:32], uint32(len(value)))
	copy(encoded[32:], dead.Key)
	copy(encoded[32+len(dead.Key):], value)
	return encoded, nil
}

func unmarshalRetryingDeduplicatingPriorityVisibilityQueue[T any](data []byte, codec RetryingDeduplicatingPriorityVisibilityQueueCodec[T], advanceEpoch bool) (*RetryingDeduplicatingPriorityVisibilityQueue[T], error) {
	if codec.Decode == nil {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueNilCodec
	}
	if len(data) < retryingDeduplicatingPriorityVisibilityQueueHeaderSize+priorityVisibilityQueueChecksumSize || len(data) > priorityVisibilityQueueMaxSnapshotBytes {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueInvalid
	}
	payload := data[:len(data)-priorityVisibilityQueueChecksumSize]
	wantChecksum := binary.LittleEndian.Uint32(data[len(data)-priorityVisibilityQueueChecksumSize:])
	if crc32.Checksum(payload, crc32.IEEETable) != wantChecksum {
		return nil, errors.New("hatriecache: retrying deduplicating priority visibility queue checksum mismatch")
	}
	if string(payload[:4]) != string(retryingDeduplicatingPriorityVisibilityQueueMagic[:]) || payload[4] != retryingDeduplicatingPriorityVisibilityQueueFormatVersion || payload[5] != 0 || payload[6] != 0 || payload[7] != 0 || payload[20] != 0 || payload[21] != 0 || payload[22] != 0 || payload[23] != 0 {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueInvalid
	}
	baseLength := binary.LittleEndian.Uint32(payload[12:16])
	deadCount := binary.LittleEndian.Uint32(payload[16:20])
	if baseLength > uint32(priorityVisibilityQueueMaxSnapshotBytes) || deadCount > uint32(priorityVisibilityQueueMaxEntries) {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueInvalid
	}
	baseStart := retryingDeduplicatingPriorityVisibilityQueueHeaderSize
	baseEnd := baseStart + int(baseLength)
	if baseEnd < baseStart || baseEnd > len(payload) {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueInvalid
	}
	base, err := UnmarshalDeduplicatingPriorityVisibilityQueue(payload[baseStart:baseEnd], DeduplicatingPriorityVisibilityQueueCodec[T]{
		Encode: codec.Encode,
		Decode: codec.Decode,
	})
	if err != nil {
		return nil, err
	}
	deadLetters := make([]RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T], 0, int(deadCount))
	position := baseEnd
	for index := uint32(0); index < deadCount; index++ {
		if len(payload)-position < retryingDeduplicatingPriorityVisibilityQueueRecordHeaderSize {
			return nil, errRetryingDeduplicatingPriorityVisibilityQueueInvalid
		}
		keyLength := binary.LittleEndian.Uint32(payload[position+24 : position+28])
		valueLength := binary.LittleEndian.Uint32(payload[position+28 : position+32])
		if keyLength == 0 || keyLength > deduplicatingPriorityVisibilityQueueMaxKeyBytes || valueLength > priorityVisibilityQueueMaxValueBytes {
			return nil, errRetryingDeduplicatingPriorityVisibilityQueueInvalid
		}
		if payload[position+21] != 0 || payload[position+22] != 0 || payload[position+23] != 0 {
			return nil, errRetryingDeduplicatingPriorityVisibilityQueueInvalid
		}
		recordLength := retryingDeduplicatingPriorityVisibilityQueueRecordHeaderSize + uint64(keyLength) + uint64(valueLength)
		if recordLength > uint64(len(payload)-position) {
			return nil, errRetryingDeduplicatingPriorityVisibilityQueueInvalid
		}
		keyStart := position + retryingDeduplicatingPriorityVisibilityQueueRecordHeaderSize
		keyEnd := keyStart + int(keyLength)
		valueEnd := keyEnd + int(valueLength)
		value, err := codec.Decode(payload[keyEnd:valueEnd])
		if err != nil {
			return nil, fmt.Errorf("decode retrying queue dead letter: %w", err)
		}
		reason := RetryDeadLetterReason(payload[position+20])
		if !validRetryDeadLetterReason(reason) {
			return nil, errRetryingDeduplicatingPriorityVisibilityQueueInvalid
		}
		deadLetters = append(deadLetters, RetryingDeduplicatingPriorityVisibilityQueueDeadLetter[T]{
			ID:       binary.LittleEndian.Uint64(payload[position : position+8]),
			Priority: int64(binary.LittleEndian.Uint64(payload[position+8 : position+16])),
			Attempts: binary.LittleEndian.Uint32(payload[position+16 : position+20]),
			Reason:   reason,
			Key:      string(payload[keyStart:keyEnd]),
			Value:    value,
		})
		position += int(recordLength)
	}
	if position != len(payload) {
		return nil, errRetryingDeduplicatingPriorityVisibilityQueueInvalid
	}
	if advanceEpoch {
		baseSnapshot := base.Snapshot()
		if baseSnapshot.Epoch == ^uint64(0) {
			return nil, errRetryingDeduplicatingPriorityVisibilityQueueEpoch
		}
		base, err = RestoreDeduplicatingPriorityVisibilityQueue(baseSnapshot, baseSnapshot.Epoch+1)
		if err != nil {
			return nil, err
		}
	}
	queue := &RetryingDeduplicatingPriorityVisibilityQueue[T]{
		queue:       base,
		maxAttempts: binary.LittleEndian.Uint32(payload[8:12]),
		active:      make(map[uint64]DeduplicatingPriorityVisibilityQueueItem[T], len(base.Snapshot().Leases)),
		deadLetters: deadLetters,
	}
	for _, lease := range base.Snapshot().Leases {
		queue.active[lease.ID] = DeduplicatingPriorityVisibilityQueueItem[T]{
			ID:         lease.ID,
			Key:        lease.Key,
			Priority:   lease.Priority,
			Value:      lease.Value,
			Attempts:   lease.Attempts,
			LeaseUntil: lease.LeaseUntil,
		}
	}
	queue.recomputeNextExpiry()
	return queue, nil
}
