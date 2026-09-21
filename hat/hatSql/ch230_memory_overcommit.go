package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrSQLMemoryOvercommitInvalid identifies malformed queue configuration or
	// a negative reservation.
	ErrSQLMemoryOvercommitInvalid = errors.New("hatSql: invalid SQL memory overcommit configuration")
	// ErrSQLMemoryOvercommitRequestTooLarge identifies a reservation that can
	// never fit in the configured queue capacity.
	ErrSQLMemoryOvercommitRequestTooLarge = errors.New("hatSql: SQL memory overcommit request exceeds queue capacity")
	// ErrSQLMemoryOvercommitQueueFull identifies a bounded wait queue with no
	// room for another waiter.
	ErrSQLMemoryOvercommitQueueFull = errors.New("hatSql: SQL memory overcommit wait queue is full")
)

const (
	// DefaultSQLMemoryOvercommitMaxWaiters bounds queued queries when callers do
	// not provide an explicit queue length.
	DefaultSQLMemoryOvercommitMaxWaiters = 64
	// MaxSQLMemoryOvercommitMaxWaiters prevents an accidental unbounded waiter
	// slice from becoming a second memory pressure source.
	MaxSQLMemoryOvercommitMaxWaiters = 1 << 20
)

// SQLMemoryOvercommitOptions configures an opt-in shared memory reservation
// queue. The queue does not estimate memory itself; SQL operators report their
// retained working bytes through SQLQueryOptions.MemoryOvercommit.
type SQLMemoryOvercommitOptions struct {
	LimitBytes int64 `json:"limit_bytes"`
	MaxWaiters int   `json:"max_waiters,omitempty"`
}

// SQLMemoryOvercommitStats is a point-in-time queue snapshot.
type SQLMemoryOvercommitStats struct {
	LimitBytes    int64  `json:"limit_bytes"`
	UsedBytes     int64  `json:"used_bytes"`
	Waiters       int    `json:"waiters"`
	Grants        uint64 `json:"grants"`
	Cancellations uint64 `json:"cancellations"`
}

// SQLMemoryOvercommitQueue coordinates opt-in retained-memory reservations
// across concurrent queries. Immediate reservations take one mutex pass and
// do not allocate; only blocked callers allocate a waiter and channel.
type SQLMemoryOvercommitQueue struct {
	mu         sync.Mutex
	limitBytes int64
	maxWaiters int
	usedBytes  int64
	waiters    []*sqlMemoryOvercommitWaiter
	grants     uint64
	canceled   uint64
}

type sqlMemoryOvercommitWaiter struct {
	bytes   int64
	ready   chan struct{}
	granted bool
}

// NewSQLMemoryOvercommitQueue creates an opt-in shared reservation queue.
// LimitBytes must be positive. A zero MaxWaiters selects the bounded default.
func NewSQLMemoryOvercommitQueue(options SQLMemoryOvercommitOptions) (*SQLMemoryOvercommitQueue, error) {
	if options.LimitBytes <= 0 {
		return nil, fmt.Errorf("%w: limit bytes must be positive", ErrSQLMemoryOvercommitInvalid)
	}
	maxWaiters := options.MaxWaiters
	if maxWaiters == 0 {
		maxWaiters = DefaultSQLMemoryOvercommitMaxWaiters
	}
	if maxWaiters < 1 || maxWaiters > MaxSQLMemoryOvercommitMaxWaiters {
		return nil, fmt.Errorf("%w: max waiters must be between 1 and %d", ErrSQLMemoryOvercommitInvalid, MaxSQLMemoryOvercommitMaxWaiters)
	}
	return &SQLMemoryOvercommitQueue{
		limitBytes: options.LimitBytes,
		maxWaiters: maxWaiters,
		waiters:    make([]*sqlMemoryOvercommitWaiter, 0, maxWaiters),
	}, nil
}

// Acquire reserves bytes or waits until an existing reservation is released.
// Context cancellation removes a blocked waiter without consuming capacity.
// Callers must pair every successful acquire with Release using the same byte
// count.
func (queue *SQLMemoryOvercommitQueue) Acquire(ctx context.Context, bytes int64) error {
	if queue == nil {
		return fmt.Errorf("%w: queue is nil", ErrSQLMemoryOvercommitInvalid)
	}
	if bytes < 0 {
		return fmt.Errorf("%w: reservation bytes cannot be negative", ErrSQLMemoryOvercommitInvalid)
	}
	if bytes == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	queue.mu.Lock()
	if bytes > queue.limitBytes {
		queue.mu.Unlock()
		return fmt.Errorf("%w: requested %d bytes, limit %d", ErrSQLMemoryOvercommitRequestTooLarge, bytes, queue.limitBytes)
	}
	if len(queue.waiters) == 0 && queue.canGrantLocked(bytes) {
		queue.usedBytes += bytes
		queue.grants++
		queue.mu.Unlock()
		return nil
	}
	if len(queue.waiters) >= queue.maxWaiters {
		queue.mu.Unlock()
		return fmt.Errorf("%w: maximum %d waiters", ErrSQLMemoryOvercommitQueueFull, queue.maxWaiters)
	}
	waiter := &sqlMemoryOvercommitWaiter{bytes: bytes, ready: make(chan struct{})}
	queue.waiters = append(queue.waiters, waiter)
	queue.mu.Unlock()

	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
		queue.mu.Lock()
		if waiter.granted {
			queue.releaseLocked(bytes)
			queue.mu.Unlock()
			return ctx.Err()
		}
		queue.removeWaiterLocked(waiter)
		queue.canceled++
		queue.mu.Unlock()
		return ctx.Err()
	}
}

// Release returns a successful reservation to the queue and wakes any fitting
// waiters. Over-release is clamped to zero so cleanup paths remain idempotent.
func (queue *SQLMemoryOvercommitQueue) Release(bytes int64) {
	if queue == nil || bytes <= 0 {
		return
	}
	queue.mu.Lock()
	queue.releaseLocked(bytes)
	queue.mu.Unlock()
}

// Snapshot returns queue state without exposing mutable waiter internals.
func (queue *SQLMemoryOvercommitQueue) Snapshot() SQLMemoryOvercommitStats {
	if queue == nil {
		return SQLMemoryOvercommitStats{}
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return SQLMemoryOvercommitStats{
		LimitBytes:    queue.limitBytes,
		UsedBytes:     queue.usedBytes,
		Waiters:       len(queue.waiters),
		Grants:        queue.grants,
		Cancellations: queue.canceled,
	}
}

func (queue *SQLMemoryOvercommitQueue) canGrantLocked(bytes int64) bool {
	return bytes <= queue.limitBytes-queue.usedBytes
}

func (queue *SQLMemoryOvercommitQueue) releaseLocked(bytes int64) {
	if bytes >= queue.usedBytes {
		queue.usedBytes = 0
	} else {
		queue.usedBytes -= bytes
	}
	queue.grantWaitersLocked()
}

func (queue *SQLMemoryOvercommitQueue) grantWaitersLocked() {
	for {
		selected := -1
		for index, waiter := range queue.waiters {
			if queue.canGrantLocked(waiter.bytes) {
				selected = index
				break
			}
		}
		if selected < 0 {
			return
		}
		waiter := queue.waiters[selected]
		copy(queue.waiters[selected:], queue.waiters[selected+1:])
		queue.waiters[len(queue.waiters)-1] = nil
		queue.waiters = queue.waiters[:len(queue.waiters)-1]
		waiter.granted = true
		queue.usedBytes += waiter.bytes
		queue.grants++
		close(waiter.ready)
	}
}

func (queue *SQLMemoryOvercommitQueue) removeWaiterLocked(target *sqlMemoryOvercommitWaiter) {
	for index, waiter := range queue.waiters {
		if waiter != target {
			continue
		}
		copy(queue.waiters[index:], queue.waiters[index+1:])
		queue.waiters[len(queue.waiters)-1] = nil
		queue.waiters = queue.waiters[:len(queue.waiters)-1]
		return
	}
}
