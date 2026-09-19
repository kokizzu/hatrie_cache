// Package hatSpill provides bounded, cancellable reservations for temporary
// spill bytes owned by one query or other short-lived operation.
package hatSpill

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
)

var (
	ErrClosed          = errors.New("spill budget is closed")
	ErrNilContext      = errors.New("spill budget requires a non-nil context")
	ErrRequestTooLarge = errors.New("spill reservation exceeds budget limit")
	ErrQuotaOverflow   = errors.New("spill budget byte counter overflow")
)

// Budget tracks the temporary bytes reserved by one query. A limit of zero
// means unlimited; callers can keep the same API while making quota enforcement
// opt in.
type Budget struct {
	mu      sync.Mutex
	limit   uint64
	used    uint64
	closed  bool
	waiters []*waiter
}

type waiter struct {
	bytes   uint64
	ready   chan struct{}
	granted bool
	err     error
}

// Snapshot is a consistent point-in-time view of a Budget.
type Snapshot struct {
	Limit     uint64
	Used      uint64
	Available uint64
	Queued    uint64
	Closed    bool
}

// Reservation represents ownership of bytes from a Budget. Copies share the
// same release token, so a reservation can safely cross helper boundaries.
type Reservation struct {
	budget   *Budget
	bytes    uint64
	released *uint32
}

// NewBudget creates a spill-byte budget. A zero limit disables enforcement but
// still records usage for diagnostics.
func NewBudget(limit uint64) *Budget {
	return &Budget{limit: limit}
}

// Limit returns the configured byte limit. Zero means unlimited.
func (b *Budget) Limit() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	limit := b.limit
	b.mu.Unlock()
	return limit
}

// Used returns the currently reserved byte count.
func (b *Budget) Used() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	used := b.used
	b.mu.Unlock()
	return used
}

// Reserve waits in FIFO order until bytes are available or ctx is canceled.
// A request larger than the finite limit is rejected immediately so it cannot
// occupy the queue forever.
func (b *Budget) Reserve(ctx context.Context, bytes uint64) (Reservation, error) {
	if b == nil {
		return Reservation{}, ErrClosed
	}
	if ctx == nil {
		return Reservation{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return Reservation{}, err
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return Reservation{}, ErrClosed
	}
	if b.requestTooLargeLocked(bytes) {
		b.mu.Unlock()
		return Reservation{}, ErrRequestTooLarge
	}
	if b.counterOverflowLocked(bytes) {
		b.mu.Unlock()
		return Reservation{}, ErrQuotaOverflow
	}
	if bytes == 0 {
		b.mu.Unlock()
		return Reservation{}, nil
	}
	if len(b.waiters) == 0 && b.canFitLocked(bytes) {
		b.used += bytes
		lease := newReservation(b, bytes)
		b.mu.Unlock()
		return lease, nil
	}

	w := &waiter{bytes: bytes, ready: make(chan struct{})}
	b.waiters = append(b.waiters, w)
	b.pumpLocked()
	b.mu.Unlock()

	select {
	case <-w.ready:
		b.mu.Lock()
		err := w.err
		granted := w.granted
		b.mu.Unlock()
		if err != nil {
			return Reservation{}, err
		}
		if !granted {
			return Reservation{}, ErrClosed
		}
		return newReservation(b, bytes), nil
	case <-ctx.Done():
		b.mu.Lock()
		if w.granted {
			b.mu.Unlock()
			// The grant won the race with cancellation. Return the accounting
			// before reporting the cancellation so no bytes leak.
			newReservation(b, bytes).Release()
			return Reservation{}, ctx.Err()
		}
		removed := b.removeWaiterLocked(w)
		if removed {
			b.pumpLocked()
		}
		b.mu.Unlock()
		return Reservation{}, ctx.Err()
	}
}

// TryReserve performs a non-blocking FIFO reservation. It returns false when
// the request would wait, exceed the limit, overflow accounting, or the budget
// is closed.
func (b *Budget) TryReserve(bytes uint64) (Reservation, bool) {
	if b == nil {
		return Reservation{}, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || bytes == 0 || len(b.waiters) != 0 || b.requestTooLargeLocked(bytes) || b.counterOverflowLocked(bytes) || !b.canFitLocked(bytes) {
		return Reservation{}, false
	}
	b.used += bytes
	return newReservation(b, bytes), true
}

// Close rejects new reservations and unblocks queued waiters. Existing
// reservations remain valid and can still be released.
func (b *Budget) Close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	for _, w := range b.waiters {
		w.err = ErrClosed
		close(w.ready)
	}
	b.waiters = nil
	b.mu.Unlock()
}

// Snapshot returns a consistent usage, queue, and closure view. For an
// unlimited budget, Available is MaxUint64 minus the recorded usage.
func (b *Budget) Snapshot() Snapshot {
	if b == nil {
		return Snapshot{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snapshotLocked()
}

// Bytes returns the amount held by the reservation.
func (r Reservation) Bytes() uint64 {
	return r.bytes
}

// Release returns the reservation's bytes. It is idempotent, including across
// copied Reservation values.
func (r Reservation) Release() bool {
	if r.budget == nil || r.released == nil || r.bytes == 0 {
		return false
	}
	if !atomic.CompareAndSwapUint32(r.released, 0, 1) {
		return false
	}
	r.budget.release(r.bytes)
	return true
}

func (b *Budget) release(bytes uint64) {
	b.mu.Lock()
	if bytes > b.used {
		b.used = 0
	} else {
		b.used -= bytes
	}
	b.pumpLocked()
	b.mu.Unlock()
}

func (b *Budget) requestTooLargeLocked(bytes uint64) bool {
	return b.limit != 0 && bytes > b.limit
}

func (b *Budget) counterOverflowLocked(bytes uint64) bool {
	return bytes > math.MaxUint64-b.used
}

func (b *Budget) canFitLocked(bytes uint64) bool {
	if bytes > math.MaxUint64-b.used {
		return false
	}
	return b.limit == 0 || bytes <= b.limit-b.used
}

func (b *Budget) pumpLocked() {
	if b.closed {
		return
	}
	for len(b.waiters) != 0 {
		w := b.waiters[0]
		if !b.canFitLocked(w.bytes) {
			return
		}
		b.waiters[0] = nil
		b.waiters = b.waiters[1:]
		b.used += w.bytes
		w.granted = true
		close(w.ready)
	}
}

func (b *Budget) removeWaiterLocked(target *waiter) bool {
	for i, w := range b.waiters {
		if w != target {
			continue
		}
		copy(b.waiters[i:], b.waiters[i+1:])
		b.waiters[len(b.waiters)-1] = nil
		b.waiters = b.waiters[:len(b.waiters)-1]
		return true
	}
	return false
}

func (b *Budget) snapshotLocked() Snapshot {
	available := math.MaxUint64 - b.used
	if b.limit != 0 {
		if b.used >= b.limit {
			available = 0
		} else {
			available = b.limit - b.used
		}
	}
	return Snapshot{
		Limit:     b.limit,
		Used:      b.used,
		Available: available,
		Queued:    uint64(len(b.waiters)),
		Closed:    b.closed,
	}
}

func newReservation(b *Budget, bytes uint64) Reservation {
	released := uint32(0)
	return Reservation{budget: b, bytes: bytes, released: &released}
}
