package hatSql

import (
	"context"
	"errors"
	"sync"
)

const (
	// DefaultSQLMemoryAdmissionMaxPending bounds retained memory waiters when
	// callers do not choose an explicit queue capacity.
	DefaultSQLMemoryAdmissionMaxPending = 1024
	maxSQLMemoryAdmissionMaxPending     = 100_000
)

var (
	// ErrSQLMemoryAdmissionNil reports a method call on a nil admission queue.
	ErrSQLMemoryAdmissionNil = errors.New("hatSql: memory admission controller is nil")
	// ErrSQLMemoryAdmissionOptionsInvalid reports an invalid capacity or queue
	// bound.
	ErrSQLMemoryAdmissionOptionsInvalid = errors.New("hatSql: memory admission options are invalid")
	// ErrSQLMemoryAdmissionRequestInvalid reports a non-positive reservation
	// where a reservation is required.
	ErrSQLMemoryAdmissionRequestInvalid = errors.New("hatSql: memory admission request is invalid")
	// ErrSQLMemoryAdmissionRequestTooLarge reports a request that can never fit
	// under the controller's hard capacity.
	ErrSQLMemoryAdmissionRequestTooLarge = errors.New("hatSql: memory admission request exceeds capacity")
	// ErrSQLMemoryAdmissionQueueFull reports a bounded queue with no free slot.
	ErrSQLMemoryAdmissionQueueFull = errors.New("hatSql: memory admission queue is full")
	// ErrSQLMemoryAdmissionClosed reports an admission queue that stopped
	// accepting requests.
	ErrSQLMemoryAdmissionClosed = errors.New("hatSql: memory admission controller is closed")
)

// SQLMemoryAdmissionOptions configures a query memory reservation queue.
// MaxBytes is a hard resident-byte budget. MaxPending is the maximum number
// of blocked requests retained in memory; zero selects
// DefaultSQLMemoryAdmissionMaxPending.
type SQLMemoryAdmissionOptions struct {
	MaxBytes   int64
	MaxPending int
}

// SQLMemoryAdmissionStats is a point-in-time controller snapshot.
type SQLMemoryAdmissionStats struct {
	MaxBytes    int64
	ActiveBytes int64
	Pending     int
	MaxPending  int
	Admitted    uint64
	Waited      uint64
	Rejected    uint64
	Canceled    uint64
	Closed      bool
}

type sqlMemoryAdmissionRequest struct {
	bytes     int64
	result    chan error
	granted   bool
	completed bool
}

// SQLMemoryAdmission queues query reservations instead of immediately
// canceling work when the shared memory budget is full. It is safe for
// concurrent callers.
type SQLMemoryAdmission struct {
	mu          sync.Mutex
	maxBytes    int64
	maxPending  int
	activeBytes int64
	pending     []*sqlMemoryAdmissionRequest
	closed      bool
	admitted    uint64
	waited      uint64
	rejected    uint64
	canceled    uint64
}

// NewSQLMemoryAdmission creates a bounded memory reservation queue. MaxBytes
// must be positive. A zero MaxPending selects a bounded default.
func NewSQLMemoryAdmission(options SQLMemoryAdmissionOptions) (*SQLMemoryAdmission, error) {
	if options.MaxBytes <= 0 || options.MaxPending < 0 || options.MaxPending > maxSQLMemoryAdmissionMaxPending {
		return nil, ErrSQLMemoryAdmissionOptionsInvalid
	}
	if options.MaxPending == 0 {
		options.MaxPending = DefaultSQLMemoryAdmissionMaxPending
	}
	return &SQLMemoryAdmission{
		maxBytes:   options.MaxBytes,
		maxPending: options.MaxPending,
	}, nil
}

// Acquire reserves bytes or waits in FIFO order until an existing reservation
// is released. The returned release function is safe to call more than once.
// A request larger than MaxBytes is rejected immediately because it can never
// become admissible. Context cancellation removes a queued request without
// changing the active reservation total.
func (admission *SQLMemoryAdmission) Acquire(ctx context.Context, bytes int64) (func(), error) {
	if admission == nil {
		return nil, ErrSQLMemoryAdmissionNil
	}
	if bytes <= 0 {
		return nil, ErrSQLMemoryAdmissionRequestInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	admission.mu.Lock()
	if admission.closed {
		admission.rejected++
		admission.mu.Unlock()
		return nil, ErrSQLMemoryAdmissionClosed
	}
	if bytes > admission.maxBytes {
		admission.rejected++
		admission.mu.Unlock()
		return nil, ErrSQLMemoryAdmissionRequestTooLarge
	}
	if len(admission.pending) == 0 && admission.maxBytes-admission.activeBytes >= bytes {
		admission.activeBytes += bytes
		admission.admitted++
		admission.mu.Unlock()
		return admission.releaseFunc(bytes), nil
	}
	if len(admission.pending) >= admission.maxPending {
		admission.rejected++
		admission.mu.Unlock()
		return nil, ErrSQLMemoryAdmissionQueueFull
	}
	request := &sqlMemoryAdmissionRequest{bytes: bytes, result: make(chan error, 1)}
	admission.pending = append(admission.pending, request)
	admission.waited++
	admission.mu.Unlock()

	select {
	case err := <-request.result:
		if err != nil {
			return nil, err
		}
		return admission.releaseFunc(bytes), nil
	case <-ctx.Done():
		admission.mu.Lock()
		if request.granted || request.completed {
			admission.mu.Unlock()
			err := <-request.result
			if err != nil {
				return nil, err
			}
			return admission.releaseFunc(bytes), nil
		}
		if admission.removePendingLocked(request) {
			request.completed = true
			admission.canceled++
			request.result <- ctx.Err()
			admission.dispatchLocked()
		}
		admission.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (admission *SQLMemoryAdmission) releaseFunc(bytes int64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			admission.release(bytes)
		})
	}
}

func (admission *SQLMemoryAdmission) release(bytes int64) {
	admission.mu.Lock()
	if bytes >= admission.activeBytes {
		admission.activeBytes = 0
	} else {
		admission.activeBytes -= bytes
	}
	admission.dispatchLocked()
	admission.mu.Unlock()
}

func (admission *SQLMemoryAdmission) dispatchLocked() {
	for len(admission.pending) > 0 {
		request := admission.pending[0]
		if admission.maxBytes-admission.activeBytes < request.bytes {
			return
		}
		admission.pending = admission.pending[1:]
		request.granted = true
		admission.activeBytes += request.bytes
		admission.admitted++
		request.result <- nil
	}
}

func (admission *SQLMemoryAdmission) removePendingLocked(target *sqlMemoryAdmissionRequest) bool {
	for index, request := range admission.pending {
		if request != target {
			continue
		}
		copy(admission.pending[index:], admission.pending[index+1:])
		admission.pending[len(admission.pending)-1] = nil
		admission.pending = admission.pending[:len(admission.pending)-1]
		return true
	}
	return false
}

// Stats returns current memory usage and queue counters.
func (admission *SQLMemoryAdmission) Stats() SQLMemoryAdmissionStats {
	if admission == nil {
		return SQLMemoryAdmissionStats{}
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	return SQLMemoryAdmissionStats{
		MaxBytes:    admission.maxBytes,
		ActiveBytes: admission.activeBytes,
		Pending:     len(admission.pending),
		MaxPending:  admission.maxPending,
		Admitted:    admission.admitted,
		Waited:      admission.waited,
		Rejected:    admission.rejected,
		Canceled:    admission.canceled,
		Closed:      admission.closed,
	}
}

// Close rejects queued and future reservations. Existing leases remain valid
// and release their bytes normally.
func (admission *SQLMemoryAdmission) Close() {
	if admission == nil {
		return
	}
	admission.mu.Lock()
	if admission.closed {
		admission.mu.Unlock()
		return
	}
	admission.closed = true
	pending := admission.pending
	admission.pending = nil
	for _, request := range pending {
		request.completed = true
		request.result <- ErrSQLMemoryAdmissionClosed
	}
	admission.mu.Unlock()
}
