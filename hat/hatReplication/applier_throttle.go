package hatReplication

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

var (
	ErrApplierThrottleInvalidOptions = errors.New("replication applier throttle options are invalid")
	ErrApplierThrottleInvalidEntries = errors.New("replication applier throttle entry count must be positive")
)

const maxApplierThrottleBurst = 1_000_000

// ApplierThrottleOptions configures an opt-in replica apply pacer.
// EntriesPerSecond is the sustained batch-entry rate and Burst is the number
// of entries that may be reserved immediately after an idle period.
type ApplierThrottleOptions struct {
	EntriesPerSecond float64
	Burst            int
	Now              func() time.Time
}

// ApplierThrottle reserves apply capacity in journal order. It does not run
// callbacks or reorder entries; callers wait for a reservation immediately
// before applying their already ordered batch.
type ApplierThrottle struct {
	mu       sync.Mutex
	interval time.Duration
	burst    int
	next     time.Time
	now      func() time.Time
}

// NewApplierThrottle creates an opt-in replica applier throttle.
func NewApplierThrottle(options ApplierThrottleOptions) (*ApplierThrottle, error) {
	if math.IsNaN(options.EntriesPerSecond) || math.IsInf(options.EntriesPerSecond, 0) || options.EntriesPerSecond <= 0 || options.Burst <= 0 || options.Burst > maxApplierThrottleBurst {
		return nil, ErrApplierThrottleInvalidOptions
	}
	interval := time.Duration(math.Ceil(float64(time.Second) / options.EntriesPerSecond))
	if interval <= 0 {
		interval = time.Nanosecond
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &ApplierThrottle{
		interval: interval,
		burst:    options.Burst,
		now:      options.Now,
	}, nil
}

// Reserve reserves capacity for entries and returns the delay before the
// caller may apply them. Reservations are serialized so concurrent appliers
// cannot collectively exceed the configured rate.
func (throttle *ApplierThrottle) Reserve(entries int) (time.Duration, error) {
	if throttle == nil {
		return 0, ErrApplierThrottleInvalidOptions
	}
	if entries <= 0 {
		return 0, ErrApplierThrottleInvalidEntries
	}
	now := throttle.now()
	throttle.mu.Lock()
	defer throttle.mu.Unlock()

	burstWindow := applierThrottleDuration(throttle.interval, throttle.burst-1)
	floor := now.Add(-burstWindow)
	if throttle.next.IsZero() || throttle.next.Before(floor) {
		throttle.next = floor
	}
	lastToken := throttle.next.Add(applierThrottleDuration(throttle.interval, entries-1))
	delay := time.Duration(0)
	if lastToken.After(now) {
		delay = lastToken.Sub(now)
	}
	throttle.next = throttle.next.Add(applierThrottleDuration(throttle.interval, entries))
	return delay, nil
}

// Wait reserves capacity and waits for its reservation or context cancellation.
func (throttle *ApplierThrottle) Wait(ctx context.Context, entries int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	delay, err := throttle.Reserve(entries)
	if err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func applierThrottleDuration(interval time.Duration, count int) time.Duration {
	if count <= 0 {
		return 0
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if uint64(count) > uint64(maxDuration/interval) {
		return maxDuration
	}
	return interval * time.Duration(count)
}
