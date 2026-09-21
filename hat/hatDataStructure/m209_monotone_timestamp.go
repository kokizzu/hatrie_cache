package hatDataStructure

import (
	"errors"
	"fmt"
	"sync/atomic"
)

var (
	ErrMonotoneLogicalTimestampNil       = errors.New("monotone logical timestamp is nil")
	ErrMonotoneLogicalTimestampRegressed = errors.New("monotone logical timestamp regressed")
)

// MonotoneLogicalTimestamp is a lock-free scalar frontier. Its zero value is
// ready for use, equal advances are idempotent, and lower advances are
// rejected.
type MonotoneLogicalTimestamp struct {
	value uint64
}

// NewMonotoneLogicalTimestamp creates a timestamp at initial.
func NewMonotoneLogicalTimestamp(initial uint64) MonotoneLogicalTimestamp {
	return MonotoneLogicalTimestamp{value: initial}
}

// Current returns the greatest timestamp observed so far. A nil receiver
// returns zero.
func (timestamp *MonotoneLogicalTimestamp) Current() uint64 {
	if timestamp == nil {
		return 0
	}
	return atomic.LoadUint64(&timestamp.value)
}

// CompareAndSwap conditionally publishes next when the current value equals
// expected. It is provided for adapters that need to preserve a custom hot
// path while sharing the timestamp's atomic storage.
func (timestamp *MonotoneLogicalTimestamp) CompareAndSwap(expected, next uint64) bool {
	return timestamp != nil && atomic.CompareAndSwapUint64(&timestamp.value, expected, next)
}

// AtLeast reports whether the timestamp has reached target. A nil receiver
// is never ready, including for target zero.
func (timestamp *MonotoneLogicalTimestamp) AtLeast(target uint64) bool {
	return timestamp != nil && timestamp.Current() >= target
}

// AdvanceIfNewer publishes next only when it is greater than the current
// value. It is the allocation-free hot path for callers that validate stale
// input separately; equal and lower values return false.
func (timestamp *MonotoneLogicalTimestamp) AdvanceIfNewer(next uint64) bool {
	if timestamp == nil {
		return false
	}
	for {
		current := atomic.LoadUint64(&timestamp.value)
		if next <= current {
			return false
		}
		if atomic.CompareAndSwapUint64(&timestamp.value, current, next) {
			return true
		}
	}
}

// Advance publishes next. Equal values return false without error. A lower
// value returns ErrMonotoneLogicalTimestampRegressed and leaves the frontier
// unchanged.
func (timestamp *MonotoneLogicalTimestamp) Advance(next uint64) (bool, error) {
	if timestamp == nil {
		return false, ErrMonotoneLogicalTimestampNil
	}
	if timestamp.AdvanceIfNewer(next) {
		return true, nil
	}
	current := timestamp.Current()
	if next < current {
		return false, fmt.Errorf("%w: current=%d requested=%d", ErrMonotoneLogicalTimestampRegressed, current, next)
	}
	return false, nil
}
