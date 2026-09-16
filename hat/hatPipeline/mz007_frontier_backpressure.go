package hatPipeline

import (
	"context"
	"errors"
	"sync"
)

// ErrFrontierBackpressureOptionsInvalid identifies invalid gate options.
var ErrFrontierBackpressureOptionsInvalid = errors.New("hatriecache: invalid frontier backpressure options")

// ErrFrontierBackpressureNil identifies a method call on a nil gate.
var ErrFrontierBackpressureNil = errors.New("hatriecache: nil frontier backpressure gate")

// ErrFrontierBackpressureClosed identifies an operation after Close.
var ErrFrontierBackpressureClosed = errors.New("hatriecache: frontier backpressure gate is closed")

// ErrFrontierBackpressureRegression identifies a consumer frontier that moves
// backward.
var ErrFrontierBackpressureRegression = errors.New("hatriecache: consumer frontier regressed")

// ErrFrontierBackpressureContextNil identifies a nil context passed to Wait.
var ErrFrontierBackpressureContextNil = errors.New("hatriecache: nil frontier backpressure context")

const DefaultFrontierBackpressureMaxLag uint64 = 1024

// FrontierBackpressureOptions bounds how far a producer may advance beyond
// the consumer frontier. MaxLag=0 selects the default; construct a gate only
// when a source explicitly opts into frontier admission.
type FrontierBackpressureOptions struct {
	MaxLag uint64
}

// FrontierBackpressureStats reports the current frontier state and bounded
// admission counters.
type FrontierBackpressureStats struct {
	MaxLag          uint64
	Produced        uint64
	Consumed        uint64
	Lag             uint64
	Admitted        uint64
	BlockedAttempts uint64
	Blocked         bool
	Closed          bool
}

// FrontierBackpressure is a small opt-in admission gate for source progress.
// It tracks frontiers, not source records, so it does not retain or copy input
// data. Producers can use TryAdmit for a nonblocking path or Wait when they
// want cancellation-aware blocking until the consumer catches up.
type FrontierBackpressure struct {
	mu              sync.Mutex
	maxLag          uint64
	produced        uint64
	consumed        uint64
	admitted        uint64
	blockedAttempts uint64
	waiters         int
	wake            chan struct{}
	closed          bool
}

// NewFrontierBackpressure creates an opt-in frontier gate. The zero options
// value uses DefaultFrontierBackpressureMaxLag.
func NewFrontierBackpressure(options FrontierBackpressureOptions) (*FrontierBackpressure, error) {
	maxLag := options.MaxLag
	if maxLag == 0 {
		maxLag = DefaultFrontierBackpressureMaxLag
	}
	return &FrontierBackpressure{maxLag: maxLag, wake: make(chan struct{})}, nil
}

// TryAdmit advances the observed producer frontier when it is within the
// configured lag. It returns false without an error when backpressure applies.
func (gate *FrontierBackpressure) TryAdmit(frontier uint64) (bool, error) {
	if gate == nil {
		return false, ErrFrontierBackpressureNil
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.ensureInitializedLocked()
	if gate.closed {
		return false, ErrFrontierBackpressureClosed
	}
	if frontier > frontierBackpressureLimit(gate.consumed, gate.maxLag) {
		gate.blockedAttempts++
		return false, nil
	}
	gate.admitLocked(frontier)
	return true, nil
}

// Wait blocks until frontier is within the configured lag or ctx is canceled.
// A nil context is rejected before any gate state changes.
func (gate *FrontierBackpressure) Wait(ctx context.Context, frontier uint64) error {
	if gate == nil {
		return ErrFrontierBackpressureNil
	}
	if ctx == nil {
		return ErrFrontierBackpressureContextNil
	}
	for {
		gate.mu.Lock()
		gate.ensureInitializedLocked()
		if gate.closed {
			gate.mu.Unlock()
			return ErrFrontierBackpressureClosed
		}
		if frontier <= frontierBackpressureLimit(gate.consumed, gate.maxLag) {
			gate.admitLocked(frontier)
			gate.mu.Unlock()
			return nil
		}
		gate.blockedAttempts++
		gate.waiters++
		wake := gate.wake
		gate.mu.Unlock()

		select {
		case <-ctx.Done():
			gate.mu.Lock()
			gate.waiters--
			gate.mu.Unlock()
			return ctx.Err()
		case <-wake:
			gate.mu.Lock()
			gate.waiters--
			gate.mu.Unlock()
		}
	}
}

// AdvanceConsumed publishes a monotone consumer frontier and wakes blocked
// producers. A consumer may advance beyond the latest observed producer
// frontier, which supports empty source batches and remains safe for future
// admissions.
func (gate *FrontierBackpressure) AdvanceConsumed(frontier uint64) error {
	if gate == nil {
		return ErrFrontierBackpressureNil
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.ensureInitializedLocked()
	if gate.closed {
		return ErrFrontierBackpressureClosed
	}
	if frontier < gate.consumed {
		return ErrFrontierBackpressureRegression
	}
	if frontier == gate.consumed {
		return nil
	}
	gate.consumed = frontier
	gate.signalLocked()
	return nil
}

// Close permanently stops admission and wakes every blocked Wait call.
func (gate *FrontierBackpressure) Close() {
	if gate == nil {
		return
	}
	gate.mu.Lock()
	gate.ensureInitializedLocked()
	if !gate.closed {
		gate.closed = true
		gate.signalLocked()
	}
	gate.mu.Unlock()
}

// Stats returns the current frontier state without retaining source values.
func (gate *FrontierBackpressure) Stats() FrontierBackpressureStats {
	if gate == nil {
		return FrontierBackpressureStats{}
	}
	gate.mu.Lock()
	gate.ensureInitializedLocked()
	lag := uint64(0)
	if gate.produced > gate.consumed {
		lag = gate.produced - gate.consumed
	}
	stats := FrontierBackpressureStats{
		MaxLag:          gate.maxLag,
		Produced:        gate.produced,
		Consumed:        gate.consumed,
		Lag:             lag,
		Admitted:        gate.admitted,
		BlockedAttempts: gate.blockedAttempts,
		Blocked:         gate.waiters > 0,
		Closed:          gate.closed,
	}
	gate.mu.Unlock()
	return stats
}

func (gate *FrontierBackpressure) ensureInitializedLocked() {
	if gate.maxLag == 0 {
		gate.maxLag = DefaultFrontierBackpressureMaxLag
	}
	if gate.wake == nil {
		gate.wake = make(chan struct{})
	}
}

func (gate *FrontierBackpressure) admitLocked(frontier uint64) {
	if frontier > gate.produced {
		gate.produced = frontier
	}
	gate.admitted++
}

func (gate *FrontierBackpressure) signalLocked() {
	if gate.waiters == 0 {
		return
	}
	close(gate.wake)
	gate.wake = make(chan struct{})
}

func frontierBackpressureLimit(consumed, maxLag uint64) uint64 {
	if ^uint64(0)-consumed < maxLag {
		return ^uint64(0)
	}
	return consumed + maxLag
}
