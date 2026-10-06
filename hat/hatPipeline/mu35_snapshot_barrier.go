package hatPipeline

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"unicode/utf8"
)

var (
	ErrSnapshotBarrierNil                 = errors.New("hatPipeline: snapshot barrier is nil")
	ErrSnapshotBarrierOptions             = errors.New("hatPipeline: snapshot barrier options are invalid")
	ErrSnapshotBarrierContextRequired     = errors.New("hatPipeline: snapshot barrier context is required")
	ErrSnapshotBarrierUnknownPrerequisite = errors.New("hatPipeline: snapshot barrier prerequisite is unknown")
	ErrSnapshotBarrierFailureRequired     = errors.New("hatPipeline: snapshot barrier failure is required")
	ErrSnapshotBarrierFailed              = errors.New("hatPipeline: snapshot barrier has failed")
	ErrSnapshotBarrierClosed              = errors.New("hatPipeline: snapshot barrier is closed")
	ErrSnapshotBarrierEpochOverflow       = errors.New("hatPipeline: snapshot barrier epoch overflow")
)

const (
	DefaultSnapshotBarrierMaxPrerequisites = 256
	MaxSnapshotBarrierPrerequisites        = 4096
	DefaultSnapshotBarrierMaxNameBytes     = 128
	MaxSnapshotBarrierNameBytes            = 1024
)

// SnapshotBarrierOptions describes the fixed set of snapshots or maintained
// objects that must be ready before dependent work may proceed.
type SnapshotBarrierOptions struct {
	Prerequisites    []string
	MaxPrerequisites int
	MaxNameBytes     int
}

// SnapshotBarrierState is the current readiness state of a barrier.
type SnapshotBarrierState uint8

const (
	SnapshotBarrierPending SnapshotBarrierState = iota
	SnapshotBarrierReady
	SnapshotBarrierFailed
	SnapshotBarrierClosed
)

func (state SnapshotBarrierState) String() string {
	switch state {
	case SnapshotBarrierPending:
		return "pending"
	case SnapshotBarrierReady:
		return "ready"
	case SnapshotBarrierFailed:
		return "failed"
	case SnapshotBarrierClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// SnapshotBarrierStatus is a deterministic, detached operator snapshot.
type SnapshotBarrierStatus struct {
	Epoch              uint64
	State              SnapshotBarrierState
	Prerequisites      []string
	Ready              []string
	Pending            []string
	FailedPrerequisite string
	Failure            string
}

// SnapshotBarrier coordinates readiness of a fixed bounded set of named
// prerequisites. It is opt-in and has no effect on callers that do not use it.
type SnapshotBarrier struct {
	mu         sync.Mutex
	names      []string
	required   map[string]struct{}
	ready      map[string]struct{}
	failedName string
	failure    error
	epoch      uint64
	closed     bool
	notify     chan struct{}
}

// NewSnapshotBarrier creates a barrier with a fixed prerequisite set. An
// empty set is ready immediately, which is useful for optional snapshots.
func NewSnapshotBarrier(options SnapshotBarrierOptions) (*SnapshotBarrier, error) {
	maxPrerequisites := options.MaxPrerequisites
	if maxPrerequisites == 0 {
		maxPrerequisites = DefaultSnapshotBarrierMaxPrerequisites
	}
	maxNameBytes := options.MaxNameBytes
	if maxNameBytes == 0 {
		maxNameBytes = DefaultSnapshotBarrierMaxNameBytes
	}
	if maxPrerequisites < 1 || maxPrerequisites > MaxSnapshotBarrierPrerequisites ||
		maxNameBytes < 1 || maxNameBytes > MaxSnapshotBarrierNameBytes ||
		len(options.Prerequisites) > maxPrerequisites {
		return nil, ErrSnapshotBarrierOptions
	}
	names := append([]string(nil), options.Prerequisites...)
	sort.Strings(names)
	required := make(map[string]struct{}, len(names))
	for _, name := range names {
		if len(name) == 0 || len(name) > maxNameBytes || !utf8.ValidString(name) {
			return nil, ErrSnapshotBarrierOptions
		}
		if _, exists := required[name]; exists {
			return nil, ErrSnapshotBarrierOptions
		}
		required[name] = struct{}{}
	}
	return &SnapshotBarrier{
		names:    names,
		required: required,
		ready:    make(map[string]struct{}, len(names)),
		epoch:    1,
		notify:   make(chan struct{}),
	}, nil
}

// Wait blocks until all prerequisites are ready, a prerequisite fails, the
// barrier closes, or ctx is canceled.
func (barrier *SnapshotBarrier) Wait(ctx context.Context) error {
	if barrier == nil {
		return ErrSnapshotBarrierNil
	}
	if ctx == nil {
		return ErrSnapshotBarrierContextRequired
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		barrier.mu.Lock()
		if barrier.closed {
			barrier.mu.Unlock()
			return ErrSnapshotBarrierClosed
		}
		if barrier.failure != nil {
			err := barrier.failureErrorLocked()
			barrier.mu.Unlock()
			return err
		}
		if barrier.readyLocked() {
			barrier.mu.Unlock()
			return nil
		}
		notify := barrier.notify
		barrier.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

// MarkReady marks one prerequisite complete. Repeating the same call is
// idempotent. A failed barrier must be reset before a new readiness epoch.
func (barrier *SnapshotBarrier) MarkReady(name string) error {
	if barrier == nil {
		return ErrSnapshotBarrierNil
	}
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	if barrier.closed {
		return ErrSnapshotBarrierClosed
	}
	if barrier.failure != nil {
		return barrier.failureErrorLocked()
	}
	if _, required := barrier.required[name]; !required {
		return fmt.Errorf("%w: %q", ErrSnapshotBarrierUnknownPrerequisite, name)
	}
	if _, alreadyReady := barrier.ready[name]; alreadyReady {
		return nil
	}
	barrier.ready[name] = struct{}{}
	barrier.signalLocked()
	return nil
}

// MarkFailed records a prerequisite failure and wakes all waiters. The
// original error is exposed only through the returned wrapped error and status
// text; callers should avoid putting secrets in it.
func (barrier *SnapshotBarrier) MarkFailed(name string, failure error) error {
	if barrier == nil {
		return ErrSnapshotBarrierNil
	}
	if failure == nil {
		return ErrSnapshotBarrierFailureRequired
	}
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	if barrier.closed {
		return ErrSnapshotBarrierClosed
	}
	if _, required := barrier.required[name]; !required {
		return fmt.Errorf("%w: %q", ErrSnapshotBarrierUnknownPrerequisite, name)
	}
	if barrier.failure != nil {
		return barrier.failureErrorLocked()
	}
	barrier.failedName = name
	barrier.failure = failure
	barrier.signalLocked()
	return nil
}

// Reset starts a new readiness epoch and clears all prerequisite outcomes.
func (barrier *SnapshotBarrier) Reset() error {
	if barrier == nil {
		return ErrSnapshotBarrierNil
	}
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	if barrier.closed {
		return ErrSnapshotBarrierClosed
	}
	if barrier.epoch == ^uint64(0) {
		return ErrSnapshotBarrierEpochOverflow
	}
	barrier.epoch++
	barrier.ready = make(map[string]struct{}, len(barrier.names))
	barrier.failedName = ""
	barrier.failure = nil
	barrier.signalLocked()
	return nil
}

// Ready reports whether all prerequisites are ready in the current epoch.
func (barrier *SnapshotBarrier) Ready() bool {
	if barrier == nil {
		return false
	}
	barrier.mu.Lock()
	ready := !barrier.closed && barrier.failure == nil && barrier.readyLocked()
	barrier.mu.Unlock()
	return ready
}

// Status returns a deterministic copy of the barrier state.
func (barrier *SnapshotBarrier) Status() SnapshotBarrierStatus {
	if barrier == nil {
		return SnapshotBarrierStatus{}
	}
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	status := SnapshotBarrierStatus{
		Epoch:              barrier.epoch,
		Prerequisites:      append([]string(nil), barrier.names...),
		FailedPrerequisite: barrier.failedName,
	}
	switch {
	case barrier.closed:
		status.State = SnapshotBarrierClosed
	case barrier.failure != nil:
		status.State = SnapshotBarrierFailed
		status.Failure = barrier.failure.Error()
	case barrier.readyLocked():
		status.State = SnapshotBarrierReady
	default:
		status.State = SnapshotBarrierPending
	}
	for _, name := range barrier.names {
		if _, ready := barrier.ready[name]; ready {
			status.Ready = append(status.Ready, name)
		} else {
			status.Pending = append(status.Pending, name)
		}
	}
	return status
}

// Close permanently stops the barrier and wakes all current waiters.
func (barrier *SnapshotBarrier) Close() error {
	if barrier == nil {
		return ErrSnapshotBarrierNil
	}
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	if barrier.closed {
		return nil
	}
	barrier.closed = true
	barrier.signalLocked()
	return nil
}

func (barrier *SnapshotBarrier) readyLocked() bool {
	return len(barrier.ready) == len(barrier.required)
}

func (barrier *SnapshotBarrier) signalLocked() {
	close(barrier.notify)
	barrier.notify = make(chan struct{})
}

func (barrier *SnapshotBarrier) failureErrorLocked() error {
	return fmt.Errorf("%w: %s: %v", ErrSnapshotBarrierFailed, barrier.failedName, barrier.failure)
}
