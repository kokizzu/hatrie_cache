package hatPipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// HydrationState identifies the lifecycle phase of a maintained view.
type HydrationState string

const (
	// HydrationStateCold means no hydration run is active and the view is not
	// admitted as current.
	HydrationStateCold HydrationState = "cold"
	// HydrationStateHydrating means a bounded hydration run is in progress.
	HydrationStateHydrating HydrationState = "hydrating"
	// HydrationStateReady means the view completed its current hydration run.
	HydrationStateReady HydrationState = "ready"
	// HydrationStateFailed means the last hydration run failed and must be
	// retried or reset by the owner.
	HydrationStateFailed HydrationState = "failed"
)

var (
	// ErrHydrationInvalid reports a nil or otherwise invalid state machine.
	ErrHydrationInvalid = errors.New("hatPipeline: hydration state machine is invalid")
	// ErrHydrationAlreadyRunning reports a transition that is not valid while a
	// hydration run is active.
	ErrHydrationAlreadyRunning = errors.New("hatPipeline: hydration is already running")
	// ErrHydrationNotRunning reports progress or terminal transitions without an
	// active hydration run.
	ErrHydrationNotRunning = errors.New("hatPipeline: hydration is not running")
	// ErrHydrationIncomplete reports an attempted completion before all work was
	// accounted for.
	ErrHydrationIncomplete = errors.New("hatPipeline: hydration is incomplete")
	// ErrHydrationProgressExceeded reports progress beyond the declared total.
	ErrHydrationProgressExceeded = errors.New("hatPipeline: hydration progress exceeds total")
	// ErrHydrationFailureNil reports a failure transition without a cause.
	ErrHydrationFailureNil = errors.New("hatPipeline: hydration failure must have a cause")
	// ErrHydrationFailed reports that a wait observed a failed hydration run.
	ErrHydrationFailed = errors.New("hatPipeline: hydration failed")
	// ErrHydrationContextNil reports a nil context passed to Wait.
	ErrHydrationContextNil = errors.New("hatPipeline: hydration wait context is nil")
	// ErrHydrationRateInvalid reports a negative, NaN, or infinite work rate.
	ErrHydrationRateInvalid = errors.New("hatPipeline: hydration rate is invalid")
)

// HydrationSnapshot is a detached view of one state-machine read. Failure is
// the original caller-owned error when State is HydrationStateFailed.
type HydrationSnapshot struct {
	State      HydrationState `json:"state"`
	Generation uint64         `json:"generation"`
	Completed  uint64         `json:"completed"`
	Total      uint64         `json:"total"`
	Remaining  uint64         `json:"remaining"`
	Failure    error          `json:"-"`
}

// HydrationStateMachine coordinates one view's cold, hydrating, ready, and
// failed lifecycle. It is safe for concurrent status readers, one hydration
// owner, and waiters. Progress updates do not allocate or wake waiters; only
// lifecycle transitions publish notifications.
type HydrationStateMachine struct {
	mu         sync.Mutex
	notify     chan struct{}
	state      HydrationState
	generation uint64
	completed  uint64
	total      uint64
	rate       float64
	failure    error
}

// NewHydrationStateMachine returns a cold hydration state machine.
func NewHydrationStateMachine() *HydrationStateMachine {
	return &HydrationStateMachine{
		notify: make(chan struct{}),
		state:  HydrationStateCold,
	}
}

// Snapshot returns the current detached lifecycle and progress state.
func (machine *HydrationStateMachine) Snapshot() HydrationSnapshot {
	if machine == nil {
		return HydrationSnapshot{}
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	return machine.snapshotLocked()
}

// Begin starts a new hydration generation with total units of work. A prior
// ready or failed generation may be retried. A zero-sized run becomes ready
// immediately.
func (machine *HydrationStateMachine) Begin(total uint64) error {
	if machine == nil {
		return ErrHydrationInvalid
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if machine.state == HydrationStateHydrating {
		return ErrHydrationAlreadyRunning
	}
	machine.generation++
	if machine.generation == 0 {
		machine.generation = 1
	}
	machine.completed = 0
	machine.total = total
	machine.rate = 0
	machine.failure = nil
	if total == 0 {
		machine.state = HydrationStateReady
	} else {
		machine.state = HydrationStateHydrating
	}
	machine.signalLocked()
	return nil
}

// Advance adds completed units to the active hydration run. It rejects
// overflow and never permits a snapshot to report progress beyond Total.
func (machine *HydrationStateMachine) Advance(units uint64) error {
	if machine == nil {
		return ErrHydrationInvalid
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if machine.state != HydrationStateHydrating {
		return ErrHydrationNotRunning
	}
	if units > machine.total-machine.completed {
		return ErrHydrationProgressExceeded
	}
	machine.completed += units
	return nil
}

// Complete publishes the active hydration generation as ready once all
// declared work has been completed.
func (machine *HydrationStateMachine) Complete() error {
	if machine == nil {
		return ErrHydrationInvalid
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if machine.state != HydrationStateHydrating {
		return ErrHydrationNotRunning
	}
	if machine.completed != machine.total {
		return ErrHydrationIncomplete
	}
	machine.state = HydrationStateReady
	machine.signalLocked()
	return nil
}

// Fail marks the active hydration generation failed and retains cause for
// detached status readers and Wait callers.
func (machine *HydrationStateMachine) Fail(cause error) error {
	if machine == nil {
		return ErrHydrationInvalid
	}
	if cause == nil {
		return ErrHydrationFailureNil
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if machine.state != HydrationStateHydrating {
		return ErrHydrationNotRunning
	}
	machine.failure = cause
	machine.state = HydrationStateFailed
	machine.signalLocked()
	return nil
}

// Reset returns a completed or failed generation to cold. An active hydration
// must be failed or completed explicitly so that its owner cannot silently
// discard accounting.
func (machine *HydrationStateMachine) Reset() error {
	if machine == nil {
		return ErrHydrationInvalid
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if machine.state == HydrationStateHydrating {
		return ErrHydrationAlreadyRunning
	}
	machine.state = HydrationStateCold
	machine.completed = 0
	machine.total = 0
	machine.rate = 0
	machine.failure = nil
	machine.signalLocked()
	return nil
}

// Wait blocks until the view is ready, the active run fails, or ctx is
// canceled. It never starts or advances hydration itself.
func (machine *HydrationStateMachine) Wait(ctx context.Context) (HydrationSnapshot, error) {
	if machine == nil {
		return HydrationSnapshot{}, ErrHydrationInvalid
	}
	if ctx == nil {
		return HydrationSnapshot{}, ErrHydrationContextNil
	}
	for {
		if err := ctx.Err(); err != nil {
			return machine.Snapshot(), err
		}
		machine.mu.Lock()
		snapshot := machine.snapshotLocked()
		switch snapshot.State {
		case HydrationStateReady:
			machine.mu.Unlock()
			return snapshot, nil
		case HydrationStateFailed:
			cause := snapshot.Failure
			machine.mu.Unlock()
			return snapshot, hydrationFailedError{cause: cause}
		default:
			notify := machine.notify
			machine.mu.Unlock()
			select {
			case <-ctx.Done():
				return machine.Snapshot(), ctx.Err()
			case <-notify:
			}
		}
	}
}

type hydrationFailedError struct {
	cause error
}

func (err hydrationFailedError) Error() string {
	return fmt.Sprintf("%s: %v", ErrHydrationFailed, err.cause)
}

func (err hydrationFailedError) Is(target error) bool {
	return target == ErrHydrationFailed || errors.Is(err.cause, target)
}

func (err hydrationFailedError) Unwrap() error {
	return err.cause
}

func (machine *HydrationStateMachine) snapshotLocked() HydrationSnapshot {
	snapshot := HydrationSnapshot{
		State:      machine.state,
		Generation: machine.generation,
		Completed:  machine.completed,
		Total:      machine.total,
		Failure:    machine.failure,
	}
	if machine.total > machine.completed {
		snapshot.Remaining = machine.total - machine.completed
	}
	return snapshot
}

func (machine *HydrationStateMachine) signalLocked() {
	if machine.notify == nil {
		machine.notify = make(chan struct{})
	}
	close(machine.notify)
	machine.notify = make(chan struct{})
}
