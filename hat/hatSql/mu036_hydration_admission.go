package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	// ErrTypedTableArrangementHydrationAdmissionNil reports a call on a nil
	// hydration admission controller.
	ErrTypedTableArrangementHydrationAdmissionNil = errors.New("hatSql: hydration admission controller is nil")
	// ErrTypedTableArrangementHydrationAdmissionClosed reports a controller
	// that has been closed and cannot accept more hydration progress.
	ErrTypedTableArrangementHydrationAdmissionClosed = errors.New("hatSql: hydration admission controller is closed")
	// ErrTypedTableArrangementHydrationAdmissionInvalid reports a progress
	// update that would move a checkpoint or target backwards.
	ErrTypedTableArrangementHydrationAdmissionInvalid = errors.New("hatSql: hydration admission progress is invalid")
	// ErrTypedTableArrangementHydrationAdmissionFailed reports a controller
	// that needs Reset before another hydration attempt can start.
	ErrTypedTableArrangementHydrationAdmissionFailed = errors.New("hatSql: arrangement hydration failed")
)

// TypedTableArrangementHydrationState is the lifecycle state visible to
// monitoring and query-admission callers.
type TypedTableArrangementHydrationState string

const (
	maxTypedTableArrangementHydrationErrorBytes = 1024

	// TypedTableArrangementHydrationPending means a failed generation was
	// reset and has not been started again.
	TypedTableArrangementHydrationPending TypedTableArrangementHydrationState = "pending"
	// TypedTableArrangementHydrationHydrating means readers should wait for
	// the current generation to reach its target checkpoint.
	TypedTableArrangementHydrationHydrating TypedTableArrangementHydrationState = "hydrating"
	// TypedTableArrangementHydrationReady means the arrangement has reached its
	// latest announced source target.
	TypedTableArrangementHydrationReady TypedTableArrangementHydrationState = "ready"
	// TypedTableArrangementHydrationFailed means the generation cannot admit
	// readers until the caller resets and retries it.
	TypedTableArrangementHydrationFailed TypedTableArrangementHydrationState = "failed"
	// TypedTableArrangementHydrationClosed means no further progress is valid.
	TypedTableArrangementHydrationClosed TypedTableArrangementHydrationState = "closed"
)

// TypedTableArrangementHydrationProgress is a point-in-time, JSON-friendly
// progress snapshot. Applied is cumulative for the current generation;
// Pending is Target minus Checkpoint with underflow clamped to zero.
type TypedTableArrangementHydrationProgress struct {
	State      TypedTableArrangementHydrationState `json:"state"`
	Generation uint64                              `json:"generation"`
	Checkpoint uint64                              `json:"checkpoint"`
	Target     uint64                              `json:"target"`
	Applied    uint64                              `json:"applied"`
	Pending    uint64                              `json:"pending"`
	Error      string                              `json:"error,omitempty"`
}

// TypedTableArrangementHydrationAdmission is an opt-in readiness gate for
// arrangement consumers. It has no goroutine and adds no cost to the direct
// Freshness, Hydrate, Rows, or query paths unless a caller supplies it.
//
// A hydrator calls Start once it observes a source target, calls Advance after
// each bounded replay, and calls Fail on an unrecoverable replay error. Query
// handlers call WaitReady before reading the arrangement. The same controller
// can be shared by all readers of one arrangement generation.
type TypedTableArrangementHydrationAdmission struct {
	mu         sync.Mutex
	notify     chan struct{}
	state      TypedTableArrangementHydrationState
	generation atomic.Uint64
	checkpoint atomic.Uint64
	target     atomic.Uint64
	applied    atomic.Uint64
	ready      atomic.Uint32
	failure    error
}

// NewTypedTableArrangementHydrationAdmission returns an initially ready,
// opt-in controller. Callers only need Start when they observe a stale target.
func NewTypedTableArrangementHydrationAdmission() *TypedTableArrangementHydrationAdmission {
	admission := &TypedTableArrangementHydrationAdmission{
		notify: make(chan struct{}),
		state:  TypedTableArrangementHydrationReady,
	}
	admission.ready.Store(1)
	return admission
}

// Progress returns a detached point-in-time snapshot. A nil receiver returns
// the zero value so optional monitoring hooks can remain nil-safe.
func (admission *TypedTableArrangementHydrationAdmission) Progress() TypedTableArrangementHydrationProgress {
	if admission == nil {
		return TypedTableArrangementHydrationProgress{}
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	return admission.progressLocked()
}

// Start announces the source checkpoint required by a new or existing
// hydration generation. Repeated calls for the same generation are cheap;
// a higher target extends the current generation without resetting progress.
func (admission *TypedTableArrangementHydrationAdmission) Start(target uint64) error {
	if admission == nil {
		return ErrTypedTableArrangementHydrationAdmissionNil
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if admission.state == TypedTableArrangementHydrationClosed {
		return ErrTypedTableArrangementHydrationAdmissionClosed
	}
	if admission.state == TypedTableArrangementHydrationFailed {
		return ErrTypedTableArrangementHydrationAdmissionFailed
	}
	if target <= admission.checkpoint.Load() {
		wasReady := admission.state == TypedTableArrangementHydrationReady
		if target > admission.target.Load() {
			admission.target.Store(target)
		}
		admission.state = TypedTableArrangementHydrationReady
		admission.ready.Store(1)
		if !wasReady {
			admission.signalLocked()
		}
		return nil
	}
	if admission.state != TypedTableArrangementHydrationHydrating {
		admission.ready.Store(0)
		admission.generation.Add(1)
		admission.applied.Store(0)
		admission.state = TypedTableArrangementHydrationHydrating
	}
	if target > admission.target.Load() {
		admission.target.Store(target)
	}
	return nil
}

// Advance publishes one bounded replay result. checkpoint and target are
// absolute sequence values; applied is the number of changes in this batch.
// Source sequence numbers are monotone, so regressions are rejected rather
// than silently reopening a ready generation.
func (admission *TypedTableArrangementHydrationAdmission) Advance(checkpoint, target, applied uint64) error {
	if admission == nil {
		return ErrTypedTableArrangementHydrationAdmissionNil
	}
	if checkpoint > target {
		return fmt.Errorf("%w: checkpoint %d exceeds target %d", ErrTypedTableArrangementHydrationAdmissionInvalid, checkpoint, target)
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if admission.state == TypedTableArrangementHydrationClosed {
		return ErrTypedTableArrangementHydrationAdmissionClosed
	}
	if admission.state == TypedTableArrangementHydrationFailed {
		return ErrTypedTableArrangementHydrationAdmissionFailed
	}
	if admission.state == TypedTableArrangementHydrationReady {
		if checkpoint == target && checkpoint >= admission.checkpoint.Load() {
			admission.checkpoint.Store(checkpoint)
			if target > admission.target.Load() {
				admission.target.Store(target)
			}
			return nil
		}
		return fmt.Errorf("%w: progress was not started", ErrTypedTableArrangementHydrationAdmissionInvalid)
	}
	if admission.state == TypedTableArrangementHydrationPending {
		return fmt.Errorf("%w: progress was not started", ErrTypedTableArrangementHydrationAdmissionInvalid)
	}
	if checkpoint < admission.checkpoint.Load() || target < admission.target.Load() {
		return fmt.Errorf("%w: checkpoint or target moved backwards", ErrTypedTableArrangementHydrationAdmissionInvalid)
	}
	admission.checkpoint.Store(checkpoint)
	admission.target.Store(target)
	admission.applied.Add(applied)
	if checkpoint >= target {
		admission.state = TypedTableArrangementHydrationReady
		admission.ready.Store(1)
		admission.signalLocked()
	}
	return nil
}

// Fail marks the current generation failed and wakes all waiting readers.
// A nil error is replaced with the package sentinel. Reset is required before
// another generation can start, preventing accidental admission of stale rows.
func (admission *TypedTableArrangementHydrationAdmission) Fail(err error) {
	if admission == nil {
		return
	}
	if err == nil {
		err = ErrTypedTableArrangementHydrationAdmissionFailed
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if admission.state == TypedTableArrangementHydrationClosed {
		return
	}
	admission.ready.Store(0)
	admission.failure = err
	admission.state = TypedTableArrangementHydrationFailed
	admission.signalLocked()
}

// Reset clears a failed generation while retaining its last known checkpoint.
// The controller becomes pending, so readers remain blocked until a hydrator
// explicitly starts a new target.
func (admission *TypedTableArrangementHydrationAdmission) Reset() error {
	if admission == nil {
		return ErrTypedTableArrangementHydrationAdmissionNil
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if admission.state == TypedTableArrangementHydrationClosed {
		return ErrTypedTableArrangementHydrationAdmissionClosed
	}
	if admission.state != TypedTableArrangementHydrationFailed {
		return nil
	}
	admission.failure = nil
	admission.ready.Store(0)
	admission.target.Store(admission.checkpoint.Load())
	admission.applied.Store(0)
	admission.state = TypedTableArrangementHydrationPending
	admission.signalLocked()
	return nil
}

// WaitReady blocks until the current generation is ready, the context is
// canceled, or hydration fails/closes. The returned snapshot is detached.
func (admission *TypedTableArrangementHydrationAdmission) WaitReady(ctx context.Context) (TypedTableArrangementHydrationProgress, error) {
	if admission == nil {
		return TypedTableArrangementHydrationProgress{}, ErrTypedTableArrangementHydrationAdmissionNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if admission.ready.Load() == 1 {
		return admission.readyProgress(), nil
	}
	for {
		admission.mu.Lock()
		switch admission.state {
		case TypedTableArrangementHydrationReady:
			progress := admission.progressLocked()
			admission.mu.Unlock()
			return progress, nil
		case TypedTableArrangementHydrationFailed:
			err := admission.failure
			if err == nil {
				err = ErrTypedTableArrangementHydrationAdmissionFailed
			}
			admission.mu.Unlock()
			return TypedTableArrangementHydrationProgress{}, err
		case TypedTableArrangementHydrationClosed:
			admission.mu.Unlock()
			return TypedTableArrangementHydrationProgress{}, ErrTypedTableArrangementHydrationAdmissionClosed
		default:
			wait := admission.notify
			admission.mu.Unlock()
			select {
			case <-ctx.Done():
				return TypedTableArrangementHydrationProgress{}, ctx.Err()
			case <-wait:
			}
		}
	}
}

// Close permanently stops admission and wakes every waiter.
func (admission *TypedTableArrangementHydrationAdmission) Close() {
	if admission == nil {
		return
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if admission.state == TypedTableArrangementHydrationClosed {
		return
	}
	admission.ready.Store(0)
	admission.state = TypedTableArrangementHydrationClosed
	admission.failure = nil
	admission.signalLocked()
}

// HydrateWithAdmission performs one bounded aggregate replay and publishes its
// progress. A nil admission preserves the direct Hydrate path exactly.
func (arrangement *TypedTableAggregateArrangement) HydrateWithAdmission(ctx context.Context, admission *TypedTableArrangementHydrationAdmission, limit int) (TypedTableAggregateArrangementHydration, error) {
	if admission == nil {
		return arrangement.Hydrate(limit)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return TypedTableAggregateArrangementHydration{}, err
	}
	freshness, err := arrangement.Freshness()
	if err != nil {
		admission.Fail(err)
		return TypedTableAggregateArrangementHydration{}, err
	}
	if err := admission.Start(freshness.SourceSequence); err != nil {
		return TypedTableAggregateArrangementHydration{}, err
	}
	report, err := arrangement.Hydrate(limit)
	if err != nil {
		admission.Fail(err)
		return TypedTableAggregateArrangementHydration{}, err
	}
	if err := admission.Advance(report.After, report.SourceSequence, uint64(report.Applied)); err != nil {
		admission.Fail(err)
		return TypedTableAggregateArrangementHydration{}, err
	}
	return report, nil
}

// HydrateWithAdmission performs one bounded two-input join replay and
// publishes summed progress units for the two independent source streams.
// A nil admission preserves the direct Hydrate path exactly.
func (arrangement *TypedTableJoinArrangement) HydrateWithAdmission(ctx context.Context, admission *TypedTableArrangementHydrationAdmission, limit int) (TypedTableJoinArrangementHydration, error) {
	if admission == nil {
		return arrangement.Hydrate(limit)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return TypedTableJoinArrangementHydration{}, err
	}
	freshness, err := arrangement.Freshness()
	if err != nil {
		admission.Fail(err)
		return TypedTableJoinArrangementHydration{}, err
	}
	if err := admission.Start(typedTableArrangementHydrationSum(freshness.LeftSourceSequence, freshness.RightSourceSequence)); err != nil {
		return TypedTableJoinArrangementHydration{}, err
	}
	report, err := arrangement.Hydrate(limit)
	if err != nil {
		admission.Fail(err)
		return TypedTableJoinArrangementHydration{}, err
	}
	checkpoint := typedTableArrangementHydrationSum(report.LeftAfter, report.RightAfter)
	target := typedTableArrangementHydrationSum(report.LeftSourceSequence, report.RightSourceSequence)
	applied := uint64(report.LeftApplied) + uint64(report.RightApplied)
	if err := admission.Advance(checkpoint, target, applied); err != nil {
		admission.Fail(err)
		return TypedTableJoinArrangementHydration{}, err
	}
	return report, nil
}

func (admission *TypedTableArrangementHydrationAdmission) progressLocked() TypedTableArrangementHydrationProgress {
	checkpoint := admission.checkpoint.Load()
	target := admission.target.Load()
	pending := uint64(0)
	if target > checkpoint {
		pending = target - checkpoint
	}
	progress := TypedTableArrangementHydrationProgress{
		State:      admission.state,
		Generation: admission.generation.Load(),
		Checkpoint: checkpoint,
		Target:     target,
		Applied:    admission.applied.Load(),
		Pending:    pending,
	}
	if admission.failure != nil {
		progress.Error = typedTableArrangementHydrationErrorText(admission.failure)
	}
	return progress
}

func typedTableArrangementHydrationErrorText(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if len(text) <= maxTypedTableArrangementHydrationErrorBytes {
		return text
	}
	return text[:maxTypedTableArrangementHydrationErrorBytes]
}

func (admission *TypedTableArrangementHydrationAdmission) readyProgress() TypedTableArrangementHydrationProgress {
	checkpoint := admission.checkpoint.Load()
	target := admission.target.Load()
	pending := uint64(0)
	if target > checkpoint {
		pending = target - checkpoint
	}
	return TypedTableArrangementHydrationProgress{
		State:      TypedTableArrangementHydrationReady,
		Generation: admission.generation.Load(),
		Checkpoint: checkpoint,
		Target:     target,
		Applied:    admission.applied.Load(),
		Pending:    pending,
	}
}

func (admission *TypedTableArrangementHydrationAdmission) signalLocked() {
	close(admission.notify)
	admission.notify = make(chan struct{})
}

func typedTableArrangementHydrationSum(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
