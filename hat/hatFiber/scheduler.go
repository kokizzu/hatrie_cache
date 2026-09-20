// Package hatFiber provides an opt-in, single-owner cooperative task
// scheduler. Tasks are stackless continuations: a StepFunc returns after each
// small unit of work and can request another round without allocating a
// goroutine or retaining a private goroutine stack.
package hatFiber

import (
	"context"
	"errors"
)

const (
	// DefaultMaxFibers bounds the zero-value scheduler configuration.
	DefaultMaxFibers = 1024
	// MaxFibers prevents configuration from reserving unbounded scheduler
	// storage from untrusted input.
	MaxFibers = 1 << 20
)

var (
	ErrNilStep          = errors.New("hatFiber: step function is nil")
	ErrMaxFibersInvalid = errors.New("hatFiber: max fibers is invalid")
	ErrFiberCapacity    = errors.New("hatFiber: fiber capacity exhausted")
	ErrSchedulerClosed  = errors.New("hatFiber: scheduler is closed")
	ErrNilContext       = errors.New("hatFiber: context is nil")
	ErrMaxStepsInvalid  = errors.New("hatFiber: max steps is invalid")
	ErrInvalidStep      = errors.New("hatFiber: step function returned an invalid step")
	ErrFiberNotFound    = errors.New("hatFiber: fiber not found")
	ErrFiberFinished    = errors.New("hatFiber: fiber has already finished")
	ErrFiberNotFinished = errors.New("hatFiber: fiber is not finished")
)

// FiberID identifies one scheduler slot and generation. A generation makes a
// stale ID fail after its slot is reaped and reused.
type FiberID uint64

// Step is the continuation action returned by a StepFunc.
type Step uint8

const (
	// StepYield puts the fiber at the end of the ready queue.
	StepYield Step = iota + 1
	// StepDone marks the fiber complete and makes it eligible for Reap.
	StepDone
)

// StepFunc performs one bounded unit of work. It must return promptly; the
// scheduler cannot preempt a running callback. Returning StepYield provides
// cooperative fairness, while returning StepDone releases the callback when
// the caller reaps the fiber.
type StepFunc func(context.Context) (Step, error)

// Status describes a fiber's lifecycle state.
type Status uint8

const (
	StatusReady Status = iota + 1
	StatusRunning
	StatusDone
	StatusFailed
	StatusCancelled
)

func (status Status) String() string {
	switch status {
	case StatusReady:
		return "ready"
	case StatusRunning:
		return "running"
	case StatusDone:
		return "done"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// Options configures a Scheduler. A Scheduler is intentionally single-owner:
// the goroutine that calls Run owns Spawn, Cancel, Status, Error, and Reap.
// Callbacks may share external state, but the scheduler itself does not add a
// mutex or per-step synchronization cost.
type Options struct {
	// MaxFibers is the fixed number of slots and ready-queue entries. Zero uses
	// DefaultMaxFibers.
	MaxFibers int
}

// RunStats reports work performed by one Run call.
type RunStats struct {
	Steps     uint64
	Yielded   uint64
	Completed uint64
	Failed    uint64
	Cancelled uint64
	Remaining int
}

type fiberSlot struct {
	function   StepFunc
	identifier FiberID
	generation uint32
	status     Status
	err        error
	queued     bool
}

// Scheduler is a bounded round-robin cooperative scheduler. It does not start
// goroutines; the caller chooses when and on which goroutine Run executes.
type Scheduler struct {
	slots      []fiberSlot
	free       []uint32
	freeCount  int
	ready      []FiberID
	readyHead  int
	readyTail  int
	readyCount int
	closed     bool
}

// New creates a bounded scheduler with preallocated slot and queue storage.
func New(options Options) (*Scheduler, error) {
	maxFibers := options.MaxFibers
	if maxFibers == 0 {
		maxFibers = DefaultMaxFibers
	}
	if maxFibers < 1 || maxFibers > MaxFibers {
		return nil, ErrMaxFibersInvalid
	}

	scheduler := &Scheduler{
		slots: make([]fiberSlot, maxFibers),
		free:  make([]uint32, maxFibers),
		ready: make([]FiberID, maxFibers),
	}
	for index := maxFibers - 1; index >= 0; index-- {
		scheduler.free[scheduler.freeCount] = uint32(index)
		scheduler.freeCount++
	}
	return scheduler, nil
}

// Spawn adds a ready fiber. The callback is retained until the fiber is
// complete or cancelled and then until Reap releases its slot.
func (scheduler *Scheduler) Spawn(function StepFunc) (FiberID, error) {
	if scheduler == nil {
		return 0, ErrSchedulerClosed
	}
	if function == nil {
		return 0, ErrNilStep
	}
	if scheduler.closed {
		return 0, ErrSchedulerClosed
	}
	if scheduler.freeCount == 0 {
		return 0, ErrFiberCapacity
	}

	index := scheduler.free[scheduler.freeCount-1]
	scheduler.freeCount--
	slot := &scheduler.slots[index]
	slot.generation++
	if slot.generation == 0 {
		slot.generation = 1
	}
	slot.identifier = makeFiberID(slot.generation, index)
	slot.function = function
	slot.status = StatusReady
	slot.err = nil
	slot.queued = false
	scheduler.enqueue(slot)
	return slot.identifier, nil
}

// Run executes ready fibers in round-robin order. A maxSteps value of zero
// drains the ready queue; a positive value returns after that many callbacks.
// Context cancellation leaves unexecuted and yielded fibers ready for a later
// Run call and is returned to the caller.
func (scheduler *Scheduler) Run(ctx context.Context, maxSteps int) (RunStats, error) {
	var stats RunStats
	if scheduler == nil {
		return stats, ErrSchedulerClosed
	}
	if ctx == nil {
		return stats, ErrNilContext
	}
	if maxSteps < 0 {
		return stats, ErrMaxStepsInvalid
	}

	for scheduler.readyCount > 0 && (maxSteps == 0 || stats.Steps < uint64(maxSteps)) {
		if err := ctx.Err(); err != nil {
			stats.Remaining = scheduler.readyCount
			return stats, err
		}
		identifier := scheduler.dequeue()
		slot, err := scheduler.lookup(identifier)
		if err != nil {
			continue
		}
		if slot.status == StatusCancelled {
			stats.Cancelled++
			continue
		}
		if slot.status != StatusReady {
			continue
		}

		slot.status = StatusRunning
		step, stepErr := slot.function(ctx)
		stats.Steps++
		switch {
		case stepErr != nil:
			slot.status = StatusFailed
			slot.err = stepErr
			slot.function = nil
			stats.Failed++
		case step == StepYield:
			slot.status = StatusReady
			scheduler.enqueue(slot)
			stats.Yielded++
		case step == StepDone:
			slot.status = StatusDone
			slot.function = nil
			stats.Completed++
		default:
			slot.status = StatusFailed
			slot.err = ErrInvalidStep
			slot.function = nil
			stats.Failed++
		}
	}
	stats.Remaining = scheduler.readyCount
	return stats, nil
}

// Cancel prevents a ready fiber from running again. Its queue entry is
// consumed by Run or removed by Reap, so cancellation never grows the queue.
func (scheduler *Scheduler) Cancel(identifier FiberID) error {
	slot, err := scheduler.lookup(identifier)
	if err != nil {
		return err
	}
	switch slot.status {
	case StatusReady:
		slot.status = StatusCancelled
		slot.err = context.Canceled
		slot.function = nil
		return nil
	case StatusRunning:
		return ErrFiberNotFinished
	default:
		return ErrFiberFinished
	}
}

// Status returns the current lifecycle state for an active or unreaped fiber.
func (scheduler *Scheduler) Status(identifier FiberID) (Status, error) {
	slot, err := scheduler.lookup(identifier)
	if err != nil {
		return 0, err
	}
	return slot.status, nil
}

// Failure returns the terminal callback error, or nil for a successful or
// unfinished fiber.
func (scheduler *Scheduler) Failure(identifier FiberID) error {
	slot, err := scheduler.lookup(identifier)
	if err != nil {
		return err
	}
	return slot.err
}

// Reap releases a terminal fiber's callback and slot. Reaping is required to
// reuse bounded capacity and also invalidates the old FiberID.
func (scheduler *Scheduler) Reap(identifier FiberID) error {
	slot, err := scheduler.lookup(identifier)
	if err != nil {
		return err
	}
	if slot.status != StatusDone && slot.status != StatusFailed && slot.status != StatusCancelled {
		return ErrFiberNotFinished
	}
	if slot.queued {
		if !scheduler.removeReady(identifier) {
			return ErrFiberNotFound
		}
	}
	index := fiberIndex(identifier)
	generation := slot.generation
	*slot = fiberSlot{generation: generation}
	scheduler.free[scheduler.freeCount] = index
	scheduler.freeCount++
	return nil
}

// Pending reports the number of queue entries waiting for Run. Cancelled
// entries remain pending until consumed or reaped.
func (scheduler *Scheduler) Pending() int {
	if scheduler == nil {
		return 0
	}
	return scheduler.readyCount
}

// Close prevents new fibers and marks all ready fibers cancelled. Call Run or
// Reap on the returned IDs to consume terminal state and release slots.
func (scheduler *Scheduler) Close() {
	if scheduler == nil || scheduler.closed {
		return
	}
	scheduler.closed = true
	for index := range scheduler.slots {
		slot := &scheduler.slots[index]
		if slot.status == StatusReady {
			slot.status = StatusCancelled
			slot.err = context.Canceled
			slot.function = nil
		}
	}
}

func makeFiberID(generation, index uint32) FiberID {
	return FiberID(generation)<<32 | FiberID(index+1)
}

func fiberIndex(identifier FiberID) uint32 {
	return uint32(identifier) - 1
}

func (scheduler *Scheduler) lookup(identifier FiberID) (*fiberSlot, error) {
	if scheduler == nil || identifier == 0 || uint32(identifier) == 0 {
		return nil, ErrFiberNotFound
	}
	index := fiberIndex(identifier)
	if uint64(index) >= uint64(len(scheduler.slots)) {
		return nil, ErrFiberNotFound
	}
	slot := &scheduler.slots[index]
	if slot.identifier != identifier {
		return nil, ErrFiberNotFound
	}
	return slot, nil
}

func (scheduler *Scheduler) enqueue(slot *fiberSlot) {
	scheduler.ready[scheduler.readyTail] = slot.identifier
	scheduler.readyTail++
	if scheduler.readyTail == len(scheduler.ready) {
		scheduler.readyTail = 0
	}
	scheduler.readyCount++
	slot.queued = true
}

func (scheduler *Scheduler) dequeue() FiberID {
	identifier := scheduler.ready[scheduler.readyHead]
	scheduler.ready[scheduler.readyHead] = 0
	scheduler.readyHead++
	if scheduler.readyHead == len(scheduler.ready) {
		scheduler.readyHead = 0
	}
	scheduler.readyCount--
	if slot, err := scheduler.lookup(identifier); err == nil {
		slot.queued = false
	}
	return identifier
}

func (scheduler *Scheduler) removeReady(identifier FiberID) bool {
	if scheduler.readyCount == 0 {
		return false
	}
	position := -1
	for offset := 0; offset < scheduler.readyCount; offset++ {
		index := scheduler.readyHead + offset
		if index >= len(scheduler.ready) {
			index -= len(scheduler.ready)
		}
		if scheduler.ready[index] == identifier {
			position = offset
			break
		}
	}
	if position < 0 {
		return false
	}
	for offset := position; offset < scheduler.readyCount-1; offset++ {
		from := scheduler.readyHead + offset + 1
		if from >= len(scheduler.ready) {
			from -= len(scheduler.ready)
		}
		to := scheduler.readyHead + offset
		if to >= len(scheduler.ready) {
			to -= len(scheduler.ready)
		}
		scheduler.ready[to] = scheduler.ready[from]
	}
	scheduler.readyTail--
	if scheduler.readyTail < 0 {
		scheduler.readyTail = len(scheduler.ready) - 1
	}
	scheduler.ready[scheduler.readyTail] = 0
	scheduler.readyCount--
	return true
}
