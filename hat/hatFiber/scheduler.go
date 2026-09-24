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
	ErrFiberNotRunning  = errors.New("hatFiber: no fiber is currently running")
	ErrFiberNotParked   = errors.New("hatFiber: StepWait returned without parking the fiber")
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
	// StepWait parks the current fiber until a synchronization primitive wakes
	// it. The callback must obtain this value from ParkCurrent or a Wait method.
	StepWait
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
	StatusWaiting
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
	case StatusWaiting:
		return "waiting"
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
	// TenantQuotas optionally bounds named fibers spawned through
	// SpawnForTenant. Unconfigured tenant names remain unlimited.
	TenantQuotas map[string]TenantQuota
}

// RunStats reports work performed by one Run call.
type RunStats struct {
	Steps     uint64
	Yielded   uint64
	Completed uint64
	Failed    uint64
	Cancelled uint64
	Waited    uint64
	Throttled uint64
	Remaining int
}

type fiberSlot struct {
	function   StepFunc
	identifier FiberID
	generation uint32
	status     Status
	err        error
	queued     bool
	tenant     string
}

// Scheduler is a bounded round-robin cooperative scheduler. It does not start
// goroutines; the caller chooses when and on which goroutine Run executes.
type Scheduler struct {
	slots         []fiberSlot
	free          []uint32
	freeCount     int
	ready         []FiberID
	readyHead     int
	readyTail     int
	readyCount    int
	locals        []fiberLocalStore
	current       FiberID
	closed        bool
	tenantQuotas  map[string]TenantQuota
	tenantFibers  map[string]int
	hasStepQuotas bool
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
	tenantQuotas, hasStepQuotas, err := cloneTenantQuotas(options.TenantQuotas)
	if err != nil {
		return nil, err
	}

	scheduler := &Scheduler{
		slots:         make([]fiberSlot, maxFibers),
		free:          make([]uint32, maxFibers),
		ready:         make([]FiberID, maxFibers),
		tenantQuotas:  tenantQuotas,
		hasStepQuotas: hasStepQuotas,
	}
	for tenant, quota := range tenantQuotas {
		if quota.MaxFibers > 0 {
			if scheduler.tenantFibers == nil {
				scheduler.tenantFibers = make(map[string]int)
			}
			scheduler.tenantFibers[tenant] = 0
		}
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
	slot.tenant = ""
	scheduler.enqueue(slot)
	return slot.identifier, nil
}

// SpawnForTenant adds a ready fiber attributed to tenant. A configured
// MaxFibers quota rejects the spawn until that tenant's terminal fibers are
// reaped. An unconfigured tenant is allowed without a quota.
func (scheduler *Scheduler) SpawnForTenant(tenant string, function StepFunc) (FiberID, error) {
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
	quota, quotaConfigured := scheduler.tenantQuotas[tenant]
	if quotaConfigured && quota.MaxFibers > 0 && scheduler.tenantFibers[tenant] >= quota.MaxFibers {
		return 0, ErrTenantQuotaExceeded
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
	slot.tenant = tenant
	if quotaConfigured && quota.MaxFibers > 0 {
		scheduler.tenantFibers[tenant]++
	}
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
	if !scheduler.hasStepQuotas {
		return scheduler.runWithoutStepQuotas(ctx, maxSteps)
	}

	var tenantSteps map[string]uint64
	if scheduler.hasStepQuotas {
		tenantSteps = make(map[string]uint64)
	}
	blockedByQuota := 0
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
			blockedByQuota = 0
			continue
		}
		if slot.status != StatusReady {
			blockedByQuota = 0
			continue
		}
		if scheduler.hasStepQuotas && scheduler.tenantStepQuotaExhausted(slot.tenant, tenantSteps) {
			scheduler.enqueue(slot)
			stats.Throttled++
			blockedByQuota++
			if blockedByQuota >= scheduler.readyCount {
				break
			}
			continue
		}
		blockedByQuota = 0

		slot.status = StatusRunning
		scheduler.current = identifier
		step, stepErr := slot.function(ctx)
		scheduler.current = 0
		stats.Steps++
		recordTenantStep(scheduler, slot.tenant, tenantSteps)
		switch {
		case stepErr != nil:
			slot.status = StatusFailed
			slot.err = stepErr
			slot.function = nil
			scheduler.clearFiberLocals(fiberIndex(identifier))
			stats.Failed++
		case step == StepWait:
			if slot.status != StatusWaiting {
				slot.status = StatusFailed
				slot.err = ErrFiberNotParked
				slot.function = nil
				scheduler.clearFiberLocals(fiberIndex(identifier))
				stats.Failed++
			} else {
				stats.Waited++
			}
		case step == StepYield:
			slot.status = StatusReady
			scheduler.enqueue(slot)
			stats.Yielded++
		case step == StepDone:
			slot.status = StatusDone
			slot.function = nil
			scheduler.clearFiberLocals(fiberIndex(identifier))
			stats.Completed++
		default:
			slot.status = StatusFailed
			slot.err = ErrInvalidStep
			slot.function = nil
			scheduler.clearFiberLocals(fiberIndex(identifier))
			stats.Failed++
		}
	}
	stats.Remaining = scheduler.readyCount
	return stats, nil
}

// runWithoutStepQuotas keeps the ordinary scheduler path free of tenant
// accounting and per-run map allocation. Fiber-count quotas are enforced at
// admission and do not affect scheduling once a fiber is ready.
func (scheduler *Scheduler) runWithoutStepQuotas(ctx context.Context, maxSteps int) (RunStats, error) {
	var stats RunStats
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
		scheduler.current = identifier
		step, stepErr := slot.function(ctx)
		scheduler.current = 0
		stats.Steps++
		switch {
		case stepErr != nil:
			slot.status = StatusFailed
			slot.err = stepErr
			slot.function = nil
			scheduler.clearFiberLocals(fiberIndex(identifier))
			stats.Failed++
		case step == StepWait:
			if slot.status != StatusWaiting {
				slot.status = StatusFailed
				slot.err = ErrFiberNotParked
				slot.function = nil
				scheduler.clearFiberLocals(fiberIndex(identifier))
				stats.Failed++
			} else {
				stats.Waited++
			}
		case step == StepYield:
			slot.status = StatusReady
			scheduler.enqueue(slot)
			stats.Yielded++
		case step == StepDone:
			slot.status = StatusDone
			slot.function = nil
			scheduler.clearFiberLocals(fiberIndex(identifier))
			stats.Completed++
		default:
			slot.status = StatusFailed
			slot.err = ErrInvalidStep
			slot.function = nil
			scheduler.clearFiberLocals(fiberIndex(identifier))
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
	case StatusReady, StatusWaiting:
		slot.status = StatusCancelled
		slot.err = context.Canceled
		slot.function = nil
		scheduler.clearFiberLocals(fiberIndex(identifier))
		return nil
	case StatusRunning:
		return ErrFiberNotFinished
	default:
		return ErrFiberFinished
	}
}

// Current returns the FiberID executing on the scheduler's owning goroutine,
// or zero when Run is outside a callback.
func (scheduler *Scheduler) Current() FiberID {
	if scheduler == nil {
		return 0
	}
	return scheduler.current
}

// ParkCurrent parks the callback currently executing on the scheduler. A
// synchronization primitive normally returns its Step/error pair directly.
func (scheduler *Scheduler) ParkCurrent() (Step, error) {
	if scheduler == nil || scheduler.current == 0 {
		return 0, ErrFiberNotRunning
	}
	slot, err := scheduler.lookup(scheduler.current)
	if err != nil {
		return 0, err
	}
	if slot.status != StatusRunning {
		return 0, ErrFiberNotRunning
	}
	slot.status = StatusWaiting
	return StepWait, nil
}

func (scheduler *Scheduler) isWaiting(identifier FiberID) bool {
	slot, err := scheduler.lookup(identifier)
	return err == nil && slot.status == StatusWaiting
}

func (scheduler *Scheduler) resume(identifier FiberID) bool {
	slot, err := scheduler.lookup(identifier)
	if err != nil || slot.status != StatusWaiting {
		return false
	}
	slot.status = StatusReady
	scheduler.enqueue(slot)
	return true
}

// Capacity returns the fixed number of fibers and synchronization waiters that
// this scheduler can retain.
func (scheduler *Scheduler) Capacity() int {
	if scheduler == nil {
		return 0
	}
	return len(scheduler.slots)
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
	scheduler.clearFiberLocals(fiberIndex(identifier))
	index := fiberIndex(identifier)
	generation := slot.generation
	if slot.tenant != "" {
		if quota, ok := scheduler.tenantQuotas[slot.tenant]; ok && quota.MaxFibers > 0 {
			scheduler.tenantFibers[slot.tenant]--
		}
	}
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
		if slot.status == StatusReady || slot.status == StatusWaiting {
			slot.status = StatusCancelled
			slot.err = context.Canceled
			slot.function = nil
			scheduler.clearFiberLocals(uint32(index))
		}
	}
}

func (scheduler *Scheduler) registerLocal(local fiberLocalStore) {
	if scheduler == nil || local == nil {
		return
	}
	scheduler.locals = append(scheduler.locals, local)
}

func (scheduler *Scheduler) clearFiberLocals(index uint32) {
	if scheduler == nil {
		return
	}
	for _, local := range scheduler.locals {
		local.clear(index)
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
