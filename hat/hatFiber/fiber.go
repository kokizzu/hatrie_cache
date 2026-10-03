// Package hatFiber provides bounded, stackless cooperative fibers.
//
// A fiber is a continuation: each Step does a bounded amount of work and
// returns the next Step to yield and resume later. The scheduler reuses a
// fixed worker set, so applications can keep many logical workflows without
// starting one goroutine per workflow.
package hatFiber

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrSchedulerInvalid reports invalid scheduler options or a nil Step.
	ErrSchedulerInvalid = errors.New("hatFiber: invalid scheduler")
	// ErrSchedulerClosed reports a Spawn after Close or Wait.
	ErrSchedulerClosed = errors.New("hatFiber: scheduler is closed")
	// ErrSchedulerFull reports that MaxFibers or QueueCapacity is exhausted.
	ErrSchedulerFull = errors.New("hatFiber: scheduler is full")
)

const (
	defaultWorkers       = 1
	defaultMaxFibers     = 1024
	defaultQueueCapacity = 1024
)

// Step performs one bounded unit of fiber work. Returning another Step yields
// to the scheduler and resumes that continuation later. Returning nil marks
// the fiber complete. A non-nil error stops the scheduler and is returned by
// Wait.
type Step func(Context) (Step, error)

// Options configures a Scheduler. Zero values use conservative defaults.
// QueueCapacity must be at least MaxFibers when both are specified, which
// ensures a yielding worker can always put its continuation back in the
// ready queue without blocking all workers.
type Options struct {
	Workers       int
	MaxFibers     int
	QueueCapacity int
}

// Context is the context visible to a running Step. It combines the context
// supplied to Spawn with the scheduler context. Context methods should be
// checked by long-running steps before returning their next continuation.
type Context struct {
	schedulerCtx context.Context
	fiberCtx     context.Context
	scheduler    *Scheduler
	fiberState   *fiber
	id           uint64
}

// Context returns the context supplied to Spawn. Use Err to observe both this
// context and scheduler cancellation without allocating a derived context.
func (ctx *Context) Context() context.Context {
	if ctx == nil {
		return nil
	}
	return ctx.fiberCtx
}

// Deadline delegates to the context supplied to Spawn.
func (ctx *Context) Deadline() (deadline time.Time, ok bool) {
	if ctx == nil || ctx.fiberCtx == nil {
		return time.Time{}, false
	}
	return ctx.fiberCtx.Deadline()
}

// Done delegates to the context supplied to Spawn. Scheduler cancellation is
// reported by Err at Step boundaries.
func (ctx *Context) Done() <-chan struct{} {
	if ctx == nil || ctx.fiberCtx == nil {
		return nil
	}
	return ctx.fiberCtx.Done()
}

// Err reports cancellation of either the fiber or its scheduler.
func (ctx *Context) Err() error {
	if ctx == nil {
		return nil
	}
	if ctx.fiberCtx != nil {
		if err := ctx.fiberCtx.Err(); err != nil {
			return err
		}
	}
	if ctx.schedulerCtx != nil {
		return ctx.schedulerCtx.Err()
	}
	return nil
}

// Value delegates value lookup to the context supplied to Spawn.
func (ctx *Context) Value(key any) any {
	if ctx == nil || ctx.fiberCtx == nil {
		return nil
	}
	return ctx.fiberCtx.Value(key)
}

// ID returns the stable identifier assigned by Spawn.
func (ctx *Context) ID() uint64 {
	if ctx == nil {
		return 0
	}
	return ctx.id
}

// Await parks the current fiber until signal wakes it, then resumes next.
// The current Step must return the result of Await immediately.
func (ctx Context) Await(signal *Signal, next Step) (Step, error) {
	if ctx.scheduler == nil || ctx.fiberState == nil || signal == nil || next == nil {
		return nil, ErrCoordinationInvalid
	}
	return signal.wait(ctx, next)
}

type fiber struct {
	step          Step
	ctx           context.Context
	id            uint64
	slot          int
	locals        map[any]any
	localMu       sync.RWMutex
	localID       atomic.Uint64
	running       bool
	parked        bool
	wakePending   bool
	cancelPending bool
	waitSignal    *Signal
	waitNext      Step
}

// Stats is a point-in-time scheduler snapshot.
type Stats struct {
	Active    int
	Queued    int
	Running   int
	Parked    int
	Completed uint64
	Yielded   uint64
	Canceled  uint64
	Closed    bool
}

// Scheduler runs continuation-based fibers over a fixed worker set.
type Scheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	stop   func() bool

	queue  []*fiber
	head   int
	size   int
	fibers []fiber
	free   []int
	freeN  int

	maxFibers int

	mu       sync.Mutex
	ready    *sync.Cond
	closed   bool
	active   int
	running  int
	firstErr error

	completed uint64
	yielded   uint64
	canceled  uint64
	nextID    uint64

	workers  sync.WaitGroup
	waitOnce sync.Once
	waitErr  error
}

// NewScheduler starts a scheduler. The scheduler is active immediately, so
// Spawn may be called as soon as this function returns.
func NewScheduler(parent context.Context, options Options) (*Scheduler, error) {
	if options.Workers < 0 || options.MaxFibers < 0 || options.QueueCapacity < 0 {
		return nil, ErrSchedulerInvalid
	}
	if options.Workers == 0 {
		options.Workers = defaultWorkers
	}
	if options.MaxFibers == 0 {
		options.MaxFibers = defaultMaxFibers
	}
	if options.QueueCapacity == 0 {
		options.QueueCapacity = defaultQueueCapacity
	}
	if options.QueueCapacity < options.MaxFibers {
		return nil, ErrSchedulerInvalid
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	scheduler := &Scheduler{
		ctx:       ctx,
		cancel:    cancel,
		maxFibers: options.MaxFibers,
		queue:     make([]*fiber, options.QueueCapacity),
		fibers:    make([]fiber, options.MaxFibers),
		free:      make([]int, options.MaxFibers),
		freeN:     options.MaxFibers,
	}
	for index := range scheduler.free {
		scheduler.free[index] = index
		scheduler.fibers[index].slot = index
	}
	scheduler.ready = sync.NewCond(&scheduler.mu)
	scheduler.stop = context.AfterFunc(ctx, scheduler.cancelParked)
	for range options.Workers {
		scheduler.workers.Add(1)
		go scheduler.run()
	}
	return scheduler, nil
}

// Spawn admits one fiber if the live-fiber and ready-queue bounds permit it.
// It returns ErrSchedulerFull instead of blocking, so callers can apply their
// own backpressure policy without tying up a worker.
func (scheduler *Scheduler) Spawn(parent context.Context, step Step) (uint64, error) {
	if scheduler == nil || step == nil {
		return 0, ErrSchedulerInvalid
	}
	if parent == nil {
		parent = context.Background()
	}
	if err := parent.Err(); err != nil {
		return 0, err
	}

	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.closed {
		return 0, ErrSchedulerClosed
	}
	if err := scheduler.ctx.Err(); err != nil {
		return 0, err
	}
	if scheduler.active >= scheduler.maxFibers || scheduler.size >= len(scheduler.queue) {
		return 0, ErrSchedulerFull
	}

	slot := scheduler.freeN - 1
	scheduler.freeN = slot
	fiber := &scheduler.fibers[scheduler.free[slot]]
	scheduler.nextID++
	fiber.step = step
	fiber.ctx = parent
	fiber.id = scheduler.nextID
	fiber.localMu.Lock()
	fiber.localID.Store(fiber.id)
	fiber.localMu.Unlock()
	scheduler.pushLocked(fiber)
	scheduler.active++
	scheduler.ready.Signal()
	return fiber.id, nil
}

// Stats returns a point-in-time scheduler snapshot.
func (scheduler *Scheduler) Stats() Stats {
	if scheduler == nil {
		return Stats{}
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	return Stats{
		Active:    scheduler.active,
		Queued:    scheduler.size,
		Running:   scheduler.running,
		Parked:    scheduler.parkedCountLocked(),
		Completed: scheduler.completed,
		Yielded:   scheduler.yielded,
		Canceled:  scheduler.canceled,
		Closed:    scheduler.closed,
	}
}

// Cancel stops waiting and queued fibers. A running Step must check its
// Context and return for cancellation to complete promptly.
func (scheduler *Scheduler) Cancel() {
	if scheduler == nil || scheduler.cancel == nil {
		return
	}
	scheduler.cancel()
	scheduler.cancelParked()
}

// Close rejects new fibers and drains fibers already admitted.
func (scheduler *Scheduler) Close() {
	if scheduler == nil {
		return
	}
	scheduler.mu.Lock()
	if !scheduler.closed {
		scheduler.closed = true
		scheduler.discardParkedLocked()
		scheduler.ready.Broadcast()
	}
	scheduler.mu.Unlock()
}

// Wait closes the scheduler, drains admitted fibers, and returns the first
// Step error. It is safe to call more than once.
func (scheduler *Scheduler) Wait() error {
	if scheduler == nil {
		return ErrSchedulerInvalid
	}
	scheduler.waitOnce.Do(func() {
		scheduler.Close()
		scheduler.workers.Wait()
		scheduler.mu.Lock()
		firstErr := scheduler.firstErr
		contextErr := scheduler.ctx.Err()
		scheduler.mu.Unlock()
		scheduler.cancel()
		if scheduler.stop != nil {
			scheduler.stop()
		}
		if firstErr != nil {
			scheduler.waitErr = firstErr
		} else {
			scheduler.waitErr = contextErr
		}
	})
	return scheduler.waitErr
}

// WaitIdle waits until all currently admitted fibers finish without closing
// the scheduler. It is useful for long-lived services that reuse the worker
// set and fixed fiber slots across batches.
func (scheduler *Scheduler) WaitIdle(parent context.Context) error {
	if scheduler == nil {
		return ErrSchedulerInvalid
	}
	if parent == nil {
		parent = context.Background()
	}
	if err := parent.Err(); err != nil {
		return err
	}
	stop := context.AfterFunc(parent, scheduler.wake)
	defer stop()

	scheduler.mu.Lock()
	for scheduler.active > 0 && scheduler.firstErr == nil && scheduler.ctx.Err() == nil && parent.Err() == nil {
		scheduler.ready.Wait()
	}
	firstErr := scheduler.firstErr
	contextErr := scheduler.ctx.Err()
	parentErr := parent.Err()
	scheduler.mu.Unlock()
	if firstErr != nil {
		return firstErr
	}
	if parentErr != nil {
		return parentErr
	}
	return contextErr
}

func (scheduler *Scheduler) run() {
	defer scheduler.workers.Done()
	for {
		scheduler.mu.Lock()
		for scheduler.size == 0 && !scheduler.closed && scheduler.ctx.Err() == nil {
			scheduler.ready.Wait()
		}
		if scheduler.ctx.Err() != nil {
			scheduler.discardLocked()
			scheduler.mu.Unlock()
			return
		}
		if scheduler.size == 0 && scheduler.closed {
			scheduler.mu.Unlock()
			return
		}
		fiber := scheduler.popLocked()
		fiber.running = true
		scheduler.running++
		scheduler.mu.Unlock()

		var next Step
		var err error
		if scheduler.fiberErr(fiber) == nil {
			next, err = fiber.step(Context{schedulerCtx: scheduler.ctx, fiberCtx: fiber.ctx, scheduler: scheduler, fiberState: fiber, id: fiber.id})
		}
		shouldCancel := scheduler.finishStep(fiber, next, err)
		if shouldCancel {
			scheduler.cancel()
			scheduler.wake()
		}
	}
}

func (scheduler *Scheduler) finishStep(fiber *fiber, next Step, err error) bool {
	scheduler.mu.Lock()
	defer func() {
		scheduler.ready.Broadcast()
		scheduler.mu.Unlock()
	}()
	scheduler.running--
	fiber.running = false

	if fiber.cancelPending || scheduler.fiberErr(fiber) != nil || errors.Is(err, ErrSchedulerClosed) {
		scheduler.finishCanceledLocked(fiber)
		return false
	}
	if fiber.wakePending {
		fiber.wakePending = false
		scheduler.pushLocked(fiber)
		scheduler.yielded++
		scheduler.ready.Signal()
		return false
	}
	if fiber.parked {
		return false
	}
	if err != nil {
		scheduler.finishFiberLocked(fiber)
		if scheduler.firstErr == nil {
			scheduler.firstErr = err
			scheduler.ready.Broadcast()
			return true
		}
		return false
	}
	if next == nil {
		scheduler.finishFiberLocked(fiber)
		return false
	}
	if scheduler.ctx.Err() != nil {
		scheduler.finishCanceledLocked(fiber)
		return false
	}
	fiber.step = next
	scheduler.pushLocked(fiber)
	scheduler.yielded++
	scheduler.ready.Signal()
	return false
}

func (scheduler *Scheduler) finishFiberLocked(fiber *fiber) {
	scheduler.active--
	scheduler.completed++
	scheduler.releaseFiberLocked(fiber)
}

func (scheduler *Scheduler) finishCanceledLocked(fiber *fiber) {
	scheduler.active--
	scheduler.canceled++
	scheduler.releaseFiberLocked(fiber)
}

func (scheduler *Scheduler) releaseFiberLocked(fiber *fiber) {
	fiber.localMu.Lock()
	if len(fiber.locals) > maxRetainedLocalEntries {
		fiber.locals = nil
	} else {
		for key := range fiber.locals {
			delete(fiber.locals, key)
		}
	}
	fiber.localID.Store(0)
	fiber.localMu.Unlock()
	fiber.step = nil
	fiber.ctx = nil
	fiber.running = false
	fiber.parked = false
	fiber.wakePending = false
	fiber.cancelPending = false
	fiber.waitSignal = nil
	fiber.waitNext = nil
	scheduler.free[scheduler.freeN] = fiber.slot
	scheduler.freeN++
}

func (scheduler *Scheduler) fiberErr(fiber *fiber) error {
	if fiber.ctx != nil {
		if err := fiber.ctx.Err(); err != nil {
			return err
		}
	}
	return scheduler.ctx.Err()
}

func (scheduler *Scheduler) parkedCountLocked() int {
	parked := 0
	for index := range scheduler.fibers {
		if scheduler.fibers[index].parked {
			parked++
		}
	}
	return parked
}

func (scheduler *Scheduler) discardLocked() {
	for scheduler.size > 0 {
		fiber := scheduler.popLocked()
		scheduler.finishCanceledLocked(fiber)
	}
	scheduler.ready.Broadcast()
}

func (scheduler *Scheduler) pushLocked(fiber *fiber) {
	index := (scheduler.head + scheduler.size) % len(scheduler.queue)
	scheduler.queue[index] = fiber
	scheduler.size++
}

func (scheduler *Scheduler) popLocked() *fiber {
	fiber := scheduler.queue[scheduler.head]
	scheduler.queue[scheduler.head] = nil
	scheduler.head = (scheduler.head + 1) % len(scheduler.queue)
	scheduler.size--
	return fiber
}

func (scheduler *Scheduler) wake() {
	scheduler.mu.Lock()
	scheduler.ready.Broadcast()
	scheduler.mu.Unlock()
}

func (scheduler *Scheduler) cancelParked() {
	scheduler.mu.Lock()
	scheduler.discardParkedLocked()
	scheduler.ready.Broadcast()
	scheduler.mu.Unlock()
}

func (scheduler *Scheduler) discardParkedLocked() {
	for index := range scheduler.fibers {
		fiber := &scheduler.fibers[index]
		if !fiber.parked {
			continue
		}
		if fiber.waitSignal != nil {
			fiber.waitSignal.removeWaiter(fiber, fiber.id)
		}
		fiber.parked = false
		fiber.waitSignal = nil
		fiber.waitNext = nil
		if fiber.running {
			fiber.cancelPending = true
			continue
		}
		scheduler.finishCanceledLocked(fiber)
	}
}
