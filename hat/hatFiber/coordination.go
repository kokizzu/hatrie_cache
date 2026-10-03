package hatFiber

import (
	"errors"
	"sync"
)

var (
	// ErrCoordinationInvalid reports an invalid coordination primitive or
	// missing continuation.
	ErrCoordinationInvalid = errors.New("hatFiber: invalid coordination primitive")
	// ErrChannelClosed reports an operation on a closed channel.
	ErrChannelClosed = errors.New("hatFiber: channel is closed")
	// ErrChannelWouldBlock reports a nonblocking channel operation with no
	// capacity or value available.
	ErrChannelWouldBlock = errors.New("hatFiber: channel would block")
	// ErrWaitGroupNegative reports a wait-group counter below zero.
	ErrWaitGroupNegative = errors.New("hatFiber: negative wait-group counter")
)

type signalWaiter struct {
	scheduler *Scheduler
	fiber     *fiber
	id        uint64
	signal    *Signal
}

// Signal is a zero-allocation-after-growth wake-up list for parked fibers.
// NotifyOne wakes one current waiter; NotifyAll wakes every current waiter.
// Signal notifications are edge-triggered, so callers should check their
// predicate before waiting.
type Signal struct {
	mu      sync.Mutex
	waiters []signalWaiter
}

// Wait parks the current fiber until this signal wakes it, then resumes next.
func (signal *Signal) Wait(ctx Context, next Step) (Step, error) {
	if signal == nil {
		return nil, ErrCoordinationInvalid
	}
	return signal.wait(ctx, next)
}

// NotifyOne wakes the oldest currently parked waiter.
func (signal *Signal) NotifyOne() {
	if signal == nil {
		return
	}
	signal.mu.Lock()
	if len(signal.waiters) == 0 {
		signal.mu.Unlock()
		return
	}
	waiter := signal.waiters[0]
	signal.waiters[0] = signal.waiters[len(signal.waiters)-1]
	signal.waiters = signal.waiters[:len(signal.waiters)-1]
	signal.mu.Unlock()
	waiter.scheduler.resumeWaiter(waiter)
}

// NotifyAll wakes every currently parked waiter.
func (signal *Signal) NotifyAll() {
	if signal == nil {
		return
	}
	signal.mu.Lock()
	waiters := append([]signalWaiter(nil), signal.waiters...)
	signal.waiters = signal.waiters[:0]
	signal.mu.Unlock()
	for _, waiter := range waiters {
		waiter.scheduler.resumeWaiter(waiter)
	}
}

// WaiterCount returns the number of currently registered waiters.
func (signal *Signal) WaiterCount() int {
	if signal == nil {
		return 0
	}
	signal.mu.Lock()
	defer signal.mu.Unlock()
	return len(signal.waiters)
}

func (signal *Signal) wait(ctx Context, next Step) (Step, error) {
	if signal == nil || ctx.scheduler == nil || ctx.fiberState == nil || next == nil {
		return nil, ErrCoordinationInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	scheduler := ctx.scheduler
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.closed {
		return nil, ErrSchedulerClosed
	}
	if err := scheduler.ctx.Err(); err != nil {
		return nil, err
	}
	fiber := ctx.fiberState
	if fiber.parked || fiber.cancelPending {
		return nil, ErrCoordinationInvalid
	}
	signal.mu.Lock()
	fiber.step = nil
	fiber.parked = true
	fiber.waitSignal = signal
	fiber.waitNext = next
	signal.waiters = append(signal.waiters, signalWaiter{
		scheduler: scheduler,
		fiber:     fiber,
		id:        fiber.id,
		signal:    signal,
	})
	signal.mu.Unlock()
	return nil, nil
}

func (signal *Signal) removeWaiter(fiber *fiber, id uint64) {
	if signal == nil {
		return
	}
	signal.mu.Lock()
	defer signal.mu.Unlock()
	for index := range signal.waiters {
		waiter := signal.waiters[index]
		if waiter.fiber != fiber || waiter.id != id {
			continue
		}
		signal.waiters[index] = signal.waiters[len(signal.waiters)-1]
		signal.waiters = signal.waiters[:len(signal.waiters)-1]
		return
	}
}

func (scheduler *Scheduler) resumeWaiter(waiter signalWaiter) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	fiber := waiter.fiber
	if fiber == nil || fiber.id != waiter.id || !fiber.parked || fiber.waitSignal != waiter.signal {
		return
	}
	fiber.parked = false
	fiber.waitSignal = nil
	fiber.step = fiber.waitNext
	fiber.waitNext = nil
	if scheduler.closed || scheduler.ctx.Err() != nil || fiber.cancelPending {
		if fiber.running {
			fiber.cancelPending = true
			return
		}
		scheduler.finishCanceledLocked(fiber)
		return
	}
	if fiber.running {
		fiber.wakePending = true
		return
	}
	scheduler.pushLocked(fiber)
	scheduler.ready.Signal()
}

// ReceiveResult describes one nonblocking or parked channel receive.
type ReceiveResult[T any] struct {
	Value   T
	OK      bool
	Blocked bool
}

// Channel is a typed, bounded, fiber-aware channel. Send and Receive never
// block a worker. ReceiveStep and SendStep atomically register a continuation
// when the operation would block, so no wake-up is lost between checking the
// predicate and parking.
type Channel[T any] struct {
	mu       sync.Mutex
	values   []T
	head     int
	size     int
	closed   bool
	readable Signal
	writable Signal
}

// NewChannel creates a bounded fiber-aware channel.
func NewChannel[T any](capacity int) (*Channel[T], error) {
	if capacity <= 0 {
		return nil, ErrCoordinationInvalid
	}
	return &Channel[T]{values: make([]T, capacity)}, nil
}

// Send publishes a value without blocking. ErrChannelWouldBlock means the
// channel is full; the caller retains ownership of value and may retry.
func (channel *Channel[T]) Send(value T) error {
	if channel == nil {
		return ErrCoordinationInvalid
	}
	channel.mu.Lock()
	if channel.closed {
		channel.mu.Unlock()
		return ErrChannelClosed
	}
	if channel.size == len(channel.values) {
		channel.mu.Unlock()
		return ErrChannelWouldBlock
	}
	index := (channel.head + channel.size) % len(channel.values)
	channel.values[index] = value
	channel.size++
	channel.mu.Unlock()
	channel.readable.NotifyOne()
	return nil
}

// Receive removes one value without blocking. An empty open channel returns
// ErrChannelWouldBlock; a closed and drained channel returns ok=false, nil.
func (channel *Channel[T]) Receive() (value T, ok bool, err error) {
	if channel == nil {
		return value, false, ErrCoordinationInvalid
	}
	channel.mu.Lock()
	value, ok, err = channel.receiveLocked()
	channel.mu.Unlock()
	if err == nil && ok {
		channel.writable.NotifyOne()
	}
	return value, ok, err
}

// ReceiveStep attempts a receive and parks the current fiber when the
// channel is empty. Blocked is true only when the current Step should return
// nil, nil after this method successfully registered its continuation.
func (channel *Channel[T]) ReceiveStep(ctx Context, next Step) (ReceiveResult[T], error) {
	if channel == nil || next == nil {
		return ReceiveResult[T]{}, ErrCoordinationInvalid
	}
	channel.mu.Lock()
	value, ok, err := channel.receiveLocked()
	if err == nil && ok {
		channel.mu.Unlock()
		channel.writable.NotifyOne()
		return ReceiveResult[T]{Value: value, OK: true}, nil
	}
	if err == nil && !ok {
		channel.mu.Unlock()
		return ReceiveResult[T]{}, nil
	}
	if !errors.Is(err, ErrChannelWouldBlock) {
		channel.mu.Unlock()
		return ReceiveResult[T]{}, err
	}
	_, waitErr := ctx.Await(&channel.readable, next)
	channel.mu.Unlock()
	if waitErr != nil {
		return ReceiveResult[T]{}, waitErr
	}
	return ReceiveResult[T]{Blocked: true}, nil
}

// SendStep attempts a send and parks the current fiber when the channel is
// full. The returned bool is true when the current Step should return nil,
// nil after its continuation is registered.
func (channel *Channel[T]) SendStep(ctx Context, value T, next Step) (blocked bool, err error) {
	if channel == nil || next == nil {
		return false, ErrCoordinationInvalid
	}
	channel.mu.Lock()
	if channel.closed {
		channel.mu.Unlock()
		return false, ErrChannelClosed
	}
	if channel.size < len(channel.values) {
		index := (channel.head + channel.size) % len(channel.values)
		channel.values[index] = value
		channel.size++
		channel.mu.Unlock()
		channel.readable.NotifyOne()
		return false, nil
	}
	_, waitErr := ctx.Await(&channel.writable, next)
	channel.mu.Unlock()
	return waitErr == nil, waitErr
}

// Close prevents sends, drains buffered values, and wakes parked senders and
// receivers. It is safe to call more than once.
func (channel *Channel[T]) Close() {
	if channel == nil {
		return
	}
	channel.mu.Lock()
	if channel.closed {
		channel.mu.Unlock()
		return
	}
	channel.closed = true
	channel.mu.Unlock()
	channel.readable.NotifyAll()
	channel.writable.NotifyAll()
}

// Len returns the number of buffered values.
func (channel *Channel[T]) Len() int {
	if channel == nil {
		return 0
	}
	channel.mu.Lock()
	defer channel.mu.Unlock()
	return channel.size
}

// Capacity returns the configured buffer capacity.
func (channel *Channel[T]) Capacity() int {
	if channel == nil {
		return 0
	}
	return len(channel.values)
}

// WaiterCount returns the number of parked channel fibers.
func (channel *Channel[T]) WaiterCount() int {
	if channel == nil {
		return 0
	}
	return channel.readable.WaiterCount() + channel.writable.WaiterCount()
}

func (channel *Channel[T]) receiveLocked() (value T, ok bool, err error) {
	if channel.size > 0 {
		value = channel.values[channel.head]
		var zero T
		channel.values[channel.head] = zero
		channel.head = (channel.head + 1) % len(channel.values)
		channel.size--
		return value, true, nil
	}
	if channel.closed {
		return value, false, nil
	}
	return value, false, ErrChannelWouldBlock
}

// Condition is a fiber-aware broadcast condition. Callers own the predicate
// and should check it before waiting.
type Condition struct {
	signal Signal
}

// Wait parks the current fiber until NotifyOne or NotifyAll, then resumes
// next.
func (condition *Condition) Wait(ctx Context, next Step) (Step, error) {
	if condition == nil {
		return nil, ErrCoordinationInvalid
	}
	return condition.signal.Wait(ctx, next)
}

// NotifyOne wakes one condition waiter.
func (condition *Condition) NotifyOne() {
	if condition != nil {
		condition.signal.NotifyOne()
	}
}

// NotifyAll wakes all condition waiters.
func (condition *Condition) NotifyAll() {
	if condition != nil {
		condition.signal.NotifyAll()
	}
}

// WaiterCount returns the number of parked condition fibers.
func (condition *Condition) WaiterCount() int {
	if condition == nil {
		return 0
	}
	return condition.signal.WaiterCount()
}

// Semaphore is a bounded fiber-aware permit counter.
type Semaphore struct {
	mu      sync.Mutex
	permits int
	signal  Signal
}

// NewSemaphore creates a semaphore with permits available.
func NewSemaphore(permits int) (*Semaphore, error) {
	if permits < 0 {
		return nil, ErrCoordinationInvalid
	}
	return &Semaphore{permits: permits}, nil
}

// TryAcquire takes one permit without blocking.
func (semaphore *Semaphore) TryAcquire() bool {
	if semaphore == nil {
		return false
	}
	semaphore.mu.Lock()
	defer semaphore.mu.Unlock()
	if semaphore.permits == 0 {
		return false
	}
	semaphore.permits--
	return true
}

// AcquireStep takes a permit or parks the current fiber. ready is true when
// the permit was acquired and next should be returned by the Step.
func (semaphore *Semaphore) AcquireStep(ctx Context, next Step) (step Step, ready bool, err error) {
	if semaphore == nil || next == nil {
		return nil, false, ErrCoordinationInvalid
	}
	semaphore.mu.Lock()
	if semaphore.permits > 0 {
		semaphore.permits--
		semaphore.mu.Unlock()
		return next, true, nil
	}
	step, err = ctx.Await(&semaphore.signal, next)
	semaphore.mu.Unlock()
	return step, false, err
}

// Release returns one permit and wakes one waiter.
func (semaphore *Semaphore) Release() error {
	if semaphore == nil {
		return ErrCoordinationInvalid
	}
	semaphore.mu.Lock()
	semaphore.permits++
	semaphore.mu.Unlock()
	semaphore.signal.NotifyOne()
	return nil
}

// Available returns the current permit count.
func (semaphore *Semaphore) Available() int {
	if semaphore == nil {
		return 0
	}
	semaphore.mu.Lock()
	defer semaphore.mu.Unlock()
	return semaphore.permits
}

// WaiterCount returns the number of parked semaphore fibers.
func (semaphore *Semaphore) WaiterCount() int {
	if semaphore == nil {
		return 0
	}
	return semaphore.signal.WaiterCount()
}

// WaitGroup counts work and lets a fiber park until the count reaches zero.
type WaitGroup struct {
	mu     sync.Mutex
	count  int
	signal Signal
}

// Add changes the wait-group count. A negative result is rejected.
func (group *WaitGroup) Add(delta int) error {
	if group == nil {
		return ErrCoordinationInvalid
	}
	group.mu.Lock()
	if delta < 0 && -delta > group.count {
		group.mu.Unlock()
		return ErrWaitGroupNegative
	}
	group.count += delta
	ready := group.count == 0 && delta < 0
	group.mu.Unlock()
	if ready {
		group.signal.NotifyAll()
	}
	return nil
}

// Done decrements the wait-group count.
func (group *WaitGroup) Done() error {
	return group.Add(-1)
}

// WaitStep parks until the count reaches zero. ready is true when next can
// continue immediately.
func (group *WaitGroup) WaitStep(ctx Context, next Step) (step Step, ready bool, err error) {
	if group == nil || next == nil {
		return nil, false, ErrCoordinationInvalid
	}
	group.mu.Lock()
	if group.count == 0 {
		group.mu.Unlock()
		return next, true, nil
	}
	step, err = ctx.Await(&group.signal, next)
	group.mu.Unlock()
	return step, false, err
}

// Count returns the current wait-group count.
func (group *WaitGroup) Count() int {
	if group == nil {
		return 0
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	return group.count
}

// WaiterCount returns the number of parked wait-group fibers.
func (group *WaitGroup) WaiterCount() int {
	if group == nil {
		return 0
	}
	return group.signal.WaiterCount()
}
