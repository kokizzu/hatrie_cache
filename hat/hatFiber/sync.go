package hatFiber

import "errors"

var (
	ErrSchedulerRequired  = errors.New("hatFiber: scheduler is required")
	ErrWaiterCapacity     = errors.New("hatFiber: synchronization waiter capacity exhausted")
	ErrChannelWouldBlock  = errors.New("hatFiber: channel operation would block")
	ErrChannelClosed      = errors.New("hatFiber: channel is closed")
	ErrChannelCapacity    = errors.New("hatFiber: channel capacity is invalid")
	ErrSemaphoreCount     = errors.New("hatFiber: semaphore count is invalid")
	ErrSemaphoreRelease   = errors.New("hatFiber: semaphore release count is invalid")
	ErrWaitGroupCount     = errors.New("hatFiber: wait-group count is invalid")
	ErrWaitGroupUnderflow = errors.New("hatFiber: wait-group count would become negative")
	ErrWaitGroupOverflow  = errors.New("hatFiber: wait-group count is too large")
)

const MaxChannelCapacity = 1 << 20

type fiberWaitQueue struct {
	scheduler *Scheduler
	ids       []FiberID
	head      int
	tail      int
	count     int
}

func newFiberWaitQueue(scheduler *Scheduler) (fiberWaitQueue, error) {
	if scheduler == nil {
		return fiberWaitQueue{}, ErrSchedulerRequired
	}
	return fiberWaitQueue{
		scheduler: scheduler,
		ids:       make([]FiberID, scheduler.Capacity()),
	}, nil
}

func (queue *fiberWaitQueue) waitCurrent() (Step, error) {
	if queue == nil || queue.scheduler == nil {
		return 0, ErrSchedulerRequired
	}
	if queue.scheduler.Current() == 0 {
		return 0, ErrFiberNotRunning
	}
	if queue.count == len(queue.ids) {
		return 0, ErrWaiterCapacity
	}
	queue.ids[queue.tail] = queue.scheduler.Current()
	queue.tail++
	if queue.tail == len(queue.ids) {
		queue.tail = 0
	}
	queue.count++
	return queue.scheduler.ParkCurrent()
}

func (queue *fiberWaitQueue) popWaiting() FiberID {
	for queue != nil && queue.count > 0 {
		identifier := queue.ids[queue.head]
		queue.ids[queue.head] = 0
		queue.head++
		if queue.head == len(queue.ids) {
			queue.head = 0
		}
		queue.count--
		if queue.scheduler.isWaiting(identifier) {
			return identifier
		}
	}
	return 0
}

func (queue *fiberWaitQueue) wakeOne() bool {
	if queue == nil {
		return false
	}
	identifier := queue.popWaiting()
	if identifier == 0 {
		return false
	}
	return queue.scheduler.resume(identifier)
}

func (queue *fiberWaitQueue) wakeAll() int {
	woken := 0
	for queue != nil {
		identifier := queue.popWaiting()
		if identifier == 0 {
			return woken
		}
		if queue.scheduler.resume(identifier) {
			woken++
		}
	}
	return woken
}

// Condition is a bounded fiber-aware condition queue. Callers should check
// their predicate before calling Wait and after the returned fiber resumes.
type Condition struct {
	waiters fiberWaitQueue
}

// NewCondition creates a condition whose waiter bound equals the scheduler's
// MaxFibers setting.
func NewCondition(scheduler *Scheduler) (*Condition, error) {
	waiters, err := newFiberWaitQueue(scheduler)
	if err != nil {
		return nil, err
	}
	return &Condition{waiters: waiters}, nil
}

// Wait parks the current fiber until Signal or Broadcast wakes it.
func (condition *Condition) Wait() (Step, error) {
	if condition == nil {
		return 0, ErrSchedulerRequired
	}
	return condition.waiters.waitCurrent()
}

// Signal wakes one waiting fiber, returning false when no live waiter exists.
func (condition *Condition) Signal() bool {
	return condition != nil && condition.waiters.wakeOne()
}

// Broadcast wakes every live waiting fiber and returns the number resumed.
func (condition *Condition) Broadcast() int {
	if condition == nil {
		return 0
	}
	return condition.waiters.wakeAll()
}

// Pending reports queued condition waiters, including stale entries not yet
// consumed by a signal.
func (condition *Condition) Pending() int {
	if condition == nil {
		return 0
	}
	return condition.waiters.count
}

// Semaphore is a bounded-count fiber-aware semaphore.
type Semaphore struct {
	available int
	waiters   fiberWaitQueue
}

// NewSemaphore creates a semaphore with an initial token count.
func NewSemaphore(scheduler *Scheduler, initial int) (*Semaphore, error) {
	if initial < 0 || initial > MaxFibers {
		return nil, ErrSemaphoreCount
	}
	waiters, err := newFiberWaitQueue(scheduler)
	if err != nil {
		return nil, err
	}
	return &Semaphore{available: initial, waiters: waiters}, nil
}

// TryAcquire consumes one token without parking.
func (semaphore *Semaphore) TryAcquire() bool {
	if semaphore == nil || semaphore.available == 0 {
		return false
	}
	semaphore.available--
	return true
}

// WaitAcquire consumes an available token or parks the current fiber. If a
// token became available before the call, StepYield lets the callback retry.
func (semaphore *Semaphore) WaitAcquire() (Step, error) {
	if semaphore == nil {
		return 0, ErrSchedulerRequired
	}
	if semaphore.TryAcquire() {
		return StepYield, nil
	}
	return semaphore.waiters.waitCurrent()
}

// Release returns count tokens and wakes up to count waiting fibers.
func (semaphore *Semaphore) Release(count int) error {
	if semaphore == nil {
		return ErrSchedulerRequired
	}
	if count < 1 || count > MaxFibers || semaphore.available > MaxFibers-count {
		return ErrSemaphoreRelease
	}
	semaphore.available += count
	for index := 0; index < count; index++ {
		if !semaphore.waiters.wakeOne() {
			break
		}
	}
	return nil
}

// Available reports currently unclaimed tokens.
func (semaphore *Semaphore) Available() int {
	if semaphore == nil {
		return 0
	}
	return semaphore.available
}

// WaitGroup is a bounded fiber-aware counter. A waiter resumes when the count
// reaches zero.
type WaitGroup struct {
	count   int
	waiters fiberWaitQueue
}

// NewWaitGroup creates a wait group with count initial.
func NewWaitGroup(scheduler *Scheduler, initial int) (*WaitGroup, error) {
	if initial < 0 || initial > MaxFibers {
		return nil, ErrWaitGroupCount
	}
	waiters, err := newFiberWaitQueue(scheduler)
	if err != nil {
		return nil, err
	}
	return &WaitGroup{count: initial, waiters: waiters}, nil
}

// Add changes the count. A negative delta cannot underflow; reaching zero
// wakes all waiters.
func (group *WaitGroup) Add(delta int) error {
	if group == nil {
		return ErrSchedulerRequired
	}
	if delta > 0 {
		if delta > MaxFibers-group.count {
			return ErrWaitGroupOverflow
		}
	} else if delta < 0 {
		if delta < -group.count {
			return ErrWaitGroupUnderflow
		}
	}
	group.count += delta
	if group.count == 0 {
		group.waiters.wakeAll()
	}
	return nil
}

// Done decrements the count by one.
func (group *WaitGroup) Done() error {
	return group.Add(-1)
}

// Wait parks the current fiber until the count reaches zero. If it is already
// zero, StepYield lets the callback proceed on its next invocation.
func (group *WaitGroup) Wait() (Step, error) {
	if group == nil {
		return 0, ErrSchedulerRequired
	}
	if group.count == 0 {
		return StepYield, nil
	}
	return group.waiters.waitCurrent()
}

// Count reports the current counter.
func (group *WaitGroup) Count() int {
	if group == nil {
		return 0
	}
	return group.count
}
