package hatPipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	// ErrWorkStealingPoolInvalid reports an invalid pool or task configuration.
	ErrWorkStealingPoolInvalid = errors.New("hatPipeline: invalid work-stealing pool")
	// ErrWorkStealingPoolClosed reports a submission after Close.
	ErrWorkStealingPoolClosed = errors.New("hatPipeline: work-stealing pool is closed")
)

// WorkStealingStats reports work-stealing pool activity since construction.
// Submitted and Completed count task executions admitted and finished by the
// pool. Stolen counts tasks claimed from a worker other than their owner.
type WorkStealingStats struct {
	Submitted uint64
	Completed uint64
	Stolen    uint64
}

// WorkStealingPool runs cooperative tasks over independent worker deques.
// Owners pop their own newest task first; idle workers steal the oldest task
// from another deque. QueueCapacity bounds the total number of waiting tasks,
// while running tasks are not counted against that bound.
//
// The pool is opt-in. Close drains admitted tasks, while Cancel stops workers;
// tasks must observe their context to stop promptly after cancellation.
type WorkStealingPool struct {
	ctx    context.Context
	cancel context.CancelFunc

	queues []workStealingDeque
	slots  chan struct{}
	notify chan struct{}

	closedCh  chan struct{}
	drainCh   chan struct{}
	closeOnce sync.Once
	waitOnce  sync.Once
	submits   sync.WaitGroup
	workers   sync.WaitGroup

	mu       sync.Mutex
	closed   bool
	firstErr error
	waitErr  error

	next      atomic.Uint64
	submitted atomic.Uint64
	completed atomic.Uint64
	stolen    atomic.Uint64
}

// NewWorkStealingPool starts workers with a bounded total waiting-task queue.
// Workers and queueCapacity must both be positive. A nil parent uses a
// background context.
func NewWorkStealingPool(parent context.Context, workers, queueCapacity int) (*WorkStealingPool, error) {
	if workers <= 0 || queueCapacity <= 0 {
		return nil, ErrWorkStealingPoolInvalid
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	pool := &WorkStealingPool{
		ctx:      ctx,
		cancel:   cancel,
		queues:   make([]workStealingDeque, workers),
		slots:    make(chan struct{}, queueCapacity),
		notify:   make(chan struct{}, workers),
		closedCh: make(chan struct{}),
		drainCh:  make(chan struct{}),
	}
	for index := range workers {
		pool.workers.Add(1)
		go pool.run(index)
	}
	return pool, nil
}

// Submit adds task to the next worker in round-robin order.
func (pool *WorkStealingPool) Submit(ctx context.Context, task Task) error {
	if pool == nil {
		return ErrWorkStealingPoolInvalid
	}
	worker := int(pool.next.Add(1)-1) % len(pool.queues)
	return pool.SubmitTo(ctx, worker, task)
}

// SubmitTo adds task to a specific worker deque. Idle workers may steal it.
// This is useful when a caller already has a natural task partition or wants
// to test queue-locality behavior explicitly.
func (pool *WorkStealingPool) SubmitTo(ctx context.Context, worker int, task Task) error {
	if pool == nil || len(pool.queues) == 0 || task == nil || worker < 0 || worker >= len(pool.queues) {
		return ErrWorkStealingPoolInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return ErrWorkStealingPoolClosed
	}
	if pool.firstErr != nil {
		err := pool.firstErr
		pool.mu.Unlock()
		return err
	}
	if err := pool.ctx.Err(); err != nil {
		pool.mu.Unlock()
		return err
	}
	pool.submits.Add(1)
	pool.mu.Unlock()
	defer pool.submits.Done()

	select {
	case pool.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-pool.ctx.Done():
		return pool.ctx.Err()
	case <-pool.closedCh:
		return ErrWorkStealingPoolClosed
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closed {
		pool.releaseSlot()
		return ErrWorkStealingPoolClosed
	}
	if pool.firstErr != nil {
		err := pool.firstErr
		pool.releaseSlot()
		return err
	}
	if err := pool.ctx.Err(); err != nil {
		pool.releaseSlot()
		return err
	}
	pool.queues[worker].pushBack(task)
	pool.submitted.Add(1)
	pool.signal()
	return nil
}

// Stats returns a snapshot of pool activity.
func (pool *WorkStealingPool) Stats() WorkStealingStats {
	if pool == nil {
		return WorkStealingStats{}
	}
	return WorkStealingStats{
		Submitted: pool.submitted.Load(),
		Completed: pool.completed.Load(),
		Stolen:    pool.stolen.Load(),
	}
}

// Cancel stops workers from accepting more tasks. Running tasks must observe
// the pool context; Wait returns context.Canceled or the parent context error
// when no task returned an error first.
func (pool *WorkStealingPool) Cancel() {
	if pool != nil && pool.cancel != nil {
		pool.cancel()
	}
}

// Close stops new submissions and lets already queued tasks drain.
func (pool *WorkStealingPool) Close() {
	if pool == nil {
		return
	}
	pool.closeOnce.Do(func() {
		pool.mu.Lock()
		pool.closed = true
		close(pool.closedCh)
		pool.mu.Unlock()
		pool.submits.Wait()
		close(pool.drainCh)
		pool.signalAll()
	})
}

// Wait closes the pool, waits for workers, and returns the first task error.
// It is safe to call more than once.
func (pool *WorkStealingPool) Wait() error {
	if pool == nil {
		return ErrWorkStealingPoolInvalid
	}
	pool.waitOnce.Do(func() {
		pool.Close()
		pool.workers.Wait()
		pool.mu.Lock()
		firstErr := pool.firstErr
		pool.mu.Unlock()
		contextErr := pool.ctx.Err()
		pool.cancel()
		if firstErr != nil {
			pool.waitErr = firstErr
			return
		}
		pool.waitErr = contextErr
	})
	return pool.waitErr
}

func (pool *WorkStealingPool) run(worker int) {
	defer pool.workers.Done()
	for {
		task, stolen, ok := pool.nextTask(worker)
		if !ok {
			return
		}
		pool.releaseSlot()
		if stolen {
			pool.stolen.Add(1)
		}
		err := task(pool.ctx)
		pool.completed.Add(1)
		if err != nil {
			pool.recordError(err)
			return
		}
	}
}

func (pool *WorkStealingPool) nextTask(worker int) (Task, bool, bool) {
	for {
		if task, ok := pool.queues[worker].popBack(); ok {
			return task, false, true
		}
		for offset := 1; offset < len(pool.queues); offset++ {
			victim := (worker + offset) % len(pool.queues)
			if task, ok := pool.queues[victim].popFront(); ok {
				return task, true, true
			}
		}

		pool.mu.Lock()
		closed := pool.closed
		pool.mu.Unlock()
		if closed {
			select {
			case <-pool.ctx.Done():
				return nil, false, false
			case <-pool.drainCh:
				return pool.take(worker)
			}
		}
		select {
		case <-pool.ctx.Done():
			return nil, false, false
		case <-pool.notify:
		}
	}
}

func (pool *WorkStealingPool) take(worker int) (Task, bool, bool) {
	if task, ok := pool.queues[worker].popBack(); ok {
		return task, false, true
	}
	for offset := 1; offset < len(pool.queues); offset++ {
		victim := (worker + offset) % len(pool.queues)
		if task, ok := pool.queues[victim].popFront(); ok {
			return task, true, true
		}
	}
	return nil, false, false
}

func (pool *WorkStealingPool) recordError(err error) {
	pool.mu.Lock()
	if pool.firstErr == nil {
		pool.firstErr = err
		pool.cancel()
	}
	pool.mu.Unlock()
}

func (pool *WorkStealingPool) releaseSlot() {
	<-pool.slots
}

func (pool *WorkStealingPool) signal() {
	select {
	case pool.notify <- struct{}{}:
	default:
	}
}

func (pool *WorkStealingPool) signalAll() {
	for range pool.queues {
		pool.signal()
	}
}

type workStealingDeque struct {
	mu    sync.Mutex
	items []Task
	head  int
}

func (deque *workStealingDeque) pushBack(task Task) {
	deque.mu.Lock()
	defer deque.mu.Unlock()
	deque.compact()
	deque.items = append(deque.items, task)
}

func (deque *workStealingDeque) popBack() (Task, bool) {
	deque.mu.Lock()
	defer deque.mu.Unlock()
	if deque.head >= len(deque.items) {
		return nil, false
	}
	index := len(deque.items) - 1
	task := deque.items[index]
	deque.items[index] = nil
	deque.items = deque.items[:index]
	if deque.head >= len(deque.items) {
		deque.items = nil
		deque.head = 0
	}
	return task, true
}

func (deque *workStealingDeque) popFront() (Task, bool) {
	deque.mu.Lock()
	defer deque.mu.Unlock()
	if deque.head >= len(deque.items) {
		return nil, false
	}
	task := deque.items[deque.head]
	deque.items[deque.head] = nil
	deque.head++
	if deque.head >= len(deque.items) {
		deque.items = nil
		deque.head = 0
	} else {
		deque.compact()
	}
	return task, true
}

func (deque *workStealingDeque) compact() {
	if deque.head == 0 {
		return
	}
	if deque.head < len(deque.items)/2 {
		return
	}
	remaining := copy(deque.items, deque.items[deque.head:])
	clear(deque.items[remaining:])
	deque.items = deque.items[:remaining]
	deque.head = 0
}
