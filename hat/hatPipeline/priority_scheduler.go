package hatPipeline

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrSchedulerPriorityUnavailable reports that priority submission was
	// requested from the legacy FIFO scheduler.
	ErrSchedulerPriorityUnavailable = errors.New("hatPipeline: priority scheduler is not enabled")
)

type prioritySchedulerItem struct {
	priority int
	sequence uint64
	task     Task
}

type prioritySchedulerQueue []prioritySchedulerItem

func (queue prioritySchedulerQueue) Len() int {
	return len(queue)
}

func (queue prioritySchedulerQueue) less(left, right int) bool {
	if queue[left].priority != queue[right].priority {
		return queue[left].priority > queue[right].priority
	}
	return queue[left].sequence < queue[right].sequence
}

func (queue *prioritySchedulerQueue) push(item prioritySchedulerItem) {
	*queue = append(*queue, item)
	for index := len(*queue) - 1; index > 0; {
		parent := (index - 1) / 2
		if !queue.less(index, parent) {
			break
		}
		(*queue)[index], (*queue)[parent] = (*queue)[parent], (*queue)[index]
		index = parent
	}
}

func (queue *prioritySchedulerQueue) pop() prioritySchedulerItem {
	items := *queue
	last := len(items) - 1
	item := items[0]
	items[0] = items[last]
	items[last] = prioritySchedulerItem{}
	items = items[:last]
	for index := 0; ; {
		left := index*2 + 1
		if left >= len(items) {
			break
		}
		smallest := left
		right := left + 1
		if right < len(items) && queue.less(right, left) {
			smallest = right
		}
		if !queue.less(smallest, index) {
			break
		}
		items[index], items[smallest] = items[smallest], items[index]
		index = smallest
	}
	*queue = items
	return item
}

// PriorityScheduler runs bounded cooperative tasks in descending priority
// order. Equal priorities retain submission order. It is opt-in; Scheduler
// remains the lower-overhead FIFO implementation.
type PriorityScheduler struct {
	ctx           context.Context
	cancel        context.CancelFunc
	capacity      int
	queue         prioritySchedulerQueue
	changed       chan struct{}
	changedClosed bool
	sequence      uint64
	waiting       int
	workers       sync.WaitGroup
	mu            sync.Mutex
	closed        bool
	firstErr      error
}

// NewPriorityScheduler starts workers with a bounded priority queue. A zero
// queue capacity uses direct handoff between submitters and waiting workers.
func NewPriorityScheduler(parent context.Context, workers, queueCapacity int) (*PriorityScheduler, error) {
	if workers <= 0 || queueCapacity < 0 {
		return nil, ErrSchedulerInvalid
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	scheduler := &PriorityScheduler{
		ctx:      ctx,
		cancel:   cancel,
		capacity: queueCapacity,
		changed:  make(chan struct{}),
	}
	for range workers {
		scheduler.workers.Add(1)
		go scheduler.run()
	}
	return scheduler, nil
}

// Submit queues a task at priority zero.
func (scheduler *PriorityScheduler) Submit(ctx context.Context, task Task) error {
	return scheduler.SubmitPriority(ctx, 0, task)
}

// SubmitPriority queues task, waiting for capacity or cancellation. Higher
// priority values run first; equal priorities retain submission order.
func (scheduler *PriorityScheduler) SubmitPriority(ctx context.Context, priority int, task Task) error {
	if scheduler == nil || task == nil || scheduler.cancel == nil {
		return ErrSchedulerInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		scheduler.mu.Lock()
		if scheduler.closed {
			scheduler.mu.Unlock()
			return ErrSchedulerClosed
		}
		if scheduler.firstErr != nil {
			err := scheduler.firstErr
			scheduler.mu.Unlock()
			return err
		}
		if err := scheduler.ctx.Err(); err != nil {
			scheduler.mu.Unlock()
			return err
		}
		if scheduler.canEnqueueLocked() {
			scheduler.queue.push(prioritySchedulerItem{
				priority: priority,
				sequence: scheduler.sequence,
				task:     task,
			})
			scheduler.sequence++
			scheduler.signalLocked()
			scheduler.mu.Unlock()
			return nil
		}
		changed := scheduler.registerWaiterLocked()
		scheduler.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-scheduler.ctx.Done():
			return scheduler.ctx.Err()
		case <-changed:
		}
	}
}

func (scheduler *PriorityScheduler) canEnqueueLocked() bool {
	if scheduler.capacity == 0 {
		return scheduler.waiting > 0 && scheduler.queue.Len() == 0
	}
	return scheduler.queue.Len() < scheduler.capacity
}

func (scheduler *PriorityScheduler) signalLocked() {
	if scheduler.changedClosed {
		return
	}
	close(scheduler.changed)
	scheduler.changedClosed = true
}

func (scheduler *PriorityScheduler) registerWaiterLocked() chan struct{} {
	if scheduler.changedClosed {
		scheduler.changed = make(chan struct{})
		scheduler.changedClosed = false
	}
	return scheduler.changed
}

// Cancel cancels queued and running scheduler work. Running tasks must
// observe the context themselves.
func (scheduler *PriorityScheduler) Cancel() {
	if scheduler == nil || scheduler.cancel == nil {
		return
	}
	scheduler.cancel()
	scheduler.mu.Lock()
	scheduler.signalLocked()
	scheduler.mu.Unlock()
}

// Close stops new submissions and lets already queued tasks drain.
func (scheduler *PriorityScheduler) Close() {
	if scheduler == nil || scheduler.cancel == nil {
		return
	}
	scheduler.mu.Lock()
	if !scheduler.closed {
		scheduler.closed = true
		scheduler.signalLocked()
	}
	scheduler.mu.Unlock()
}

// Wait closes the queue, waits for workers, and returns the first task error.
func (scheduler *PriorityScheduler) Wait() error {
	if scheduler == nil || scheduler.cancel == nil {
		return ErrSchedulerInvalid
	}
	scheduler.Close()
	scheduler.workers.Wait()
	scheduler.mu.Lock()
	firstErr := scheduler.firstErr
	contextErr := scheduler.ctx.Err()
	scheduler.mu.Unlock()
	scheduler.cancel()
	if firstErr != nil {
		return firstErr
	}
	return contextErr
}

func (scheduler *PriorityScheduler) run() {
	defer scheduler.workers.Done()
	for {
		task, ok := scheduler.nextTask()
		if !ok {
			return
		}
		if scheduler.ctx.Err() != nil {
			return
		}
		if err := task(scheduler.ctx); err != nil {
			scheduler.recordError(err)
			return
		}
	}
}

func (scheduler *PriorityScheduler) nextTask() (Task, bool) {
	for {
		scheduler.mu.Lock()
		if scheduler.queue.Len() > 0 {
			item := scheduler.queue.pop()
			scheduler.signalLocked()
			scheduler.mu.Unlock()
			return item.task, true
		}
		if scheduler.closed || scheduler.ctx.Err() != nil {
			scheduler.mu.Unlock()
			return nil, false
		}
		scheduler.waiting++
		if scheduler.capacity == 0 {
			// Wake a submitter that may have observed no waiting worker just
			// before this worker registered for direct handoff.
			scheduler.signalLocked()
		}
		changed := scheduler.registerWaiterLocked()
		scheduler.mu.Unlock()

		select {
		case <-scheduler.ctx.Done():
			scheduler.mu.Lock()
			scheduler.waiting--
			scheduler.mu.Unlock()
			return nil, false
		case <-changed:
			scheduler.mu.Lock()
			scheduler.waiting--
			scheduler.mu.Unlock()
		}
	}
}

func (scheduler *PriorityScheduler) recordError(err error) {
	scheduler.mu.Lock()
	if scheduler.firstErr == nil {
		scheduler.firstErr = err
		scheduler.cancel()
		scheduler.signalLocked()
	}
	scheduler.mu.Unlock()
}
