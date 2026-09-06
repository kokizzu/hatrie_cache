package hatPipeline

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrSchedulerClosed reports that a task was submitted after Close.
	ErrSchedulerClosed = errors.New("hatPipeline: scheduler is closed")
	// ErrSchedulerInvalid reports an invalid scheduler or task.
	ErrSchedulerInvalid = errors.New("hatPipeline: invalid scheduler")
)

// Task is one unit of cooperative work. A task should periodically observe
// ctx when it performs long-running work.
type Task func(context.Context) error

// Scheduler runs bounded cooperative tasks over a fixed worker set. It does
// not preempt tasks; cancellation takes effect when a task observes its
// context or returns.
type Scheduler struct {
	ctx     context.Context
	cancel  context.CancelFunc
	queue   *Channel[Task]
	workers sync.WaitGroup

	mu       sync.Mutex
	closed   bool
	firstErr error
}

// NewScheduler starts workers with a bounded task queue. A zero queue
// capacity uses direct handoff between submitters and workers.
func NewScheduler(parent context.Context, workers, queueCapacity int) (*Scheduler, error) {
	if workers <= 0 || queueCapacity < 0 {
		return nil, ErrSchedulerInvalid
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	scheduler := &Scheduler{ctx: ctx, cancel: cancel}
	queue, err := NewChannel[Task](queueCapacity)
	if err != nil {
		cancel()
		return nil, err
	}
	scheduler.queue = queue
	for range workers {
		scheduler.workers.Add(1)
		go scheduler.run()
	}
	return scheduler, nil
}

// Submit queues task, waiting for queue capacity or ctx cancellation.
// Scheduler cancellation also unblocks a blocked submitter.
func (scheduler *Scheduler) Submit(ctx context.Context, task Task) error {
	if scheduler == nil || scheduler.queue == nil || task == nil {
		return ErrSchedulerInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
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
	scheduler.mu.Unlock()
	if err := scheduler.ctx.Err(); err != nil {
		return err
	}
	err := scheduler.queue.sendWithStop(ctx, scheduler.ctx.Done(), context.Canceled, task)
	if errors.Is(err, ErrChannelClosed) {
		return ErrSchedulerClosed
	}
	return err
}

// Cancel stops workers from accepting more tasks. Running tasks must observe
// the context themselves; Wait returns context.Canceled if no task failed.
func (scheduler *Scheduler) Cancel() {
	if scheduler != nil && scheduler.cancel != nil {
		scheduler.cancel()
	}
}

// Close stops new submissions and lets already queued tasks drain.
func (scheduler *Scheduler) Close() {
	if scheduler == nil || scheduler.queue == nil {
		return
	}
	scheduler.mu.Lock()
	if scheduler.closed {
		scheduler.mu.Unlock()
		return
	}
	scheduler.closed = true
	scheduler.mu.Unlock()
	scheduler.queue.Close()
}

// Wait closes the queue, waits for all workers, and returns the first task
// error. Parent or explicit cancellation is returned when no task failed.
func (scheduler *Scheduler) Wait() error {
	if scheduler == nil || scheduler.queue == nil {
		return ErrSchedulerInvalid
	}
	scheduler.Close()
	scheduler.workers.Wait()
	scheduler.mu.Lock()
	firstErr := scheduler.firstErr
	scheduler.mu.Unlock()
	contextErr := scheduler.ctx.Err()
	scheduler.cancel()
	if firstErr != nil {
		return firstErr
	}
	return contextErr
}

func (scheduler *Scheduler) run() {
	defer scheduler.workers.Done()
	for {
		task, ok, err := scheduler.queue.Receive(scheduler.ctx)
		if err != nil || !ok {
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

func (scheduler *Scheduler) recordError(err error) {
	scheduler.mu.Lock()
	if scheduler.firstErr == nil {
		scheduler.firstErr = err
		scheduler.cancel()
	}
	scheduler.mu.Unlock()
}
