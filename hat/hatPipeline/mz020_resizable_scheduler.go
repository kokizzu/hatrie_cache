package hatPipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	// ErrResizableSchedulerInvalid reports invalid scheduler or task settings.
	ErrResizableSchedulerInvalid = errors.New("hatPipeline: invalid resizable scheduler")
	// ErrResizableSchedulerClosed reports a submission or resize after close.
	ErrResizableSchedulerClosed = errors.New("hatPipeline: resizable scheduler is closed")
	// ErrResizableSchedulerWorkerCount reports a non-positive worker target.
	ErrResizableSchedulerWorkerCount = errors.New("hatPipeline: invalid resizable scheduler worker count")
)

// ResizableSchedulerStats is a point-in-time scheduler snapshot.
type ResizableSchedulerStats struct {
	ConfiguredWorkers int
	ActiveWorkers     int
	QueueLength       int
	QueueCapacity     int
	Closed            bool
}

// ResizableScheduler runs cooperative tasks over a bounded queue whose worker
// target can be changed while it is running. Resize never preempts a running
// task and never removes a queued task. A downsize takes effect as workers
// finish; an upsize starts workers immediately.
//
// The scheduler is opt-in. Existing Scheduler users keep the fixed-worker
// implementation and its behavior. Task callbacks must observe their context
// when they need prompt cancellation.
type ResizableScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	queue  chan Task
	wake   atomic.Pointer[chan struct{}]

	workers    sync.WaitGroup
	submitters sync.WaitGroup

	mu            sync.RWMutex
	targetWorkers atomic.Int64
	activeWorkers atomic.Int64
	closed        bool
	firstErr      error
}

// NewResizableScheduler starts a scheduler with workers and queueCapacity.
// queueCapacity may be zero for direct handoff between submitters and workers.
func NewResizableScheduler(parent context.Context, workers, queueCapacity int) (*ResizableScheduler, error) {
	if workers <= 0 || queueCapacity < 0 {
		return nil, ErrResizableSchedulerInvalid
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	wake := make(chan struct{})
	scheduler := &ResizableScheduler{
		ctx:    ctx,
		cancel: cancel,
		queue:  make(chan Task, queueCapacity),
	}
	scheduler.targetWorkers.Store(int64(workers))
	scheduler.wake.Store(&wake)
	for range workers {
		scheduler.startWorker()
	}
	return scheduler, nil
}

func (scheduler *ResizableScheduler) startWorker() {
	scheduler.activeWorkers.Add(1)
	scheduler.workers.Add(1)
	go scheduler.runWorker()
}

// Submit queues task, waiting for capacity or context cancellation. Scheduler
// cancellation also unblocks a blocked submitter.
func (scheduler *ResizableScheduler) Submit(ctx context.Context, task Task) error {
	if scheduler == nil || scheduler.queue == nil || task == nil {
		return ErrResizableSchedulerInvalid
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
		return ErrResizableSchedulerClosed
	}
	if scheduler.firstErr != nil {
		err := scheduler.firstErr
		scheduler.mu.Unlock()
		return err
	}
	scheduler.submitters.Add(1)
	scheduler.mu.Unlock()
	defer scheduler.submitters.Done()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-scheduler.ctx.Done():
		return scheduler.ctx.Err()
	case scheduler.queue <- task:
		return nil
	}
}

// Resize changes the target worker count. Workers that are already executing
// finish their current task before retiring. A positive worker count is
// required; callers that want to stop the scheduler should use Close or Wait.
func (scheduler *ResizableScheduler) Resize(workers int) error {
	if scheduler == nil || scheduler.queue == nil {
		return ErrResizableSchedulerInvalid
	}
	if workers <= 0 {
		return ErrResizableSchedulerWorkerCount
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.closed {
		return ErrResizableSchedulerClosed
	}
	scheduler.targetWorkers.Store(int64(workers))
	oldWake := scheduler.wake.Load()
	newWake := make(chan struct{})
	scheduler.wake.Store(&newWake)
	if oldWake != nil {
		close(*oldWake)
	}
	for scheduler.activeWorkers.Load() < int64(workers) {
		scheduler.startWorker()
	}
	return nil
}

// WorkerCount returns the current target worker count.
func (scheduler *ResizableScheduler) WorkerCount() int {
	if scheduler == nil {
		return 0
	}
	scheduler.mu.RLock()
	defer scheduler.mu.RUnlock()
	return int(scheduler.targetWorkers.Load())
}

// ActiveWorkerCount returns workers that have not yet retired or exited.
func (scheduler *ResizableScheduler) ActiveWorkerCount() int {
	if scheduler == nil {
		return 0
	}
	scheduler.mu.RLock()
	defer scheduler.mu.RUnlock()
	return int(scheduler.activeWorkers.Load())
}

// Stats returns a point-in-time queue and worker snapshot.
func (scheduler *ResizableScheduler) Stats() ResizableSchedulerStats {
	if scheduler == nil {
		return ResizableSchedulerStats{}
	}
	scheduler.mu.RLock()
	defer scheduler.mu.RUnlock()
	return ResizableSchedulerStats{
		ConfiguredWorkers: int(scheduler.targetWorkers.Load()),
		ActiveWorkers:     int(scheduler.activeWorkers.Load()),
		QueueLength:       len(scheduler.queue),
		QueueCapacity:     cap(scheduler.queue),
		Closed:            scheduler.closed,
	}
}

// Cancel stops workers and rejects blocked submissions. Running tasks must
// observe the shared context; queued tasks are discarded after cancellation.
func (scheduler *ResizableScheduler) Cancel() {
	if scheduler != nil && scheduler.cancel != nil {
		scheduler.cancel()
	}
}

// Close stops new submissions and lets queued tasks drain. It does not cancel
// task contexts, so a caller can use Close before Wait for graceful shutdown.
func (scheduler *ResizableScheduler) Close() {
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
	scheduler.submitters.Wait()
	close(scheduler.queue)
}

// Wait closes the scheduler, waits for all workers, and returns the first task
// error. Parent or explicit cancellation is returned when no task failed.
func (scheduler *ResizableScheduler) Wait() error {
	if scheduler == nil || scheduler.queue == nil {
		return ErrResizableSchedulerInvalid
	}
	scheduler.Close()
	scheduler.workers.Wait()
	scheduler.mu.RLock()
	firstErr := scheduler.firstErr
	scheduler.mu.RUnlock()
	contextErr := scheduler.ctx.Err()
	scheduler.cancel()
	if firstErr != nil {
		return firstErr
	}
	return contextErr
}

func (scheduler *ResizableScheduler) claimRetirement() bool {
	for {
		target := scheduler.targetWorkers.Load()
		active := scheduler.activeWorkers.Load()
		if active <= target {
			return false
		}
		if scheduler.activeWorkers.CompareAndSwap(active, active-1) {
			return true
		}
	}
}

func (scheduler *ResizableScheduler) runWorker() {
	retired := false
	defer func() {
		if !retired {
			scheduler.activeWorkers.Add(-1)
		}
		scheduler.workers.Done()
	}()

	wakePointer := scheduler.wake.Load()
	var wake <-chan struct{}
	if wakePointer != nil {
		wake = *wakePointer
	}
	for {
		if scheduler.claimRetirement() {
			retired = true
			return
		}

		select {
		case <-scheduler.ctx.Done():
			return
		case <-wake:
			wakePointer = scheduler.wake.Load()
			if wakePointer != nil {
				wake = *wakePointer
			}
			continue
		case task, ok := <-scheduler.queue:
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
}

func (scheduler *ResizableScheduler) recordError(err error) {
	scheduler.mu.Lock()
	if scheduler.firstErr == nil {
		scheduler.firstErr = err
		scheduler.cancel()
	}
	scheduler.mu.Unlock()
}
