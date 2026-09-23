package hatFiber

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	// DefaultWorkerPoolWorkers keeps the opt-in pool small unless callers need
	// more parallelism.
	DefaultWorkerPoolWorkers = 1
	// DefaultWorkerPoolMaxFibers bounds live continuations per worker.
	DefaultWorkerPoolMaxFibers = 256
	// DefaultWorkerPoolQueueCapacity bounds pending submissions per worker.
	DefaultWorkerPoolQueueCapacity = 256
	// DefaultWorkerPoolStepsPerTurn bounds one worker's continuous scheduler
	// run before it admits more work.
	DefaultWorkerPoolStepsPerTurn = 64
	// MaxWorkerPoolWorkers prevents untrusted configuration from starting an
	// excessive number of goroutines.
	MaxWorkerPoolWorkers = 1024
	// MaxWorkerPoolTotalFibers bounds scheduler storage across all workers.
	MaxWorkerPoolTotalFibers = 1 << 20
	// MaxWorkerPoolQueueCapacity bounds pending storage per worker.
	MaxWorkerPoolQueueCapacity = 1 << 20
	// MaxWorkerPoolStepsPerTurn bounds one scheduler turn.
	MaxWorkerPoolStepsPerTurn = 1 << 20
)

var (
	// ErrWorkerPoolInvalid reports invalid pool options or a nil pool/task.
	ErrWorkerPoolInvalid = errors.New("hatFiber: worker pool is invalid")
	// ErrWorkerPoolClosed reports a submission after pool shutdown begins.
	ErrWorkerPoolClosed = errors.New("hatFiber: worker pool is closed")
	// ErrWorkerFutureInvalid reports a method call on a nil future.
	ErrWorkerFutureInvalid = errors.New("hatFiber: worker future is invalid")
)

// WorkerPoolOptions configures an opt-in pool of cooperative fiber workers.
// QueueCapacity is per worker. Zero selects the documented default queue
// capacity.
type WorkerPoolOptions struct {
	Context            context.Context
	Workers            int
	MaxFibersPerWorker int
	QueueCapacity      int
	StepsPerTurn       int
}

// Future represents one submitted cooperative task. Waiting on a Future does
// not occupy a worker goroutine; the worker continues scheduling other tasks.
type Future struct {
	done chan struct{}
	once sync.Once
	mu   sync.Mutex
	err  error
}

// Done returns a channel closed after the task reaches a terminal state. A
// nil future returns nil.
func (future *Future) Done() <-chan struct{} {
	if future == nil {
		return nil
	}
	return future.done
}

// Wait blocks until the task completes or ctx is canceled. A nil context is
// treated as context.Background.
func (future *Future) Wait(ctx context.Context) error {
	if future == nil || future.done == nil {
		return ErrWorkerFutureInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-future.done:
		future.mu.Lock()
		err := future.err
		future.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (future *Future) complete(err error) {
	if future == nil {
		return
	}
	future.once.Do(func() {
		future.mu.Lock()
		future.err = err
		future.mu.Unlock()
		close(future.done)
	})
}

type workerRequest struct {
	context context.Context
	step    StepFunc
	future  *Future
}

type workerPoolWorker struct {
	context      context.Context
	scheduler    *Scheduler
	queue        chan workerRequest
	stepsPerTurn int
	done         chan struct{}
}

// WorkerPool runs bounded stackless continuations over fixed worker
// goroutines. Each worker owns its Scheduler, so scheduler state remains
// single-owner while Submit and Future.Wait are safe for concurrent callers.
type WorkerPool struct {
	context context.Context
	cancel  context.CancelFunc
	workers []*workerPoolWorker
	next    atomic.Uint64

	submitters sync.WaitGroup
	mu         sync.Mutex
	closed     bool
}

// NewWorkerPool starts an opt-in cooperative worker pool. Zero-valued numeric
// options use the documented defaults. Tasks must return promptly and use
// StepYield between bounded units of work; the pool does not preempt a task.
// StepWait is reserved for a Scheduler owned directly by the caller because
// pooled callbacks do not expose the worker's synchronization primitives.
func NewWorkerPool(options WorkerPoolOptions) (*WorkerPool, error) {
	workers := options.Workers
	if workers == 0 {
		workers = DefaultWorkerPoolWorkers
	}
	maxFibers := options.MaxFibersPerWorker
	if maxFibers == 0 {
		maxFibers = DefaultWorkerPoolMaxFibers
	}
	queueCapacity := options.QueueCapacity
	if queueCapacity == 0 {
		queueCapacity = DefaultWorkerPoolQueueCapacity
	}
	stepsPerTurn := options.StepsPerTurn
	if stepsPerTurn == 0 {
		stepsPerTurn = DefaultWorkerPoolStepsPerTurn
	}
	if workers < 1 || workers > MaxWorkerPoolWorkers ||
		maxFibers < 1 || maxFibers > MaxFibers ||
		workers > MaxWorkerPoolTotalFibers/maxFibers ||
		queueCapacity < 0 || queueCapacity > MaxWorkerPoolQueueCapacity ||
		stepsPerTurn < 1 || stepsPerTurn > MaxWorkerPoolStepsPerTurn {
		return nil, ErrWorkerPoolInvalid
	}
	parent := options.Context
	if parent == nil {
		parent = context.Background()
	}
	poolContext, cancel := context.WithCancel(parent)
	pool := &WorkerPool{
		context: poolContext,
		cancel:  cancel,
		workers: make([]*workerPoolWorker, workers),
	}
	for index := range pool.workers {
		scheduler, err := New(Options{MaxFibers: maxFibers})
		if err != nil {
			cancel()
			return nil, err
		}
		worker := &workerPoolWorker{
			context:      poolContext,
			scheduler:    scheduler,
			queue:        make(chan workerRequest, queueCapacity),
			stepsPerTurn: stepsPerTurn,
			done:         make(chan struct{}),
		}
		pool.workers[index] = worker
		go worker.run()
	}
	go pool.watchContext()
	return pool, nil
}

// Submit queues one continuation and returns immediately after admission. The
// submission context controls admission and is also the context given to the
// task callback once it runs.
func (pool *WorkerPool) Submit(ctx context.Context, step StepFunc) (*Future, error) {
	if pool == nil || len(pool.workers) == 0 || step == nil {
		return nil, ErrWorkerPoolInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil, ErrWorkerPoolClosed
	}
	pool.submitters.Add(1)
	workerIndex := (pool.next.Add(1) - 1) % uint64(len(pool.workers))
	worker := pool.workers[workerIndex]
	pool.mu.Unlock()
	defer pool.submitters.Done()

	future := &Future{done: make(chan struct{})}
	request := workerRequest{context: ctx, step: step, future: future}
	select {
	case worker.queue <- request:
		return future, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pool.context.Done():
		return nil, pool.context.Err()
	}
}

// Close stops accepting new submissions, drains admitted tasks, and waits for
// workers. If ctx is canceled first, running workers are canceled and ctx's
// error is returned.
func (pool *WorkerPool) Close(ctx context.Context) error {
	if pool == nil {
		return ErrWorkerPoolInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil
	}
	pool.closed = true
	pool.mu.Unlock()

	pool.submitters.Wait()
	for _, worker := range pool.workers {
		close(worker.queue)
	}
	for _, worker := range pool.workers {
		select {
		case <-worker.done:
		case <-ctx.Done():
			pool.cancel()
			return ctx.Err()
		}
	}
	pool.cancel()
	return nil
}

func (pool *WorkerPool) watchContext() {
	<-pool.context.Done()
	_ = pool.Close(context.Background())
}

func (worker *workerPoolWorker) run() {
	defer close(worker.done)
	active := make(map[FiberID]*Future, worker.scheduler.Capacity())
	for {
		if err := worker.context.Err(); err != nil {
			worker.stop(active, err)
			return
		}
		worker.admit(active)
		if len(active) == 0 {
			request, ok := <-worker.queue
			if !ok {
				return
			}
			worker.spawn(request, active)
			continue
		}
		if _, err := worker.scheduler.Run(worker.context, worker.stepsPerTurn); err != nil {
			worker.stop(active, err)
			return
		}
		worker.finish(active)
	}
}

func (worker *workerPoolWorker) admit(active map[FiberID]*Future) {
	for len(active) < worker.scheduler.Capacity() {
		select {
		case request, ok := <-worker.queue:
			if !ok {
				return
			}
			worker.spawn(request, active)
		default:
			return
		}
	}
}

func (worker *workerPoolWorker) spawn(request workerRequest, active map[FiberID]*Future) {
	identifier, err := worker.scheduler.Spawn(func(context.Context) (Step, error) {
		if err := request.context.Err(); err != nil {
			return StepDone, err
		}
		return runWorkerStep(request.context, request.step)
	})
	if err != nil {
		request.future.complete(err)
		return
	}
	active[identifier] = request.future
}

func runWorkerStep(ctx context.Context, step StepFunc) (result Step, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = StepDone
			err = fmt.Errorf("hatFiber: worker task panic: %v", recovered)
		}
	}()
	return step(ctx)
}

func (worker *workerPoolWorker) finish(active map[FiberID]*Future) {
	for identifier, future := range active {
		status, err := worker.scheduler.Status(identifier)
		if err != nil {
			future.complete(err)
			delete(active, identifier)
			continue
		}
		switch status {
		case StatusDone:
			future.complete(nil)
		case StatusFailed:
			future.complete(worker.scheduler.Failure(identifier))
		case StatusCancelled:
			future.complete(context.Canceled)
		default:
			continue
		}
		_ = worker.scheduler.Reap(identifier)
		delete(active, identifier)
	}
}

func (worker *workerPoolWorker) stop(active map[FiberID]*Future, err error) {
	worker.scheduler.Close()
	for identifier, future := range active {
		future.complete(err)
		_ = worker.scheduler.Reap(identifier)
		delete(active, identifier)
	}
	for request := range worker.queue {
		request.future.complete(err)
	}
}
